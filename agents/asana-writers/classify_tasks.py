import os, json, time, datetime as dt, pathlib, glob
from typing import Any, Dict, List, Tuple
from openai import OpenAI

# --------- ENV ---------
OPENAI_API_KEY         = os.getenv("OPENAI_API_KEY")
# Prefer dedicated classifier id, fall back to the old single-assistant id
CLASSIFIER_ASSISTANT_ID = os.getenv("OPENAI_CLASSIFIER_ID") or os.getenv("OPENAI_ASSISTANT_ID")

PROMPT_PATH       = os.getenv("PROMPT_PATH", "prompt.txt")          # optional; assistant may already have system instructions
RULES_JSON_PATH   = os.getenv("RULES_JSON_PATH", "rules.json")      # optional helper hints
ANALYZE_JSONL_PATH = os.getenv("ANALYZE_JSONL_PATH", "data/asana_writer_tasks_sample.jsonl")

OUTPUT_DIR = "output"
BATCH_SIZE = int(os.getenv("CLASSIFY_BATCH_SIZE", "80"))
RUN_TIMEOUT_SECS = int(os.getenv("CLASSIFY_RUN_TIMEOUT_SECS", "600"))  # 10 minutes per batch
POLL_SLEEP = 1.2

# --------- Helpers ---------
def ensure_output_dir() -> str:
    pathlib.Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR

def load_jsonl(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise SystemExit(f"ANALYZE_JSONL_PATH not found: {path}")
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except Exception:
                # keep moving; but log? we'll just skip malformed lines
                pass
    if not out:
        raise SystemExit(f"No tasks found in {path}")
    return out

def chunked(xs: List[Any], n: int) -> List[List[Any]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

def read_or_default(path: str, default: str = "") -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return default

def poll_run(client: OpenAI, thread_id: str, run_id: str, timeout_s: int) -> str:
    t0 = time.time()
    while True:
        run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
        if run.status in ("completed", "failed", "cancelled", "expired"):
            return run.status
        if time.time() - t0 > timeout_s:
            return "timeout"
        time.sleep(POLL_SLEEP)

def collect_assistant_text(client: OpenAI, thread_id: str) -> str:
    msgs = client.beta.threads.messages.list(thread_id=thread_id, order="desc", limit=10)
    parts = []
    for m in msgs.data:
        if m.role == "assistant":
            for c in m.content:
                if c.type == "text" and c.text and c.text.value:
                    parts.append(c.text.value)
    # messages were requested newest-first; preserve chronological order
    return "\n".join(reversed(parts)).strip()

def classify_batch(
    client: OpenAI,
    assistant_id: str,
    batch_tasks: List[Dict[str, Any]],
    rules_text: str,
    prompt_text: str,
    batch_index: int
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Runs one Assistant classification batch.
    Returns (task_list, raw_text)
    """
    # Prepare a small upload file for this batch
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    tmp_json_path = os.path.join(OUTPUT_DIR, f"tasks_batch_{batch_index:02d}_{stamp}_upload.json")
    with open(tmp_json_path, "w", encoding="utf-8") as w:
        json.dump(batch_tasks, w, ensure_ascii=False)

    file_obj = client.files.create(file=open(tmp_json_path, "rb"), purpose="assistants")

    # Create thread + message with file_search tool attachment
    thread = client.beta.threads.create()
    user_msg = (
        "Classify the attached tasks JSON. "
        "Return a single JSON object with a top-level key 'tasks', where each item contains:\n"
        "  name, ad_type, effort (with minutes per category or a single chosen minutes), "
        "  assignee, due_on, and permalink_url.\n"
        "Use the decision rules below as authoritative for ad_type and effort.\n\n"
        f"RULES JSON:\n{rules_text}\n"
    )
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=user_msg,
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}],
    )

    # Kick the run; enforce JSON
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
        instructions=prompt_text or "Classify the tasks and return JSON only.",
        response_format={"type": "json_object"},
    )

    status = poll_run(client, thread.id, run.id, RUN_TIMEOUT_SECS)
    raw_text = collect_assistant_text(client, thread.id) if status == "completed" else f"(status={status})"

    # Save raw for debugging
    raw_path = os.path.join(OUTPUT_DIR, f"classify_batch_{batch_index:02d}_{stamp}.txt")
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(raw_text or "")

    # Parse JSON if we got it
    tasks_out: List[Dict[str, Any]] = []
    if status == "completed" and raw_text:
        try:
            obj = json.loads(raw_text)
            items = obj.get("tasks", [])
            if isinstance(items, list):
                tasks_out = items
        except Exception:
            # leave empty; caller will see counts in summary
            pass

    return tasks_out, raw_path

# --------- Main ---------
def main():
    if not OPENAI_API_KEY or not CLASSIFIER_ASSISTANT_ID:
        raise SystemExit("Missing OPENAI_API_KEY or OPENAI_CLASSIFIER_ASSISTANT_ID/OPENAI_ASSISTANT_ID.")

    ensure_output_dir()

    tasks = load_jsonl(ANALYZE_JSONL_PATH)
    prompt_text = read_or_default(PROMPT_PATH, "")
    rules_text  = read_or_default(RULES_JSON_PATH, "")

    client = OpenAI(api_key=OPENAI_API_KEY)

    batches = chunked(tasks, BATCH_SIZE)
    print(f"Classifying {len(tasks)} tasks in {len(batches)} batch(es) of up to {BATCH_SIZE} each...")

    all_classified: List[Dict[str, Any]] = []
    debug_files: List[str] = []
    for i, batch in enumerate(batches, start=1):
        classified, raw_file = classify_batch(
            client=client,
            assistant_id=CLASSIFIER_ASSISTANT_ID,
            batch_tasks=batch,
            rules_text=rules_text,
            prompt_text=prompt_text,
            batch_index=i
        )
        debug_files.append(raw_file)
        print(f"Batch {i}: received {len(classified)} classified items")
        all_classified.extend(classified)

    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    out_path = os.path.join(OUTPUT_DIR, f"classified_tasks_{stamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"tasks": all_classified}, f, ensure_ascii=False, indent=2)

    # Short summary for the workflow log
    print("----- Classification summary -----")
    print(f"Input tasks:    {len(tasks)}")
    print(f"Classified:     {len(all_classified)}")
    print(f"Raw logs:       {', '.join(os.path.basename(p) for p in debug_files)}")
    print(f"Output JSON:    {out_path}")

if __name__ == "__main__":
    main()
