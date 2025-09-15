import os, json, time, datetime as dt
from typing import List, Dict, Any
from openai import OpenAI

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CLASSIFIER_ID  = os.getenv("OPENAI_CLASSIFIER_ID") or os.getenv("OPENAI_ASSISTANT_ID")  # fallback

JSONL_PATH     = os.getenv("ANALYZE_JSONL_PATH", "data/asana_writer_tasks_sample.jsonl")
BATCH_SIZE     = int(os.getenv("CLASSIFY_BATCH_SIZE", "60"))          # safer than 80
MIN_BATCH      = int(os.getenv("CLASSIFY_MIN_BATCH", "10"))
RUN_POLL_SECS  = int(os.getenv("CLASSIFY_RUN_TIMEOUT_SECS", "600"))
PROMPT_CLASSIFY_PATH = os.getenv("PROMPT_CLASSIFY_PATH", "prompt_classify.txt")

if not OPENAI_API_KEY or not CLASSIFIER_ID:
    raise SystemExit("Missing OPENAI_API_KEY or OPENAI_CLASSIFIER_ID (or OPENAI_ASSISTANT_ID).")

def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def load_tasks(jsonl_path: str) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            tasks.append({
                "gid": obj.get("gid", ""),
                "name": obj.get("name", "") or "",
                "parent_name": (obj.get("parent") or {}).get("name") or "",
                "assignee_name": (obj.get("assignee") or {}).get("name") or "",
                "due_on": obj.get("due_on") or "",
            })
    return tasks

def make_batch_prompt(batch: List[Dict[str, Any]]) -> str:
    # Embed the batch as JSON inside the user message (no tool attachments).
    return json.dumps({"input": batch}, ensure_ascii=False)

def poll_until_done(client: OpenAI, thread_id: str, run_id: str, timeout_s: int = RUN_POLL_SECS):
    start = time.time()
    while True:
        run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
        if run.status in ("completed", "failed", "cancelled", "expired"):
            return run
        if time.time() - start > timeout_s:
            return run  # caller will handle non-completed status
        time.sleep(1.2)

def run_classify_batch(
    client: OpenAI,
    assistant_id: str,
    batch: List[Dict[str, Any]],
    tag: str,
    prompt_base: str,   # <-- pass the prompt in
) -> List[Dict[str, Any]]:
    """
    Runs one batch through the Assistant. If the JSON is short/invalid,
    we recursively split the batch until <= MIN_BATCH or it succeeds.
    """
    # 1) Create a thread and post the input JSON in the user message
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=make_batch_prompt(batch)
    )

    # 2) Run with strict, per-batch constraint
    per_batch_note = (
        f"\n\nHARD REQUIREMENT: Return a JSON object with a 'tasks' array "
        f"of exactly {len(batch)} items — one for each input — in the same order. "
        "No commentary, no extra fields."
    )

    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id,
        instructions=prompt_base + per_batch_note
    )
    run = poll_until_done(client, thread.id, run.id)

    # 3) Collect text and save raw for debugging
    raw_texts: List[str] = []
    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=10)
    for m in msgs.data:
        if m.role == "assistant":
            for c in m.content:
                if c.type == "text" and getattr(c, "text", None) and getattr(c.text, "value", None):
                    raw_texts.append(c.text.value)
    raw_texts.reverse()
    raw_concat = "\n".join(raw_texts).strip()

    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    raw_path = os.path.join(OUTPUT_DIR, f"classify_batch_{tag}_{stamp}.txt")
    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(raw_concat)

    # 4) Try parse + validate count
    try:
        obj = json.loads(raw_concat)
        out = obj.get("tasks", [])
        if isinstance(out, list) and len(out) == len(batch):
            return out
    except Exception:
        pass

    # If here, not a full valid set. Split unless at MIN_BATCH; then salvage best-effort.
    if len(batch) > MIN_BATCH:
        mid = len(batch) // 2
        left  = run_classify_batch(client, assistant_id, batch[:mid],  tag + "_L", prompt_base)
        right = run_classify_batch(client, assistant_id, batch[mid:], tag + "_R", prompt_base)
        return left + right
    else:
        try:
            obj = json.loads(raw_concat)
            out = obj.get("tasks", [])
            if isinstance(out, list):
                return out
        except Exception:
            return []
        return []

def main():
    client = OpenAI(api_key=OPENAI_API_KEY)
    tasks = load_tasks(JSONL_PATH)
    total = len(tasks)

    prompt_base = load_text(PROMPT_CLASSIFY_PATH)  # single source of truth for the rubric
    print(f"Classifying {total} tasks in batch(es) of up to {BATCH_SIZE}…")

    results: List[Dict[str, Any]] = []
    batches = [tasks[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    for i, batch in enumerate(batches, 1):
        tag = f"{i:02d}"
        out = run_classify_batch(client, CLASSIFIER_ID, batch, tag, prompt_base)
        print(f"Batch {i}: received {len(out)} classified items")
        results.extend(out)

    # Merge by gid (dedupe)
    by_gid: Dict[str, Dict[str, Any]] = {}
    for r in results:
        gid = r.get("gid")
        if gid:
            by_gid[gid] = r

    # Persist final JSON
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    out_path = os.path.join(OUTPUT_DIR, f"classified_tasks_{stamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"tasks": list(by_gid.values())}, f, ensure_ascii=False, indent=2)

    print("----- Classification summary -----")
    print("Input tasks:   ", total)
    print("Classified:    ", len(by_gid))
    print("Output JSON:   ", out_path)
    print("Raw logs saved under:", OUTPUT_DIR)

if __name__ == "__main__":
    main()
