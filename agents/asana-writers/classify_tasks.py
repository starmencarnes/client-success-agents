import os, json, time, datetime as dt, glob
from typing import Any, Dict, List
from openai import OpenAI

# --- config
PROMPT_CLASSIFY_PATH = "prompt_classify.txt"
RULES_PATH = "rules.json"
OUTPUT_DIR = "output"
CLASSIFIED_BASENAME = "classified_tasks"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CLASSIFIER_ID   = os.getenv("OPENAI_CLASSIFIER_ID")   # Assistant ID (Classifier)

def latest_jsonl(default="data/asana_writer_tasks_sample.jsonl") -> str:
    override = os.getenv("ANALYZE_JSONL_PATH", "").strip()
    if override:
        if not os.path.exists(override):
            raise SystemExit(f"ANALYZE_JSONL_PATH not found: {override}")
        return override
    return default if os.path.exists(default) else sorted(glob.glob("*.jsonl"))[-1]

def jsonl_to_json_array(jsonl_path: str) -> List[Dict[str, Any]]:
    arr: List[Dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            arr.append(json.loads(line))
    return arr

def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def poll_until_complete(client: OpenAI, thread_id: str, run_id: str, timeout_s=600):
    t0 = time.time()
    while True:
        run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
        if run.status in ("completed", "failed", "cancelled", "expired"):
            return run
        if time.time() - t0 > timeout_s:
            raise TimeoutError("Assistant run timed out")
        time.sleep(1.2)

def main():
    if not (OPENAI_API_KEY and CLASSIFIER_ID):
        raise SystemExit("Missing OPENAI_API_KEY or OPENAI_CLASSIFIER_ID")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # load input
    jsonl_path = latest_jsonl()
    tasks = jsonl_to_json_array(jsonl_path)
    week_start = os.getenv("WEEK_START", "")
    week_end   = os.getenv("WEEK_END", "")
    upload_payload = {"meta": {"week_start": week_start, "week_end": week_end}, "tasks": tasks}

    # write temp upload json
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    upload_path = f"{OUTPUT_DIR}/tasks_upload_{stamp}.json"
    write_json(upload_path, upload_payload)

    # upload files
    f_tasks = client.files.create(file=open(upload_path, "rb"), purpose="assistants")
    f_rules = client.files.create(file=open(RULES_PATH, "rb"), purpose="assistants")

    # run classifier
    prompt = load_text(PROMPT_CLASSIFY_PATH)
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Classify the attached tasks JSON using the attached rules.json. Return STRICT JSON.",
        attachments=[
            {"file_id": f_tasks.id, "tools": [{"type": "file_search"}]},
            {"file_id": f_rules.id, "tools": [{"type": "file_search"}]},
        ],
    )
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=CLASSIFIER_ID,
        instructions=prompt,
        response_format={"type": "json_object"},
    )
    run = poll_until_complete(client, thread.id, run.id)
    if run.status != "completed":
        raise SystemExit(f"Classifier status={run.status}")

    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=1)
    text = "".join(c.text.value for c in msgs.data[0].content if c.type == "text")
    classified = json.loads(text)  # response_format enforces JSON

    out_path = f"{OUTPUT_DIR}/{CLASSIFIED_BASENAME}_{stamp}.json"
    write_json(out_path, classified)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
