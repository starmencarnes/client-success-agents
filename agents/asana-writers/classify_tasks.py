#!/usr/bin/env python3
import os
import json
import time
import glob
import datetime as dt
from pathlib import Path
from openai import OpenAI

# -------- Config / env --------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CLASSIFIER_ASSISTANT_ID = os.getenv("OPENAI_CLASSIFIER_ID") or os.getenv("OPENAI_ASSISTANT_ID")
PROMPT_PATH = os.getenv("PROMPT_PATH", "prompt.txt")          # optional (you can keep all rules in the assistant)
RULES_JSON_PATH = os.getenv("RULES_JSON_PATH", "rules.json")   # optional (if you embed rules via content)
INCLUDE_HUB_PARENTS = os.getenv("INCLUDE_HUB_PARENTS", "false").lower() == "true"

# I/O locations
DATA_DIR = Path(__file__).parent / "data"
OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)


# -------- Helpers --------
def latest_jsonl(pattern="data/asana_writer_tasks_*.jsonl") -> str:
    """Return override path (ANALYZE_JSONL_PATH) or the newest JSONL under data/."""
    override = os.getenv("ANALYZE_JSONL_PATH", "").strip()
    if override:
        if not os.path.exists(override):
            raise SystemExit(f"ANALYZE_JSONL_PATH not found: {override}")
        return override
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit("No JSONL files found (set ANALYZE_JSONL_PATH or place a file under data/).")
    return files[-1]


def is_hub_parent(name: str) -> bool:
    """
    Light heuristic to skip top-level container rows like:
      '<Client> | <Thing> runs <date> | <EDITION>' or '... posted ... | <EDITION>'
    """
    n = (name or "")
    low = n.lower()
    bars = "|" in n
    has_runs_or_posted = (" runs " in low) or (" posted " in low)
    actionable_prefix = low.startswith(("writer:", "editor:", "ready", "published", "planner:", "planning:", "preview link", "send to client"))
    return bars and has_runs_or_posted and not actionable_prefix


def jsonl_to_json_file(jsonl_path: str) -> str:
    """Convert JSONL to a single JSON array, pruning hub parents unless opted in."""
    out_path = os.path.splitext(jsonl_path)[0] + "_upload.json"
    objs, skipped = [], 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            obj = json.loads(s)
            if not INCLUDE_HUB_PARENTS and is_hub_parent(obj.get("name", "")):
                skipped += 1
                continue
            objs.append(obj)

    with open(out_path, "w", encoding="utf-8") as w:
        json.dump(objs, w, ensure_ascii=False)
    print(f"Prepared upload JSON with {len(objs)} tasks (skipped {skipped} hub parents).")
    return out_path


def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def save_debug_blob(prefix: str, content: str) -> str:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    path = OUT_DIR / f"{prefix}_{ts}.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content or "")
    print(f"[debug] saved assistant raw to {path}")
    return str(path)


def save_json(prefix: str, obj) -> str:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    path = OUT_DIR / f"{prefix}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"[debug] saved JSON to {path}")
    return str(path)


# -------- Main --------
def main():
    if not OPENAI_API_KEY or not CLASSIFIER_ASSISTANT_ID:
        raise SystemExit("Missing OPENAI_API_KEY or OPENAI_CLASSIFIER_ASSISTANT_ID (or OPENAI_ASSISTANT_ID).")

    # Input prep
    jsonl_path = latest_jsonl("data/asana_writer_tasks_*.jsonl")
    print(f"Using file: {jsonl_path}")
    upload_path = jsonl_to_json_file(jsonl_path)

    # Optional prompt/rules—if you want to keep logic in code rather than the Assistant
    prompt_text = None
    if os.path.exists(PROMPT_PATH):
        prompt_text = load_text(PROMPT_PATH)
        print(f"Loaded prompt from {PROMPT_PATH}")
    rules_json = None
    if os.path.exists(RULES_JSON_PATH):
        rules_json = load_text(RULES_JSON_PATH)
        print(f"Attached rules from {os.path.abspath(RULES_JSON_PATH)}")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Upload compact JSON as a file for the Assistant
    file_obj = client.files.create(file=open(upload_path, "rb"), purpose="assistants")

    # Create thread and send user message with the attachment + optional rules
    thread = client.beta.threads.create()
    user_content = "Classify the attached tasks and return strict JSON."
    if rules_json:
        user_content += "\n\nRULES JSON (authoritative for ad type & effort):\n" + rules_json

    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=user_content,
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}],
    )

    # Launch the run (optionally pass extra instructions from prompt.txt)
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=CLASSIFIER_ASSISTANT_ID,
        instructions=prompt_text or None,
        # response_format={"type": "json_object"}  # Assistants API may not support this flag yet everywhere
    )

    # Poll to completion
    while True:
        r = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if r.status in ("completed", "failed", "cancelled", "expired"):
            break
        time.sleep(1.2)

    print(f"[debug] run status = {r.status}")
    if getattr(r, "last_error", None):
        print("[debug] last_error:", r.last_error)

    if r.status != "completed":
        # Try to dump step errors for more context
        try:
            steps = client.beta.threads.runs.steps.list(thread_id=thread.id, run_id=run.id)
            for st in steps.data:
                print(f"[debug] step: {st.type} status={st.status}")
                if getattr(st, "last_error", None):
                    print("        last_error:", st.last_error)
        except Exception:
            pass
        raise SystemExit(f"Assistant run did not complete: status={r.status}")

    # Collect assistant text content
    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=10)
    parts = []
    for m in msgs.data:
        if m.role != "assistant":
            continue
        for c in m.content:
            if c.type == "text" and getattr(c.text, "value", ""):
                parts.append(c.text.value)
            elif c.type != "text":
                # Helpful to see if we accidentally received non-text tool output
                print(f"[debug] assistant non-text content type: {c.type}")

    assistant_text = "\n".join(reversed(parts)).strip()
    raw_path = save_debug_blob("assistant_raw", assistant_text)

    # Try to parse JSON; if it fails, save an error JSON to keep pipeline alive
    try:
        classified = json.loads(assistant_text)
    except Exception as e:
        print("[error] JSON parse failed:", repr(e))
        print("[error] First 300 chars of assistant output:")
        print(assistant_text[:300])
        classified = {
            "error": "invalid_json",
            "message": str(e),
            "raw_file": raw_path
        }

    out_path = save_json("classified_tasks", classified)
    print("Done. Classified tasks written to:", out_path)


if __name__ == "__main__":
    main()
