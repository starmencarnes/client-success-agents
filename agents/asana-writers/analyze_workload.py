import os, json, datetime as dt, time
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

SHEET_ID = os.getenv("ASANA_GOOGLE_SHEET_ID")
SHEET_SUMMARY_TAB = "asana_writers_summary"
SA_PATH = "service_account.json"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID   = os.getenv("OPENAI_ASSISTANT_ID")  # pre-created Assistant

PROMPT_PATH = os.getenv("PROMPT_PATH", "prompt.txt")
RULES_JSON_PATH = os.getenv("RULES_JSON_PATH", "rules.json")
INCLUDE_HUB_PARENTS = os.getenv("INCLUDE_HUB_PARENTS", "false").lower() == "true"

def latest_jsonl(pattern="data/asana_writer_tasks_*.jsonl"):
    override = os.getenv("ANALYZE_JSONL_PATH")
    if override:
        if not os.path.exists(override):
            raise SystemExit(f"ANALYZE_JSONL_PATH not found: {override}")
        return override
    import glob
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit("No JSONL files found (set ANALYZE_JSONL_PATH or place a file under data/).")
    return files[-1]

def jsonl_to_json_file(jsonl_path: str) -> str:
    out_path = os.path.splitext(jsonl_path)[0] + "_upload.json"
    objs = []
    skipped = 0
    def is_hub_parent(name: str) -> bool:
        # very light heuristic; we keep using LLM rules for final handling
        # This only prunes obvious “container” rows to keep token size down.
        name_low = (name or "").lower()
        bars = "|" in (name or "")
        has_runs_or_posted = (" runs " in name_low) or (" posted " in name_low)
        return bars and has_runs_or_posted and not name_low.startswith(("writer:", "editor:", "ready", "published"))
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not INCLUDE_HUB_PARENTS and is_hub_parent(obj.get("name", "")):
                skipped += 1
                continue
            objs.append(obj)
    with open(out_path, "w", encoding="utf-8") as w:
        json.dump(objs, w, ensure_ascii=False)
    print(f"Prepared upload JSON with {len(objs)} tasks (skipped {skipped} hub parents).")
    return out_path

def open_sheet(sheet_id: str, tab_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name); ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="500", cols="10")
    return ws

def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def main():
    if not (OPENAI_API_KEY and ASSISTANT_ID and SHEET_ID):
        raise SystemExit("Missing OPENAI_API_KEY, OPENAI_ASSISTANT_ID, or ASANA_GOOGLE_SHEET_ID.")

    jsonl_path = latest_jsonl()
    print(f"Using file: {jsonl_path}")
    upload_path = jsonl_to_json_file(jsonl_path)

    prompt_text = load_text(PROMPT_PATH)
    rules_json  = load_text(RULES_JSON_PATH)
    print(f"Attached rules from {os.path.abspath(RULES_JSON_PATH)}")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Upload the compact JSON to the files API for Assistants
    file_obj = client.files.create(file=open(upload_path, "rb"), purpose="assistants")

    # Kick off an Assistant run with your prompt
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=(
            "Analyze the attached tasks JSON for next week using the decision rules. "
            "Rules JSON follows; treat it as authoritative for ad type & effort.\n\n"
            f"RULES JSON:\n{rules_json}\n"
        ),
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}],
    )

    # Use your prompt text as the run instructions
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID,
        instructions=prompt_text
    )

    # Poll to completion
    while True:
        r = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if r.status in ("completed", "failed", "cancelled", "expired"):
            break
        time.sleep(1.2)

    if r.status != "completed":
        raise SystemExit(f"Run status={r.status}")

    # Collect the assistant message text
    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=10)
    parts = []
    for m in msgs.data:
        if m.role == "assistant":
            for c in m.content:
                if c.type == "text" and c.text and c.text.value:
                    parts.append(c.text.value)
    if not parts:
        raise SystemExit("Assistant returned no text.")
    full_text = "\n".join(reversed(parts)).strip()

    # Save and write to Sheet
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    out_txt = f"assistant_narrative_{stamp}.txt"
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(full_text)

    ws = open_sheet(SHEET_ID, SHEET_SUMMARY_TAB)
    ws.append_row(["Narrative"])
    ws.append_row([full_text])
    print("Wrote narrative to sheet tab:", SHEET_SUMMARY_TAB)
    print("Saved narrative:", out_txt)

if __name__ == "__main__":
    main()
