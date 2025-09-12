# analyze_workload.py – narrative-only flow for Slack + Sheet
import os
import json
import time
import datetime as dt
import glob
import re

import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# -------- Config (shared sheet) --------
SHEET_ID = os.getenv("ASANA_GOOGLE_SHEET_ID")
SHEET_SUMMARY_TAB = "asana_writers_summary"
SA_PATH = "service_account.json"

# -------- OpenAI --------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID   = os.getenv("OPENAI_ASSISTANT_ID")  # your pre-created Assistant

# -------- helpers --------
def resolve_path_candidates(path_str: str):
    paths = []
    paths.append(os.path.abspath(path_str))  # as-is
    paths.append(os.path.abspath(os.path.join(os.getcwd(), path_str)))  # relative to CWD
    repo_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
    paths.append(os.path.abspath(os.path.join(repo_root, path_str.lstrip("/"))))  # repo root
    return paths

def latest_jsonl(pattern="asana_writer_tasks_*.jsonl") -> str:
    override = (os.getenv("ANALYZE_JSONL_PATH") or "").strip()
    if override:
        for cand in resolve_path_candidates(override):
            if os.path.exists(cand):
                return cand
        raise SystemExit(f"ANALYZE_JSONL_PATH not found (tried): {resolve_path_candidates(override)}")
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit("No JSONL files found (run the pull step once, or set ANALYZE_JSONL_PATH).")
    return files[-1]

def jsonl_to_json_file(jsonl_path: str) -> str:
    out_path = os.path.splitext(jsonl_path)[0] + "_upload.json"
    objs = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            objs.append(json.loads(line))
    with open(out_path, "w", encoding="utf-8") as w:
        json.dump(objs, w, ensure_ascii=False)
    return out_path

def open_sheet(sheet_id: str, tab_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="200", cols="2")
    return ws

def clean_narrative(text: str) -> str:
    """Strip code fences, citations like 【...】, and extra whitespace for Slack/Sheets."""
    # remove triple-fenced code blocks
    text = re.sub(r"^```[\s\S]*?```", "", text, flags=re.MULTILINE)
    text = text.replace("```", "")
    # remove bracketed citation-looking bits
    text = re.sub(r"【[^】]*】", "", text)
    # collapse overly long blank lines
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

# -------- main --------
def main():
    if not (OPENAI_API_KEY and ASSISTANT_ID and SHEET_ID):
        raise SystemExit("Missing OPENAI_API_KEY, OPENAI_ASSISTANT_ID or ASANA_GOOGLE_SHEET_ID.")

    # 1) Pick the data file and convert for upload
    jsonl_path = latest_jsonl()
    print(f"Using file: {jsonl_path}")
    upload_path = jsonl_to_json_file(jsonl_path)

    # 2) Create client + upload file for assistants
    client = OpenAI(api_key=OPENAI_API_KEY)
    file_obj = client.files.create(file=open(upload_path, "rb"), purpose="assistants")

    # 3) Per-run prompt: ask ONLY for a short, Slack-friendly narrative
    per_run = (
        "You will receive a JSON array of Asana tasks due next week. "
        "Apply your System Instructions to classify and estimate effort. "
        "Return a short Slack-ready memo (no code fences, no citations) with:\n"
        "• Who is overloaded (by approx minutes) and why\n"
        "• 3–6 concrete reassignments for the floating writer (task type + due day + from → to)\n"
        "• Any risks or follow-ups\n"
        "Keep it under ~12 lines, crisp and actionable."
    )

    # 4) Create thread + attach file
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Analyze the attached tasks for next week and propose a support plan.",
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}],
    )

    # 5) Run (text output; do NOT force JSON)
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID,
        instructions=per_run,
        # no response_format here — we want natural text
    )

    # 6) Poll to completion (with timeout)
    deadline = time.time() + 300  # 5 min
    while True:
        r = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if r.status in ("completed", "failed", "cancelled", "expired"):
            break
        if time.time() > deadline:
            raise SystemExit(f"Assistant run timed out (last status: {r.status})")
        time.sleep(1.2)

    if r.status != "completed":
        raise SystemExit(f"Assistant run did not complete (status={r.status})")

    # 7) Fetch the assistant's text message
    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=10)
    narrative = None
    for m in msgs.data:
        if m.role != "assistant":
            continue
        for c in m.content:
            if c.type == "text":
                narrative = c.text.value.strip()
                break
        if narrative:
            break

    if not narrative:
        raise SystemExit("Assistant returned no text.")

    narrative = clean_narrative(narrative)

    # 8) Save narrative to file and to the Sheet
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    with open(f"assistant_narrative_{stamp}.txt", "w", encoding="utf-8") as f:
        f.write(narrative)

    ws = open_sheet(SHEET_ID, SHEET_SUMMARY_TAB)
    ws.append_row([f"Narrative (generated {stamp} UTC)"])
    # Split into ~500-char chunks so a single cell doesn't become unruly
    MAX_CHUNK = 500
    for i in range(0, len(narrative), MAX_CHUNK):
        ws.append_row([narrative[i:i+MAX_CHUNK]])

    print("Wrote narrative to sheet and saved .txt. Ready for Slack paste.")

if __name__ == "__main__":
    main()
