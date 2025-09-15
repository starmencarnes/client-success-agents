import os, json, time, datetime as dt, glob
from typing import Any, Dict
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUMMARIZER_ID  = os.getenv("OPENAI_SUMMARIZER_ID")
SHEET_ID       = os.getenv("ASANA_GOOGLE_SHEET_ID")
SA_PATH        = "service_account.json"
SUMMARY_TAB    = "asana_writers_summary"
OUTPUT_DIR     = "output"

CAPACITY_JSON = os.getenv("CAPACITY_JSON", "")  # optional: {"Name": minutes, ...}

def open_sheet(sheet_id: str, tab_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name); ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="200", cols="20")
    return ws

def find_latest_classified() -> str:
    candidates = sorted(glob.glob(f"{OUTPUT_DIR}/classified_tasks_*.json"))
    if not candidates:
        raise SystemExit("No classified_tasks_*.json found. Run classify_tasks.py first.")
    return candidates[-1]

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
    if not (OPENAI_API_KEY and SUMMARIZER_ID and SHEET_ID):
        raise SystemExit("Missing OPENAI_API_KEY, OPENAI_SUMMARIZER_ID or ASANA_GOOGLE_SHEET_ID")

    client = OpenAI(api_key=OPENAI_API_KEY)

    classified_path = find_latest_classified()
    f_classified = client.files.create(file=open(classified_path, "rb"), purpose="assistants")

    extra_instr = ""
    if CAPACITY_JSON:
        extra_instr = f"Use this capacity map (minutes/week): {CAPACITY_JSON}"

    prompt = open("prompt_summarize.txt", "r", encoding="utf-8").read()

    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Summarize the attached classified tasks into a Slack message.",
        attachments=[{"file_id": f_classified.id, "tools": [{"type": "file_search"}]}],
    )
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=SUMMARIZER_ID,
        instructions=prompt + ("\n\n" + extra_instr if extra_instr else ""),
    )
    run = poll_until_complete(client, thread.id, run.id)
    if run.status != "completed":
        raise SystemExit(f"Summarizer status={run.status}")

    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=1)
    message_text = "".join(c.text.value for c in msgs.data[0].content if c.type == "text").strip()

    # Save txt artifact
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    txt_path = f"{OUTPUT_DIR}/writer_support_{stamp}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(message_text)

    # Write to sheet (cell A1)
    ws = open_sheet(SHEET_ID, SUMMARY_TAB)
    ws.update("A1", [[message_text]])
    print(f"Wrote Slack message to sheet '{SUMMARY_TAB}' and saved {txt_path}")

if __name__ == "__main__":
    main()
