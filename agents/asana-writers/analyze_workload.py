import os, glob, json, time, datetime as dt
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# -------- Config (shared sheet) --------
SHEET_ID = os.getenv("ASANA_GOOGLE_SHEET_ID")
SHEET_SUMMARY_TAB = "asana_writers_summary"   # new tab for the summary
SA_PATH = "service_account.json"

# -------- OpenAI (Assistant you already created) --------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID   = os.getenv("OPENAI_ASSISTANT_ID")  # pre-created Assistant

# -------- helpers --------
def latest_jsonl(pattern="asana_writer_tasks_*.jsonl"):
    # Allow override via env for testing
    override = os.getenv("ANALYZE_JSONL_PATH")
    if override:
        if not os.path.exists(override):
            raise SystemExit(f"ANALYZE_JSONL_PATH not found: {override}")
        return override

    import glob
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit("No JSONL files found (run the pull step once, or set ANALYZE_JSONL_PATH).")
    return files[-1]


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

# -------- main --------
def main():
    if not (OPENAI_API_KEY and ASSISTANT_ID and SHEET_ID):
        raise SystemExit("Missing OPENAI_API_KEY, OPENAI_ASSISTANT_ID or ASANA_GOOGLE_SHEET_ID.")

    # 1) get latest JSONL
    jsonl_path = latest_jsonl()
    print(f"Using file: {jsonl_path}")

    # 2) upload to Assistant and run
    client = OpenAI(api_key=OPENAI_API_KEY)

    file_obj = client.files.create(
        file=open(jsonl_path, "rb"),
        purpose="assistants"
    )

    # prompt: ask for strict JSON plus a short narrative
    instructions = """
You are a production editor’s assistant. The attached file is JSONL: one Asana task per line.
For each task, classify:
- category: Drafting | Publishing | Editing | Other (based ONLY on the subtask or parent tasks's own name/notes)
- ad_type: Article | Mini Article | Lead Story | Text Ad | Social Post | Instagram Story | Dedicated Email | Unclear
- effort_minutes: integer estimate (defaults: Article 90, Mini 45, Lead Story 120, Social 20, Text Ad 10, Dedicated Email 120; Editing 10-30; Publishing 10)

Aggregate by assignee:
- drafting_tasks, publishing_tasks, editing_tasks, drafting_minutes, publishing_minutes; editing_minutes; total minutes
- by_ad_type counts

Return two things:
1) JSON (strict, one object; no markdown code fence) with the shape:
{
  "by_assignee": {
    "<assignee_name>": {
      "drafting_tasks": 0,
      "planning_tasks": 0,
      "drafting_minutes": 0,
      "total_minutes": 0,
      "by_ad_type": {"Article": 0, "Mini Article": 0, "Lead Story": 0, "Text Ad": 0, "Social Post": 0, "Instagram Story": 0, "Dedicated Email": 0, "Unclear": 0}
    }
  },
  "recommendations": [ "short staffing notes ..." ]
}
2) A short 5-8 line narrative summary for a floating copywriter with concrete suggestions on what to take over (who is overloaded).
"""

    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Analyze the attached JSONL of tasks due next week.",
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}],
    )
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID,
        instructions=instructions
    )

    # poll to completion
    while True:
        r = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if r.status in ("completed", "failed", "cancelled", "expired"):
            break
        time.sleep(1.2)

    if r.status != "completed":
        raise SystemExit(f"Run status={r.status}")

    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=5)
    # Expect first assistant message to contain both JSON and narrative; split by first '{...}' block
    text_chunks = []
    for m in msgs.data:
        if m.role == "assistant":
            for c in m.content:
                if c.type == "text":
                    text_chunks.append(c.text.value)
    if not text_chunks:
        raise SystemExit("Assistant returned no text.")

    full_text = "\n".join(text_chunks).strip()

    # Try to extract leading JSON object
    json_obj = None
    narrative = full_text
    try:
        # Find the first '{'..matching '}' span
        first_brace = full_text.find("{")
        last_brace  = full_text.rfind("}")
        if first_brace != -1 and last_brace != -1:
            possible = full_text[first_brace:last_brace+1]
            json_obj = json.loads(possible)
            narrative = (full_text[last_brace+1:]).strip()
    except Exception:
        pass

    stamp = dt.datetime.utcnow().strftime("%Y-%m-%d")
    # Save raw assistant text (for audit)
    with open(f"assistant_output_{stamp}.txt", "w", encoding="utf-8") as f:
        f.write(full_text)

    if json_obj:
        with open(f"writer_workload_summary_{stamp}.json", "w", encoding="utf-8") as f:
            json.dump(json_obj, f, ensure_ascii=False, indent=2)

        # Write summary to a Sheet tab
        ws = open_sheet(SHEET_ID, SHEET_SUMMARY_TAB)
        ws.append_row(["Assignee", "Drafting Tasks", "Planning Tasks", "Drafting Minutes", "Total Minutes",
                       "Article","Mini Article","Lead Story","Email Header","Email Banner","Social","Display","Podcast","Unclear"])

        by = json_obj.get("by_assignee", {})
        for assignee, stats in by.items():
            ad = stats.get("by_ad_type", {})
            row = [
                assignee,
                stats.get("drafting_tasks", 0),
                stats.get("planning_tasks", 0),
                stats.get("drafting_minutes", 0),
                stats.get("total_minutes", 0),
                ad.get("Article", 0),
                ad.get("Mini Article", 0),
                ad.get("Lead Story", 0),
                ad.get("Email Header", 0),
                ad.get("Email Banner", 0),
                ad.get("Social", 0),
                ad.get("Display", 0),
                ad.get("Podcast", 0),
                ad.get("Unclear", 0),
            ]
            ws.append_row(row)

        # Put the narrative at the bottom (easy to read in the Sheet)
        ws.append_row([])
        ws.append_row(["Narrative"])
        ws.append_row([narrative or "(no narrative)"])
        print("Wrote summary + narrative to sheet tab:", SHEET_SUMMARY_TAB)
    else:
        print("Could not parse JSON from the assistant. Raw text saved to assistant_output.txt")
        # Still write narrative to a one-cell sheet so you see something
        ws = open_sheet(SHEET_ID, SHEET_SUMMARY_TAB)
        ws.append_row(["Narrative only (JSON parse failed)"])
        ws.append_row([full_text])

if __name__ == "__main__":
    main()
