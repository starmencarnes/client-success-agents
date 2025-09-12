# analyze_workload.py
import os
import json
import time
import datetime as dt
import glob
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# -------- Config (shared sheet) --------
SHEET_ID = os.getenv("ASANA_GOOGLE_SHEET_ID")
SHEET_SUMMARY_TAB = "asana_writers_summary"
SA_PATH = "service_account.json"

# -------- OpenAI --------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID   = os.getenv("OPENAI_ASSISTANT_ID")  # pre-created Assistant

# -------- helpers --------
def resolve_path_candidates(path_str: str) -> list[str]:
    """Return a few candidate absolute paths to try for ANALYZE_JSONL_PATH."""
    paths = []
    # as-is
    paths.append(os.path.abspath(path_str))
    # relative to CWD
    paths.append(os.path.abspath(os.path.join(os.getcwd(), path_str)))
    # relative to repo root (../../ from agents/asana-writers)
    repo_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
    paths.append(os.path.abspath(os.path.join(repo_root, path_str.lstrip("/"))))
    return paths

def latest_jsonl(pattern="asana_writer_tasks_*.jsonl") -> str:
    """Find a JSONL file to analyze. Allow ANALYZE_JSONL_PATH override."""
    override = (os.getenv("ANALYZE_JSONL_PATH") or "").strip()
    if override:
        for cand in resolve_path_candidates(override):
            if os.path.exists(cand):
                return cand
        raise SystemExit(f"ANALYZE_JSONL_PATH not found (tried variants): {resolve_path_candidates(override)}")

    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit("No JSONL files found (run the pull step once, or set ANALYZE_JSONL_PATH).")
    return files[-1]

def jsonl_to_json_file(jsonl_path: str) -> str:
    """Convert JSONL -> JSON array file that Assistants can ingest."""
    out_path = os.path.splitext(jsonl_path)[0] + "_upload.json"  # e.g., sample_upload.json
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
        ws = sh.add_worksheet(title=tab_name, rows="500", cols="24")
    return ws

# -------- main --------
def main():
    # sanity
    if not (OPENAI_API_KEY and ASSISTANT_ID and SHEET_ID):
        raise SystemExit("Missing OPENAI_API_KEY, OPENAI_ASSISTANT_ID or ASANA_GOOGLE_SHEET_ID.")

    # 1) get latest JSONL
    jsonl_path = latest_jsonl()
    print(f"Using file: {jsonl_path}")

    # 2) upload to Assistant (convert to .json array first)
    client = OpenAI(api_key=OPENAI_API_KEY)
    upload_path = jsonl_to_json_file(jsonl_path)
    file_obj = client.files.create(file=open(upload_path, "rb"), purpose="assistants")

    # 3) per-run instructions (System Instructions live in the Assistant)
    per_run = """
Analyze tasks due next week (Mon–Sun, America/New_York).
Capacity (minutes/week): {"Dalton Phillips": 1200, "Julia Pizzuto": 1200, "Michaela Leung": 1200, "Germaine Foo": 1200, "Rachel Taylor-Northam": 1200, "Lexa Garian": 1200, "Bethany Osborn": 1200}
Treat “Preview Link sent”, “send to client”, and “sent to press” as Publishing.
Return ONE JSON object only (no markdown, no citations), matching the schema in your System Instructions.
""".strip()

    # 4) create thread, attach file
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Analyze the attached task file.",
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}],
    )

    # 5) run with JSON-only response
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID,
        instructions=per_run,
        response_format={"type": "json_object"},
    )

    # 6) poll to completion (with timeout)
    deadline = time.time() + 300  # 5 minutes
    while True:
        r = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if r.status in ("completed", "failed", "cancelled", "expired"):
            break
        if time.time() > deadline:
            raise SystemExit(f"Assistant run timed out (last status: {r.status})")
        time.sleep(1.2)

    if r.status != "completed":
        raise SystemExit(f"Assistant run did not complete (status={r.status})")

    # 7) fetch assistant message and parse JSON strictly
    msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=10)
    json_text = None
    for m in msgs.data:
        if m.role != "assistant":
            continue
        for c in m.content:
            if c.type == "text":
                json_text = c.text.value.strip()
                break
        if json_text:
            break

    if not json_text:
        raise SystemExit("Assistant returned no text. Expected JSON (response_format=json_object).")

    try:
        json_obj = json.loads(json_text)
    except Exception:
        # save for debugging
        stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        with open(f"assistant_output_{stamp}.txt", "w", encoding="utf-8") as f:
            f.write(json_text)
        raise SystemExit(f"Assistant did not return valid JSON. Saved raw to assistant_output_{stamp}.txt")

    # 8) basic schema guard
    required_top = {"by_assignee", "ranking", "support_plan"}
    missing = [k for k in required_top if k not in json_obj]
    if missing:
        raise SystemExit(f"Assistant JSON missing keys: {missing}")

    # 9) write summary tables to the Sheet
    ws = open_sheet(SHEET_ID, SHEET_SUMMARY_TAB)

    # header for per-assignee stats
    ws.append_row([
        "Assignee",
        "Drafting Tasks","Publishing Tasks","Editing Tasks","Planning Tasks",
        "Drafting Minutes","Publishing Minutes","Editing Minutes","Planning Minutes","Total Minutes",
        "Article","Mini Article","Lead Story","Text Ad","Social Post","Instagram Story","Dedicated Email","Unclear",
        "Capacity","Load Ratio","Deficit","Status",
    ])

    by = json_obj["by_assignee"]
    for assignee, stats in by.items():
        tc = stats.get("task_counts", {})
        mm = stats.get("minutes", {})
        ad = stats.get("by_ad_type", {})
        row = [
            assignee,
            tc.get("Drafting", 0), tc.get("Publishing", 0), tc.get("Editing", 0), tc.get("Planning", 0),
            mm.get("Drafting", 0), mm.get("Publishing", 0), mm.get("Editing", 0), mm.get("Planning", 0), mm.get("Total", 0),
            ad.get("Article", 0), ad.get("Mini Article", 0), ad.get("Lead Story", 0), ad.get("Text Ad", 0),
            ad.get("Social Post", 0), ad.get("Instagram Story", 0), ad.get("Dedicated Email", 0), ad.get("Unclear", 0),
            stats.get("capacity_minutes", 0), stats.get("load_ratio", 0.0), stats.get("deficit_minutes", 0), stats.get("status", ""),
        ]
        ws.append_row(row)

    # ranking table
    ws.append_row([])
    ws.append_row(["Ranking (highest deficit first)"])
    ws.append_row(["#", "Assignee", "Total Minutes", "Capacity", "Deficit", "Load Ratio", "Status"])
    for i, rnk in enumerate(json_obj.get("ranking", []), start=1):
        ws.append_row([
            i,
            rnk.get("assignee", ""),
            rnk.get("total_minutes", 0),
            rnk.get("capacity_minutes", 0),
            rnk.get("deficit_minutes", 0),
            rnk.get("load_ratio", 0.0),
            rnk.get("status", "")
        ])

    # support plan
    ws.append_row([])
    ws.append_row(["Support Plan (suggested rebalancing)"])
    for line in json_obj.get("support_plan", []):
        ws.append_row([line])

    # audit save
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%d")
    with open(f"writer_workload_summary_{stamp}.json", "w", encoding="utf-8") as f:
        json.dump(json_obj, f, ensure_ascii=False, indent=2)

    print(f"Wrote summary to sheet tab '{SHEET_SUMMARY_TAB}' and saved JSON.")

if __name__ == "__main__":
    main()
