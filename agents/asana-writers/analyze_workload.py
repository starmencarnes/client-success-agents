# analyze_workload.py — Responses API + prompt.txt + rules.json + hub-filter + single-cell narrative
import os
import re
import glob
import json
import datetime as dt
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# quick sanity check (runs at import time)
import sys
print("python:", sys.version)
import openai
print("openai version:", openai.__version__)

# instantiate client
client = OpenAI()

# confirm whether responses is available
print("has .responses?", hasattr(client, "responses"))

# ---------- Config ----------
SHEET_ID = os.getenv("ASANA_GOOGLE_SHEET_ID")
SHEET_SUMMARY_TAB = "asana_writers_summary"
SA_PATH = "service_account.json"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Paths (relative to repo)
PROMPT_PATH = os.getenv("PROMPT_PATH", "agents/asana-writers/prompt.txt")
RULES_JSON_PATH = os.getenv("RULES_JSON_PATH", "agents/asana-writers/rules.json")

# Optional capacity map (JSON string). If missing, we’ll pass a reasonable default.
WRITER_CAPACITY_JSON = os.getenv("WRITER_CAPACITY_JSON", "").strip()

# ---------- Hub container detection ----------
ACTION_RE = re.compile(
    r'\b('
    r'writer|editor|editing|edit(?:ed|ing)?|draft(?:ed|ing)?|writing|write|compose|outline|'
    r'publish(?:ed|ing)?|schedule(?:d)?|plan(?:ning)?|'
    r'preview link|send to client|sent to press|feedback|integrat(?:e|ed)|build|create'
    r')\b',
    re.IGNORECASE
)
HUB_RE = re.compile(r'\b(runs|posted)\b', re.IGNORECASE)
EDITION_RE = re.compile(r'\|\s*[A-Z]{2,5}\s*$', re.IGNORECASE)

def is_hub_container_task(t: dict) -> bool:
    """Top-level 'hub' parents like 'Client | … runs … | RAL' that hold real work in subtasks."""
    if t.get("is_subtask") is True:
        return False
    name = (t.get("name") or "").strip()
    if not name:
        return False
    # actionable words mean it's not a container
    if ACTION_RE.search(name):
        return False
    # pipe title with 'runs' or 'posted'; often ends with edition code like | RAL
    if '|' in name and HUB_RE.search(name):
        if EDITION_RE.search(name) or True:
            return True
    return False

# ---------- File utils ----------
def resolve_path_candidates(path_str: str):
    """Return a few plausible absolute paths for a repo-relative string."""
    paths = []
    cwd = os.getcwd()
    paths.append(os.path.abspath(path_str))
    paths.append(os.path.abspath(os.path.join(cwd, path_str)))
    repo_root = os.path.abspath(os.path.join(cwd, "..", ".."))
    paths.append(os.path.abspath(os.path.join(repo_root, path_str.lstrip("/"))))
    return paths

def latest_jsonl(pattern="asana_writer_tasks_*.jsonl") -> str:
    """Find the newest JSONL or use ANALYZE_JSONL_PATH override."""
    override = (os.getenv("ANALYZE_JSONL_PATH") or "").strip()
    if override:
        for cand in resolve_path_candidates(override):
            if os.path.exists(cand):
                return cand
        raise SystemExit(f"ANALYZE_JSONL_PATH not found (tried): {resolve_path_candidates(override)}")
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit("No JSONL files found (run the pull step first, or set ANALYZE_JSONL_PATH).")
    return files[-1]

def jsonl_to_json_file(jsonl_path: str) -> str:
    """Convert JSONL -> JSON array; filter hub parents unless INCLUDE_HUB_PARENTS=true."""
    include_hub = (os.getenv("INCLUDE_HUB_PARENTS", "false").strip().lower() in ("1", "true", "yes", "on"))
    out_path = os.path.splitext(jsonl_path)[0] + "_upload.json"
    objs, kept, skipped = [], 0, 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            task = json.loads(line)
            if not include_hub and is_hub_container_task(task):
                skipped += 1
                continue
            objs.append(task)
            kept += 1
    with open(out_path, "w", encoding="utf-8") as w:
        json.dump(objs, w, ensure_ascii=False)
    print(f"Prepared upload JSON with {kept} tasks (skipped {skipped} hub parents).")
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
    """Strip code fences, bracketed citations, and excess whitespace for Slack/Sheets."""
    text = re.sub(r"^```[\s\S]*?```", "", text, flags=re.MULTILINE)
    text = text.replace("```", "")
    text = re.sub(r"【[^】]*】", "", text)
    text = "\n".join(line.rstrip() for line in text.splitlines())
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

# ---------- Main ----------
def main():
    if not (OPENAI_API_KEY and SHEET_ID):
        raise SystemExit("Missing OPENAI_API_KEY or ASANA_GOOGLE_SHEET_ID.")

    # 1) Find data file and convert for upload (with hub filtering)
    jsonl_path = latest_jsonl()
    print(f"Using file: {jsonl_path}")
    upload_path = jsonl_to_json_file(jsonl_path)

    # 2) Load prompt.txt (system instructions)
    prompt_text = None
    for cand in resolve_path_candidates(PROMPT_PATH):
        if os.path.exists(cand):
            with open(cand, "r", encoding="utf-8") as f:
                prompt_text = f.read()
            break
    if not prompt_text:
        raise SystemExit(f"prompt.txt not found (tried): {resolve_path_candidates(PROMPT_PATH)}")

    # Append capacity to the prompt if provided, else a sensible default
    capacity_block = ""
    if WRITER_CAPACITY_JSON:
        capacity_block = f'\n\nCapacity (minutes/week): {WRITER_CAPACITY_JSON}'
    else:
        # Default (edit here if you want different per-writer caps)
        default_capacity = {
            "Dalton Phillips": 1200,
            "Julia Pizzuto": 1200,
            "Michaela Leung": 1200,
            "Germaine Foo": 1200,
            "Rachel Taylor-Northam": 1200,
            "Lexa Garian": 1200,
            "Bethany Osborn": 1200
        }
        capacity_block = f"\n\nCapacity (minutes/week): {json.dumps(default_capacity)}"

    instructions = prompt_text + capacity_block

    # 3) OpenAI client + uploads
    client = OpenAI(api_key=OPENAI_API_KEY)

    task_file = client.files.create(file=open(upload_path, "rb"), purpose="assistants")

    rules_file = None
    for cand in resolve_path_candidates(RULES_JSON_PATH):
        if os.path.exists(cand):
            rules_file = client.files.create(file=open(cand, "rb"), purpose="assistants")
            print(f"Attached rules from {cand}")
            break

    attachments = [{"file_id": task_file.id, "tools": [{"type": "file_search"}]}]
    if rules_file:
        attachments.append({"file_id": rules_file.id, "tools": [{"type": "file_search"}]})

    # 4) Responses API call
    resp = client.responses.create(
        model=OPENAI_MODEL,
        tools=[{"type": "file_search"}],
        attachments=attachments,
        instructions=instructions,
    )

    # 5) Extract plain text from Responses output
    narrative = ""
    if hasattr(resp, "output") and resp.output:
        for item in resp.output:
            if getattr(item, "type", "") == "message":
                for c in getattr(item, "content", []):
                    if getattr(c, "type", "") == "output_text":
                        narrative = c.text or ""
                        break
            if narrative:
                break
    if not narrative:
        # Fallback for SDKs that expose output_text
        narrative = getattr(resp, "output_text", "") or ""
    if not narrative:
        raise SystemExit("No text returned from Responses API.")

    narrative = clean_narrative(narrative)

    # 6) Save + write to Sheet (single cell)
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    txt_path = f"assistant_narrative_{stamp}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(narrative)

    ws = open_sheet(SHEET_ID, SHEET_SUMMARY_TAB)
    ws.append_row([f"Narrative (generated {stamp} UTC)"])
    ws.update("A2", [[narrative]])  # entire memo in one cell

    print("Wrote narrative to sheet and saved:", txt_path)

if __name__ == "__main__":
    main()
