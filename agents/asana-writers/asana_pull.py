import os
import time
import json
import math
import logging
import datetime as dt
from typing import Dict, List, Any, Optional
import requests
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

# ------------- CONFIG -------------
load_dotenv()

ASANA_TOKEN = os.getenv("ASANA_TOKEN")  # Personal Access Token
ASANA_WORKSPACE_GID = os.getenv("ASANA_WORKSPACE_GID")  # e.g. "1234567890"
GOOGLE_SHEET_ID = os.getenv("ASANA_GOOGLE_SHEET_ID")  # The spreadsheet ID (not the name)

SHEET_NAME = "This Week's Writer Tasks"
ASSIGNEE_GIDS: Dict[str, str] = {
    "Dalton Phillips": "1205690156575999",
    "Julia Pizzuto": "1209238639668940",
    "Michaela Leung": "1200917162428165",
    "Germaine Foo": "1206510876664915",
    "Rachel Taylor-Northam": "1169606456113439",
    "Lexa Garian": "1206455589639247",
    "Bethany Osborn": "1207757806946425",
}

OPT_FIELDS = ",".join([
    "name",
    "assignee.name",
    "assignee.gid",
    "due_on",
    "completed",
    "projects.name",
    "parent.name",
    "gid",
    "permalink_url",
    "notes",
    "is_subtask",
])

PAGE_LIMIT = 100
ASANA_BASE = "https://app.asana.com/api/1.0"
HEADERS = {
    "Authorization": f"Bearer {ASANA_TOKEN}",
    "Accept": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s  %(message)s",
)

# ------------- DATE HELPERS -------------

def week_bounds(offset_weeks: int = 0, tz: dt.tzinfo = dt.timezone.utc) -> tuple[dt.date, dt.date]:
    """Return Monday..Sunday for the week offset from the current week.
    offset_weeks=0 → this week, 1 → next week, -1 → last week."""
    today = dt.datetime.now(tz).date()
    dow = today.weekday()               # 0 = Monday
    monday_this = today - dt.timedelta(days=dow)
    monday = monday_this + dt.timedelta(weeks=offset_weeks)
    sunday = monday + dt.timedelta(days=6)
    return monday, sunday


def daterange(start_date: dt.date, end_date: dt.date):
    d = start_date
    while d <= end_date:
        yield d
        d += dt.timedelta(days=1)

# ------------- GOOGLE SHEETS -------------
def open_sheet(sheet_id: str, tab_name: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="100", cols="20")
    return ws

# ------------- ASANA HTTP + PAGINATION -------------
def fetch_with_retry(url: str, max_attempts: int = 5) -> requests.Response:
    backoff = 1.0
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code == 429 and attempt < max_attempts:
            retry_after = float(resp.headers.get("Retry-After", "2"))
            time.sleep(max(1.0, retry_after))
            continue
        if 500 <= resp.status_code < 600 and attempt < max_attempts:
            time.sleep(backoff)
            backoff = min(backoff * 2, 10.0)
            continue
        return resp
    return resp

def build_url(path: str, params: Dict[str, Any]) -> str:
    from urllib.parse import urlencode
    q = {k: v for k, v in params.items() if v not in (None, "", [])}
    return f"{ASANA_BASE}{path}?{urlencode(q)}"

def fetch_tasks_for_day(
    day_iso: str,
    assignee_any: Optional[str],
    is_subtask: Optional[bool],
) -> List[Dict[str, Any]]:
    """
    Call /workspaces/{workspace_gid}/tasks/search with:
      - due_on = day_iso
      - assignee.any = assignee_any (gid or 'me'), if provided
      - is_subtask = true/false, if provided
      - limit = 100
      - opt_fields pre-defined
    Use next_page.uri if present, else fall back to offset.
    """
    params = {
        "due_on": day_iso,
        "sort_by": "due_date",
        "sort_ascending": True,
        "limit": PAGE_LIMIT,
        "opt_fields": OPT_FIELDS,
    }
    if assignee_any:
        params["assignee.any"] = assignee_any
    if is_subtask is not None:
        params["is_subtask"] = str(is_subtask).lower()

    url = build_url(f"/workspaces/{ASANA_WORKSPACE_GID}/tasks/search", params)
    out: List[Dict[str, Any]] = []
    page = 0

    while True:
        resp = fetch_with_retry(url)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(f"Asana error {resp.status_code}: {resp.text}")

        body = resp.json()
        data = body.get("data", [])
        out.extend(data)
        page += 1
        logging.info("Asana day %s subtask=%s page %d fetched %d (total %d)",
                     day_iso, is_subtask, page, len(data), len(out))

        next_page = body.get("next_page")
        if not next_page:
            break
        if next_page.get("uri"):
            url = next_page["uri"]
        elif next_page.get("offset"):
            # rebuild url with offset
            params["offset"] = next_page["offset"]
            url = build_url(f"/workspaces/{ASANA_WORKSPACE_GID}/tasks/search", params)
        else:
            break

        # polite pacing
        time.sleep(0.1)

    return out

# ------------- MAIN PULL -------------
def pull_writer_tasks_next_week() -> List[Dict[str, Any]]:
    monday, sunday = week_bounds(0)
    results: List[Dict[str, Any]] = []

    for d in daterange(monday, sunday):
        day_iso = d.isoformat()
        for friendly_name, gid in ASSIGNEE_GIDS.items():
            parents = fetch_tasks_for_day(day_iso, assignee_any=gid, is_subtask=False)
            subs    = fetch_tasks_for_day(day_iso, assignee_any=gid, is_subtask=True)

            # Merge + de-dupe by gid
            by_gid: Dict[str, Dict[str, Any]] = {}
            for t in [*parents, *subs]:
                if t and t.get("gid"):
                    by_gid[t["gid"]] = t

            # annotate with our friendly assignee (helps when Asana name differs)
            for t in by_gid.values():
                if t.get("assignee") and t["assignee"].get("gid") == gid:
                    t["_assignee_friendly"] = friendly_name
                else:
                    # still store for context
                    t["_assignee_friendly"] = friendly_name

            logging.info("Day %s · %s: parents %d, subs %d, merged %d",
                         day_iso, friendly_name, len(parents), len(subs), len(by_gid))

            results.extend(by_gid.values())

    return results

# ------------- WRITE SHEET + JSONL -------------
def write_to_sheet(rows: List[List[Any]]):
    ws = open_sheet(GOOGLE_SHEET_ID, SHEET_NAME)
    header = [
        "Task Name", "Assignee", "Due Date", "Completed",
        "Project(s)", "Parent Task", "Task GID", "Task URL",
        "Assignee GID", "Assignee (Friendly)"
    ]
    ws.append_row(header)

    # gspread supports batch update via .append_rows
    ws.append_rows(rows, value_input_option="RAW")

def format_rows(tasks: List[Dict[str, Any]]) -> List[List[Any]]:
    out = []
    for t in tasks:
        name = (t.get("name") or "")
        assignee_name = (t.get("assignee") or {}).get("name") or ""
        assignee_gid  = (t.get("assignee") or {}).get("gid") or ""
        due_on = t.get("due_on") or ""
        completed = bool(t.get("completed"))
        projects = ", ".join([p.get("name") for p in (t.get("projects") or []) if p and p.get("name")])
        parent_name = ((t.get("parent") or {}).get("name") or "")
        gid = t.get("gid") or ""
        url = t.get("permalink_url") or ""
        friendly = t.get("_assignee_friendly") or ""
        out.append([
            name, assignee_name, due_on, completed, projects, parent_name, gid, url, assignee_gid, friendly
        ])
    return out

def save_jsonl(tasks: List[Dict[str, Any]], path: str):
    with open(path, "w", encoding="utf-8") as f:
        for t in tasks:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    logging.info("Saved JSONL: %s", path)

# ------------- CLI -------------
if __name__ == "__main__":
    # basic sanity
    if not ASANA_TOKEN or not ASANA_WORKSPACE_GID or not GOOGLE_SHEET_ID:
        raise SystemExit("Missing ASANA_TOKEN, ASANA_WORKSPACE_GID, or ASANA_GOOGLE_SHEET_ID in .env")

    tasks = pull_writer_tasks_next_week()

    # Write Google Sheet
    rows = format_rows(tasks)
    write_to_sheet(rows)

    # Save JSONL (for your GPT assistant)
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%d")
    save_jsonl(tasks, f"asana_writer_tasks_{stamp}.jsonl")

    logging.info("Done. Wrote %d tasks to sheet '%s' and JSONL.", len(rows), SHEET_NAME)
