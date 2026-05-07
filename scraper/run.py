import os, json, time, hashlib, logging
from datetime import datetime, timezone
import requests, gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

APIFY_TOKEN    = os.environ.get("APIFY_TOKEN", "")
SHEET_ID       = os.environ.get("GOOGLE_SHEET_ID", "")
GCP_CREDS_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
KEYWORDS_PATH  = "config/keywords.json"
MAX_RESULTS    = 50
POLL_INTERVAL  = 10
TIMEOUT        = 600

SHEET_HEADERS = ["post_id","platform","username","text","likes","replies","reposts","url","posted_at","keyword","scraped_at"]

def load_keywords():
    with open(KEYWORDS_PATH, encoding="utf-8") as f:
        kw_config = json.load(f)
    keywords = [kw for group in kw_config.values() for kw in group]
    log.info(f"Loaded {len(keywords)} keywords")
    return keywords

def make_id(raw_id):
    return hashlib.md5(f"threads:{raw_id}".encode()).hexdigest()

def apify_run(actor_input):
    url = "https://api.apify.com/v2/acts/automation-lab~threads-scraper/runs"
    r = requests.post(url, params={"token": APIFY_TOKEN}, json=actor_input, timeout=30)
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    log.info(f"Run started: {run_id}")
    return run_id

def apify_wait(run_id):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        try:
            r = requests.get(url, params={"token": APIFY_TOKEN}, timeout=30)
            if r.status_code != 200:
                log.warning(f"HTTP {r.status_code}, retrying...")
                time.sleep(POLL_INTERVAL)
                continue
            text = r.text.strip()
            if not text:
                log.warning("Empty response, retrying...")
                time.sleep(POLL_INTERVAL)
                continue
            data = r.json()["data"]
            status = data["status"]
            log.info(f"Status: {status}")
            if status == "SUCCEEDED":
                return data["defaultDatasetId"]
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                log.error(f"Run ended with {status}")
                return None
        except Exception as e:
            log.warning(f"Poll error: {e}, retrying...")
        time.sleep(POLL_INTERVAL)
    log.error("Timeout waiting for run")
    return None

def apify_fetch(dataset_id):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    r = requests.get(url, params={"token": APIFY_TOKEN, "clean": True}, timeout=60)
    r.raise_for_status()
    return r.json()

def get_or_create_sheet():
    creds = Credentials.from_service_account_info(
        json.loads(GCP_CREDS_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        ws = sh.worksheet(today)
        log.info(f"Using tab: {today}")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=today, rows=5000, cols=len(SHEET_HEADERS))
        ws.append_row(SHEET_HEADERS, value_input_option="RAW")
        log.info(f"Created tab: {today}")
    return ws

def main():
    log.info("=== START ===")
    keywords = load_keywords()
    run_id = apify_run({
        "mode": "search",
        "searchQueries": keywords,
        "maxPosts": MAX_RESULTS,
    })
    dataset_id = apify_wait(run_id)
    if not dataset_id:
        log.error("No dataset, exiting")
        return
    items = apify_fetch(dataset_id)
    log.info(f"Fetched {len(items)} posts")
    ws = get_or_create_sheet()
    existing = set(ws.col_values(1)[1:])
    rows = []
    for i in items:
        pid = make_id(i.get("postId", ""))
        if pid in existing:
            continue
        rows.append([pid,"threads",i.get("username",""),i.get("text",""),
            i.get("likeCount",0),i.get("replyCount",0),i.get("repostCount",0),
            i.get("url",""),i.get("date",""),",".join(keywords[:3]),
            datetime.now(timezone.utc).isoformat()])
    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        log.info(f"Pushed {len(rows)} rows")
    else:
        log.info("No new rows")
    log.info("=== DONE ===")

if __name__ == "__main__":
    main()
