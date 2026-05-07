import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

APIFY_TOKEN    = os.environ.get("APIFY_TOKEN", "")
SHEET_ID       = os.environ.get("GOOGLE_SHEET_ID", "")
GCP_CREDS_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
KEYWORDS_PATH  = "config/keywords.json"
MAX_RESULTS    = 50
POLL_INTERVAL  = 5
TIMEOUT        = 300

SHEET_HEADERS = [
    "post_id", "platform", "username", "text",
    "likes", "replies", "reposts", "url",
    "posted_at", "keyword", "scraped_at",
]

def check_env():
    missing = []
    if not APIFY_TOKEN:    missing.append("APIFY_TOKEN")
    if not SHEET_ID:       missing.append("GOOGLE_SHEET_ID")
    if not GCP_CREDS_JSON: missing.append("GCP_SERVICE_ACCOUNT_JSON")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")
    log.info("All env vars present")

def load_keywords():
    with open(KEYWORDS_PATH, encoding="utf-8") as f:
        kw_config = json.load(f)
    keywords = [kw for group in kw_config.values() for kw in group]
    log.info(f"Loaded {len(keywords)} keywords from {KEYWORDS_PATH}")
    return keywords

def post_id(platform, raw_id):
    return hashlib.md5(f"{platform}:{raw_id}".encode()).hexdigest()

def apify_run(actor_id, actor_input):
    url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
    r = requests.post(url, params={"token": APIFY_TOKEN}, json=actor_input, timeout=30)
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    log.info(f"[{actor_id}] started run {run_id}")
    return run_id

def apify_wait(run_id, actor_id):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        r = requests.get(url, params={"token": APIFY_TOKEN}, timeout=15)
        data = r.json()["data"]
        status = data["status"]
        if status == "SUCCEEDED":
            log.info(f"[{actor_id}] SUCCEEDED -> dataset {data['defaultDatasetId']}")
            return data["defaultDatasetId"]
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            log.error(f"[{actor_id}] ended with {status}")
            return None
        time.sleep(POLL_INTERVAL)
    log.error(f"[{actor_id}] timeout")
    return None

def apify_fetch(dataset_id):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    r = requests.get(url, params={"token": APIFY_TOKEN, "clean": True}, timeout=60)
    r.raise_for_status()
    return r.json()

def scrape_threads(keywords):
    run_id = apify_run("automation-lab/threads-scraper", {
        "mode": "search",
        "searchQueries": keywords,
        "maxPosts": MAX_RESULTS,
    })
    dataset_id = apify_wait(run_id, "threads")
    if not dataset_id:
        return []
    items = apify_fetch(dataset_id)
    log.info(f"[threads] {len(items)} items")
    return [{
        "post_id":    post_id("threads", i.get("postId", "")),
        "platform":   "threads",
        "username":   i.get("username", ""),
        "text":       i.get("text", ""),
        "likes":      i.get("likeCount", 0),
        "replies":    i.get("replyCount", 0),
        "reposts":    i.get("repostCount", 0),
        "url":        i.get("url", ""),
        "posted_at":  i.get("date", ""),
        "keyword":    ",".join(keywords[:3]),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    } for i in items]

def scrape_facebook(keywords):
    run_id = apify_run("apify/facebook-posts-scraper", {
        "query": " OR ".join(keywords[:5]),
        "maxPosts": MAX_RESULTS,
    })
    dataset_id = apify_wait(run_id, "facebook")
    if not dataset_id:
        return []
    items = apify_fetch(dataset_id)
    log.info(f"[facebook] {len(items)} items")
    return [{
        "post_id":    post_id("facebook", i.get("postId", i.get("id", ""))),
        "platform":   "facebook",
        "username":   i.get("pageName", i.get("authorName", "")),
        "text":       i.get("text", i.get("message", "")),
        "likes":      i.get("likes", 0),
        "replies":    i.get("comments", 0),
        "reposts":    i.get("shares", 0),
        "url":        i.get("url", ""),
        "posted_at":  i.get("time", ""),
        "keyword":    ",".join(keywords[:3]),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    } for i in items]

def scrape_tiktok(keywords):
    run_id = apify_run("clockworks/tiktok-scraper", {
        "searchQueries": keywords[:5],
        "maxItems": MAX_RESULTS,
    })
    dataset_id = apify_wait(run_id, "tiktok")
    if not dataset_id:
        return []
    items = apify_fetch(dataset_id)
    log.info(f"[tiktok] {len(items)} items")
    return [{
        "post_id":    post_id("tiktok", i.get("id", "")),
        "platform":   "tiktok",
        "username":   i.get("authorMeta", {}).get("name", ""),
        "text":       i.get("text", ""),
        "likes":      i.get("diggCount", 0),
        "replies":    i.get("commentCount", 0),
        "reposts":    i.get("shareCount", 0),
        "url":        i.get("webVideoUrl", ""),
        "posted_at":  i.get("createTimeISO", ""),
        "keyword":    ",".join(keywords[:3]),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    } for i in items]

def scrape_youtube(keywords):
    run_id = apify_run("streamers/youtube-scraper", {
        "searchKeywords": keywords[:5],
        "maxResults": MAX_RESULTS,
    })
    dataset_id = apify_wait(run_id, "youtube")
    if not dataset_id:
        return []
    items = apify_fetch(dataset_id)
    log.info(f"[youtube] {len(items)} items")
    return [{
        "post_id":    post_id("youtube", i.get("id", "")),
        "platform":   "youtube",
        "username":   i.get("channelName", ""),
        "text":       i.get("title", ""),
        "likes":      i.get("likes", 0),
        "replies":    i.get("commentsCount", 0),
        "reposts":    0,
        "url":        i.get("url", ""),
        "posted_at":  i.get("date", ""),
        "keyword":    ",".join(keywords[:3]),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    } for i in items]

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
        log.info(f"Using existing tab: {today}")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=today, rows=5000, cols=len(SHEET_HEADERS))
        ws.append_row(SHEET_HEADERS, value_input_option="RAW")
        log.info(f"Created new tab: {today}")
    return ws

def get_existing_ids(ws):
    try:
        return set(ws.col_values(1)[1:])
    except Exception:
        return set()

def push_rows(ws, rows, existing_ids):
    new = [r for r in rows if r["post_id"] not in existing_ids]
    if not new:
        log.info("No new rows to push")
        return 0
    matrix = [[str(r.get(h, "")) for h in SHEET_HEADERS] for r in new]
    ws.append_rows(matrix, value_input_option="RAW")
    log.info(f"Pushed {len(new)} rows")
    return len(new)

def main():
    log.info("=== Social Listening START ===")
    check_env()
    keywords = load_keywords()
    ws = get_or_create_sheet()
    existing_ids = get_existing_ids(ws)
    log.info(f"Existing rows in sheet: {len(existing_ids)}")

    scrapers = [scrape_threads, scrape_facebook, scrape_tiktok, scrape_youtube]
    all_posts = []

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fn, keywords): fn.__name__ for fn in scrapers}
        for future in as_completed(futures):
            name = futures[future]
            try:
                posts = future.result()
                all_posts.extend(posts)
                log.info(f"{name} -> {len(posts)} posts")
            except Exception as e:
                log.error(f"{name} failed: {e}")

    seen, deduped = set(), []
    for p in all_posts:
        if p["post_id"] not in seen:
            seen.add(p["post_id"])
            deduped.append(p)

    log.info(f"Total after dedup: {len(deduped)}")
    pushed = push_rows(ws, deduped, existing_ids)
    log.info(f"=== DONE: {pushed} new posts written ===")

if __name__ == "__main__":
    main()
