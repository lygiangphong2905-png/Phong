"""
social_listening/scraper/run.py
--------------------------------
1. Load keywords từ config/keywords.json
2. Chạy Apify actors song song (Threads, Facebook, TikTok, YouTube)
3. Normalize + deduplicate kết quả
4. Push lên Google Sheets
"""

import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

APIFY_TOKEN       = os.environ["APIFY_TOKEN"]
SHEET_ID          = os.environ["GOOGLE_SHEET_ID"]
GCP_CREDS_JSON    = os.environ["GCP_SERVICE_ACCOUNT_JSON"]
KEYWORDS_PATH     = os.path.join(os.path.dirname(__file__), "../config/keywords.json")

ACTORS = {
    "threads":  "automation-lab/threads-scraper",
    "facebook": "apify/facebook-posts-scraper",
    "tiktok":   "clockworks/tiktok-scraper",
    "youtube":  "streamers/youtube-scraper",
}

MAX_RESULTS_PER_KEYWORD = 50
APIFY_POLL_INTERVAL     = 5
APIFY_TIMEOUT           = 300

SHEET_HEADERS = [
    "post_id", "platform", "username", "text",
    "likes", "replies", "reposts", "url",
    "posted_at", "keyword", "scraped_at",
]
