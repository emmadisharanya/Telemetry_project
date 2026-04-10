"""
NASA HTTP Log CSV Replay Script
Reads the NASA CSV dataset and sends events to your Firetiger ingestion API.

Columns: ,host,time,method,url,response,bytes
"""

import csv
import requests
import time
import os
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
API_URL       = "http://localhost:8000/api/ingest"
CSV_FILE      = "NASA_access_log_Jul95.csv"   # change this if your filename is different
BATCH_SIZE = 50000
SLEEP_BETWEEN_BATCHES = 0.01
MAX_EVENTS = None



# Map hosts to fake customer IDs
HOST_TO_CUSTOMER = {}
CUSTOMER_POOL = [
    "acme-corp", "tech-startup", "big-bank", "retail-co",
    "healthtech", "fintech-inc", "media-group", "logistics-co",
    "edtech-ltd", "saas-platform"
]

def get_customer(host: str) -> str:
    if host not in HOST_TO_CUSTOMER:
        index = hash(host) % len(CUSTOMER_POOL)
        HOST_TO_CUSTOMER[host] = CUSTOMER_POOL[index]
    return HOST_TO_CUSTOMER[host]

def status_to_level(status: str) -> str:
    try:
        code = int(status)
        if code >= 500:
            return "ERROR"
        elif code >= 400:
            return "WARN"
        else:
            return "INFO"
    except:
        return "INFO"

def url_to_service(url: str) -> str:
    url = url.lower()
    if "shuttle" in url:
        return "shuttle-service"
    elif "history" in url:
        return "history-api"
    elif "images" in url or "img" in url:
        return "image-service"
    elif "cgi" in url:
        return "cgi-service"
    elif "pub" in url:
        return "public-api"
    elif "ksc" in url:
        return "ksc-service"
    else:
        return "web-api"

def parse_row(row: dict):
    """Convert a CSV row to a telemetry event."""
    host     = row.get("host", "unknown").strip()
    method   = row.get("method", "GET").strip()
    url      = row.get("url", "/").strip()
    response = row.get("response", "200").strip()
    bytes_   = row.get("bytes", "0").strip()
    time_val = row.get("time", "").strip()

    # Convert Unix timestamp to ISO string
    try:
        ts = datetime.fromtimestamp(int(time_val), tz=timezone.utc).isoformat()
    except Exception:
        ts = datetime.now(timezone.utc).isoformat()

    return {
        "type": "log",
        "timestamp": ts,
        "customer_id": get_customer(host),
        "service": url_to_service(url),
        "level": status_to_level(response),
        "message": f"{method} {url} -> {response}",
        "attributes": {
            "host": host,
            "method": method,
            "url": url,
            "http_status": int(response) if response.isdigit() else 0,
            "bytes": int(bytes_) if bytes_.isdigit() else 0
        }
    }

def send_batch(batch: list) -> bool:
    try:
        response = requests.post(API_URL, json={"events": batch}, timeout=60)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        print("\n❌ Cannot connect to API. Is your server running?")
        print("   Run in another terminal: python main.py")
        exit(1)

def replay():
    if not os.path.exists(CSV_FILE):
        print(f"❌ File not found: {CSV_FILE}")
        print(f"   Make sure your CSV is in this folder: {os.getcwd()}")
        print(f"   And update CSV_FILE at the top of this script if the name is different")
        exit(1)

    print("=" * 50)
    print("Firetiger Demo - NASA Log Replay")
    print("=" * 50)
    print(f"   File:       {CSV_FILE}")
    print(f"   Batch size: {BATCH_SIZE}")
    print(f"   Max events: {MAX_EVENTS or 'ALL'}")
    print()
    print("Starting replay...")
    print()

    batch        = []
    total_sent   = 0
    total_errors = 0
    total_warn   = 0

    with open(CSV_FILE, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in reader:
            event = parse_row(row)

            if event["level"] == "ERROR":
                total_errors += 1
            elif event["level"] == "WARN":
                total_warn += 1

            batch.append(event)

            if len(batch) >= BATCH_SIZE:
                if send_batch(batch):
                    total_sent += len(batch)
                    print(
                        f"\r   Sent: {total_sent} | "
                        f"Errors: {total_errors} | "
                        f"Warns: {total_warn} | "
                        f"Customers: {len(HOST_TO_CUSTOMER)}",
                        end="", flush=True
                    )
                batch = []
                time.sleep(SLEEP_BETWEEN_BATCHES)

            if MAX_EVENTS and total_sent >= MAX_EVENTS:
                break

    # Send any remaining events
    if batch:
        if send_batch(batch):
            total_sent += len(batch)

    print()
    print()
    print("=" * 50)
    print("Done!")
    print(f"   Total sent:        {total_sent} events")
    print(f"   ERROR events:      {total_errors}")
    print(f"   WARN events:       {total_warn}")
    print(f"   Unique customers:  {len(HOST_TO_CUSTOMER)}")
    print()
    print("Now query your data:")
    for customer in list(set(HOST_TO_CUSTOMER.values()))[:4]:
        print(f"   python cli.py errors --customer {customer} --hours 9999")
    print("=" * 50)


if __name__ == "__main__":
    replay()