#!/usr/bin/env python3
"""Download a Tender Radar export without placing its bearer token on stdout."""
import csv
import json
import os
from pathlib import Path
from urllib import parse, request

url = os.environ["EXPORT_API_URL"].rstrip("/")
token = os.environ["EXPORT_API_TOKEN"]
include_raw = os.getenv("EXPORT_INCLUDE_RAW") == "1"
items, offset = [], 0
while True:
    query = parse.urlencode({"limit": 500, "offset": offset, "include_raw": "1" if include_raw else "0"})
    req = request.Request(f"{url}?{query}", headers={"Authorization": f"Bearer {token}"})
    with request.urlopen(req, timeout=20) as response:
        page = json.load(response)
    batch = page["items"]
    items.extend(batch)
    if len(batch) < 500:
        break
    offset += len(batch)

Path("tenders.jsonl").write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")
fields = sorted({key for item in items for key in item if key != "raw_payload"})
with Path("tenders.csv").open("w", newline="", encoding="utf-8") as output:
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for item in items:
        writer.writerow({key: "; ".join(value) if isinstance(value, list) else value for key, value in item.items() if key != "raw_payload"})
print(f"Wrote {len(items)} tenders to tenders.jsonl and tenders.csv")
