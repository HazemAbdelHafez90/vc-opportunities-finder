"""Enrich an analysis export, saving progress after each historical notice."""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api._lib import DEFAULT_HEADERS, SSL_CONTEXT
from lib.tender_documents import SOURCE_HOSTS, allowed_url, read_source_documents


def run(source, output, limit):
    if source.resolve() == output.resolve():
        raise ValueError("Choose a separate output path")
    original = json.loads(source.read_text())
    data = json.loads(output.read_text()) if output.exists() else original
    if data["exported_at"] != original["exported_at"]:
        raise ValueError("Output belongs to another export")
    rows = sorted(data["opportunities"], key=lambda r: (
        r.get("action_status") not in {"won", "applied"} and r.get("action_reason") != "not_relevant",
        r.get("status") != "open", r["id"]))
    attempted = 0
    for row in rows:
        raw = row.get("raw_payload") or {}
        item = {**raw, "source": row["source"], "link": row.get("link") or ""}
        if row.get("backfillCheckedAt") or not allowed_url(item["link"], SOURCE_HOSTS.get(item["source"], set())):
            continue
        if attempted >= limit:
            break
        attempted += 1
        try:
            docs = read_source_documents(item, headers=DEFAULT_HEADERS, ssl_context=SSL_CONTEXT)
        except (OSError, ValueError) as exc:
            print(f'{row["id"]}: {type(exc).__name__}', file=sys.stderr)
            continue
        # Retain previous readable text if a file disappeared or became inaccessible.
        found = {d["url"]: d for d in docs}
        for old in raw.get("documents") or []:
            if old["url"] not in found or (old.get("status") == "read" and found[old["url"]].get("status") != "read"):
                found[old["url"]] = old
        item["documents"] = list(found.values())
        row["raw_payload"] = item
        row["backfillCheckedAt"] = datetime.now(timezone.utc).isoformat()
        temp = output.with_suffix(output.suffix + ".tmp")
        temp.write_text(json.dumps(data, ensure_ascii=False))
        os.replace(temp, output)
    print(f"Attempted {attempted} notices")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    run(args.source, args.output, args.limit)
