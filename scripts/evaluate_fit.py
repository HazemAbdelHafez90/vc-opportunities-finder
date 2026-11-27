"""Evaluate saved scores and the experimental assessment against an export.

Usage: python scripts/evaluate_fit.py /path/to/opportunities-analysis.json
The test split is stable and grouped by normalized title; manually group related
notices before using this as a final held-out evaluation.
"""
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.fit_assessment import assess
from api._lib import get_fit_analysis


def split(row):
    title = re.sub(r"[^a-z0-9]+", " ", (row.get("title") or "").lower()).strip()
    return "test" if int(hashlib.sha256(title.encode()).hexdigest()[:8], 16) % 5 == 0 else "development"


def main(path):
    data = json.load(sys.stdin) if path == "-" else json.loads(Path(path).read_text(encoding="utf-8"))
    counts = Counter()
    for row in data["opportunities"]:
        label = "negative" if row.get("action_reason") == "not_relevant" else "positive" if row.get("action_status") in ("applied", "won") else None
        if not label:
            continue
        group = split(row)
        counts[(group, label, "total")] += 1
        if (row.get("fit_score") or 0) >= 40:
            counts[(group, label, "saved_medium_or_high")] += 1
        inputs = {**(row.get("raw_payload") or {}), "title": row.get("title"), "type": row.get("type"), "organization": row.get("organization")}
        if get_fit_analysis(inputs)["score"] >= 40:
            counts[(group, label, "current_medium_or_high")] += 1
        result = assess(row)
        counts[(group, label, "experimental_" + result["visualProduction"])] += 1
        counts[(group, label, "confidence_" + result["confidence"])] += 1
    for (group, label, metric), value in sorted(counts.items()):
        print(f"{group}\t{label}\t{metric}\t{value}")
    print("Saved scores can come from different rule versions; this is not a uniform rescore.")
    print("Title grouping does not guarantee related notices are separated; review before final testing.")


if __name__ == "__main__":
    main(sys.argv[1])
