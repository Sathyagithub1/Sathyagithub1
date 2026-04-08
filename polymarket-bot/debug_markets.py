#!/usr/bin/env python3
"""Run this on the VPS to diagnose why scanner returns 0 markets."""

import urllib.request
import json
from datetime import datetime

URL = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100"

print("Fetching markets...")
try:
    with urllib.request.urlopen(URL, timeout=10) as r:
        raw = json.loads(r.read())
except Exception as e:
    print(f"API ERROR: {e}")
    raise

items = raw if isinstance(raw, list) else raw.get("data", raw.get("markets", []))
print(f"\nTotal markets returned: {len(items)}")

if not items:
    print("API returned empty list. Check URL or Polymarket status.")
    exit()

# Show first 3 market structures
print("\n--- Sample market keys ---")
print(list(items[0].keys()))

# Show all questions
print("\n--- All questions ---")
for m in items:
    print(" ", m.get("question", "")[:100])

# Filter: BTC or ETH
crypto = [m for m in items if "BTC" in (m.get("question","")).upper() or "ETH" in (m.get("question","")).upper()]
print(f"\nBTC/ETH markets: {len(crypto)}")
for m in crypto:
    q = m.get("question", "")
    end = m.get("end_date_iso") or m.get("endDateIso", "")
    vol = m.get("volume") or m.get("volumeNum", 0)
    if end:
        try:
            expiry = datetime.fromisoformat(end.replace("Z", "+00:00"))
            mins = (expiry - datetime.now(expiry.tzinfo)).total_seconds() / 60
            print(f"  [{mins:.0f}m] vol=${vol}  {q[:80]}")
        except:
            print(f"  [?m] {q[:80]}")
    else:
        print(f"  [no expiry] {q[:80]}")
