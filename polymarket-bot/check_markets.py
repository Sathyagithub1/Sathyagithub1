#!/usr/bin/env python3
"""Quick check of what BTC/ETH markets exist on Polymarket right now."""
import urllib.request, json
from datetime import datetime

url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=500"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=15) as r:
    items = json.loads(r.read())

items = items if isinstance(items, list) else items.get("data", [])
print(f"Total: {len(items)}")

btc_eth = [m for m in items if any(
    s in (m.get("question") or "").upper() for s in ["BTC", "ETH", "BITCOIN", "ETHEREUM"]
)]
print(f"BTC/ETH markets: {len(btc_eth)}\n")

for m in btc_eth:
    end = m.get("endDate") or m.get("end_date_iso") or ""
    liq = m.get("liquidity") or m.get("volume") or 0
    mins = "?"
    if end:
        try:
            e = datetime.fromisoformat(end.replace("Z", "+00:00"))
            mins = int((e - datetime.now(e.tzinfo)).total_seconds() / 60)
        except: pass
    print(f"  [{mins}m] liq=${liq} | {(m.get('question') or '')[:90]}")
