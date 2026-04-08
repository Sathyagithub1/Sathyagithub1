#!/usr/bin/env python3
"""
Polymarket-Kalshi Cross-Platform Arbitrage Bot
Detects price discrepancies for the same event on both platforms
and executes both legs simultaneously for risk-free profit.

Paper mode ENABLED BY DEFAULT.
Live: python3 arb_bot.py --live --confirm --i-understand-risks
"""

import asyncio
import aiohttp
import json
import time
import logging
import argparse
import sqlite3
import sys
import math
import hmac
import hashlib
import re
import signal
import getpass
from datetime import datetime, date
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

# ─── Constants ────────────────────────────────────────────────────────────────

POLY_CLOB              = "https://clob.polymarket.com"
POLY_GAMMA             = "https://gamma-api.polymarket.com"
KALSHI_API             = "https://api.elections.kalshi.com/trade-api/v2"

MIN_EXECUTION_EDGE     = 0.05   # 5% net after ~2% fees both sides
MAX_POSITION_PCT       = 0.08   # 8% of portfolio per arb pair
DAILY_HALT_PCT         = 0.20
ATH_DRAWDOWN_HALT_PCT  = 0.40
CONSEC_LOSS_LIMIT      = 5
CONSEC_LOSS_PAUSE_MIN  = 30
SCAN_INTERVAL          = 30    # seconds
MIN_LIQUIDITY_POLY     = 5_000
MIN_LIQUIDITY_KALSHI   = 1_000
STRIKE_MATCH_PCT       = 0.02  # strikes within 2%
EXPIRY_MATCH_DAYS      = 3     # expiries within 3 days
ESTIMATED_FEES         = 0.02  # 2% round-trip fee estimate

# ─── Credentials ──────────────────────────────────────────────────────────────

@dataclass
class Credentials:
    poly_api_key:    str
    poly_api_secret: str
    poly_passphrase: str
    poly_priv_key:   str
    alchemy_rpc:     str
    kalshi_api_key:  str
    kalshi_api_secret: str
    tg_token:        str
    tg_chat_id:      str


def collect_credentials(live: bool) -> Credentials:
    print("\n─── Credential input (hidden, never stored) ─────────────────")
    if not live:
        print("Paper mode: Telegram only (optional — press Enter to skip).\n")

    def ask(prompt: str, required: bool = False) -> str:
        while True:
            val = getpass.getpass(f"  {prompt}: ").strip()
            if val or not required:
                return val
            print("  (required)")

    tg_token   = ask("Telegram bot token  (blank = skip)")
    tg_chat_id = ask("Telegram chat ID    (blank = skip)")

    if not live:
        print("─────────────────────────────────────────────────────────────\n")
        return Credentials("", "", "", "", "", "", "", tg_token, tg_chat_id)

    print()
    poly_api_key    = ask("Polymarket API key",        required=True)
    poly_api_secret = ask("Polymarket API secret",     required=True)
    poly_passphrase = ask("Polymarket API passphrase", required=True)
    poly_priv_key   = ask("Wallet private key (0x…)",  required=True)
    alchemy_rpc     = ask("Alchemy RPC URL",            required=True)
    kalshi_api_key  = ask("Kalshi API key",             required=True)
    kalshi_api_secret = ask("Kalshi API secret",        required=True)

    print("─────────────────────────────────────────────────────────────")
    print("✓ Credentials loaded into memory only.\n")

    return Credentials(
        poly_api_key=poly_api_key, poly_api_secret=poly_api_secret,
        poly_passphrase=poly_passphrase, poly_priv_key=poly_priv_key,
        alchemy_rpc=alchemy_rpc, kalshi_api_key=kalshi_api_key,
        kalshi_api_secret=kalshi_api_secret,
        tg_token=tg_token, tg_chat_id=tg_chat_id,
    )


class _CredentialScrubber(logging.Filter):
    def __init__(self):
        super().__init__()
        self._secrets: List[str] = []

    def register(self, *values: str):
        self._secrets.extend(v for v in values if v)

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for s in self._secrets:
            if s and s in msg:
                record.msg  = record.msg.replace(s, "***")
                record.args = ()
        return True


_scrubber = _CredentialScrubber()


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class PolyMarket:
    market_id:   str
    token_id:    str
    question:    str
    symbol:      str
    direction:   str
    strike:      float
    expiry:      datetime
    yes_ask:     float
    yes_bid:     float
    liquidity:   float


@dataclass
class KalshiMarket:
    ticker:    str
    title:     str
    symbol:    str
    direction: str
    strike:    float
    expiry:    datetime
    yes_ask:   float
    yes_bid:   float
    no_ask:    float
    liquidity: float


@dataclass
class ArbPair:
    poly:         PolyMarket
    kalshi:       KalshiMarket
    poly_yes:     float   # Polymarket YES ask (we buy this)
    kalshi_no:    float   # Kalshi NO ask = 1 - kalshi YES bid (we buy this)
    gross_edge:   float   # kalshi_yes_bid - poly_yes_ask
    net_edge:     float   # gross_edge - ESTIMATED_FEES


@dataclass
class ArbTrade:
    trade_id:       str
    poly_market_id: str
    kalshi_ticker:  str
    question:       str
    poly_price:     float
    kalshi_no_price: float
    size:           float
    gross_edge:     float
    net_edge:       float
    entry_time:     float
    is_paper:       bool
    status:         str   = "OPEN"
    pnl:            float = 0.0
    exit_time:      float = 0.0


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addFilter(_scrubber)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    fh = RotatingFileHandler("arb_bot.log", maxBytes=50*1024*1024, backupCount=5)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)


# ─── Telegram ─────────────────────────────────────────────────────────────────

class Telegram:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self._sess: Optional[aiohttp.ClientSession] = None
        self._log = logging.getLogger("telegram")

    async def _s(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def send(self, msg: str) -> None:
        self._log.info(f"[TG] {msg[:100]}")
        if not self.enabled:
            return
        try:
            s = await self._s()
            await s.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=6),
            )
        except Exception as e:
            self._log.warning(f"Telegram: {e}")

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


# ─── Database ─────────────────────────────────────────────────────────────────

class DB:
    def __init__(self):
        self.conn = sqlite3.connect("arb_trades.db", check_same_thread=False)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                poly_market_id TEXT, kalshi_ticker TEXT, question TEXT,
                poly_price REAL, kalshi_no_price REAL, size REAL,
                gross_edge REAL, net_edge REAL, entry_time REAL,
                exit_time REAL, status TEXT, pnl REAL, is_paper INTEGER
            );
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
        """)
        self.conn.commit()

    def save(self, t: ArbTrade):
        self.conn.execute(
            "INSERT OR REPLACE INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (t.trade_id, t.poly_market_id, t.kalshi_ticker, t.question,
             t.poly_price, t.kalshi_no_price, t.size,
             t.gross_edge, t.net_edge, t.entry_time,
             t.exit_time, t.status, t.pnl, int(t.is_paper)),
        )
        self.conn.commit()

    def get(self, key: str, default: str = "") -> str:
        r = self.conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return r[0] if r else default

    def set(self, key: str, val: str):
        self.conn.execute("INSERT OR REPLACE INTO kv VALUES (?,?)", (key, val))
        self.conn.commit()

    def today_stats(self) -> Tuple[int, int, float]:
        start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        r = self.conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), SUM(pnl) "
            "FROM trades WHERE entry_time>=? AND status!='OPEN'", (start,)
        ).fetchone()
        return (r[0] or 0), (r[1] or 0), (r[2] or 0.0)

    def ath(self, default: float) -> float:
        return float(self.get("arb_ath", str(default)))

    def update_ath(self, v: float):
        self.set("arb_ath", str(v))


# ─── Risk Manager ─────────────────────────────────────────────────────────────

class RiskManager:
    def __init__(self, tg: Telegram, db: DB, start_balance: float):
        self.tg            = tg
        self.db            = db
        self.portfolio     = start_balance
        self.day_start     = start_balance
        self.ath           = db.ath(start_balance)
        self.consec_losses = 0
        self.daily_halted  = False
        self.ath_halted    = False
        self.pause_until: Optional[float] = None
        self._hb_due       = time.time() + 3600
        self._log          = logging.getLogger("risk")

    def update_balance(self, v: float):
        self.portfolio = v
        if v > self.ath:
            self.ath = v
            self.db.update_ath(v)

    async def record_result(self, pnl: float):
        if pnl > 0:
            self.consec_losses = 0
        else:
            self.consec_losses += 1
            if self.consec_losses >= CONSEC_LOSS_LIMIT:
                self.pause_until = time.time() + CONSEC_LOSS_PAUSE_MIN * 60
                await self.tg.send(
                    f"⚠️ <b>PAUSE — {CONSEC_LOSS_LIMIT} consecutive losses</b>\n"
                    f"Pausing {CONSEC_LOSS_PAUSE_MIN} min. Manual restart required."
                )

    async def should_halt(self) -> bool:
        if self.day_start > 0:
            pct = (self.portfolio - self.day_start) / self.day_start
            if pct <= -DAILY_HALT_PCT and not self.daily_halted:
                self.daily_halted = True
                await self.tg.send(
                    f"🛑 <b>DAILY LOSS HALT</b>\n"
                    f"P&L: {pct:.1%}  (limit −{DAILY_HALT_PCT:.0%})\n"
                    f"Balance: ${self.portfolio:.2f}\n<b>Manual restart required.</b>"
                )
        if self.daily_halted:
            return True

        if self.ath > 0:
            dd = (self.ath - self.portfolio) / self.ath
            if dd >= ATH_DRAWDOWN_HALT_PCT and not self.ath_halted:
                self.ath_halted = True
                await self.tg.send(
                    f"🛑 <b>ATH DRAWDOWN HALT</b>\n"
                    f"Drawdown: {dd:.1%}  ATH: ${self.ath:.2f}\n"
                    f"<b>Manual restart required.</b>"
                )
        if self.ath_halted:
            return True

        if self.pause_until:
            if time.time() < self.pause_until:
                return True
            self.pause_until = None
            self.consec_losses = 0
            await self.tg.send("✅ Pause lifted. Resuming.")

        return False

    async def daily_reset(self):
        self.day_start    = self.portfolio
        self.daily_halted = False
        logging.getLogger("risk").info(f"Daily reset. Balance: ${self.day_start:.2f}")

    async def heartbeat(self):
        if time.time() < self._hb_due:
            return
        self._hb_due = time.time() + 3600
        total, wins, pnl = self.db.today_stats()
        wr = f"{wins/total*100:.1f}%" if total else "n/a"
        await self.tg.send(
            f"💓 <b>Heartbeat [ARB BOT]</b>\n"
            f"Balance: ${self.portfolio:.2f}  ATH: ${self.ath:.2f}\n"
            f"Today: {total} arbs  WR: {wr}  P&L: ${pnl:.2f}\n"
            f"Consecutive losses: {self.consec_losses}"
        )


# ─── Kelly sizing ─────────────────────────────────────────────────────────────

def kelly_size(portfolio: float, net_edge: float) -> float:
    """
    For a risk-free arb, Kelly = edge / (1 - edge).
    Half-Kelly capped at MAX_POSITION_PCT.
    """
    if net_edge <= 0:
        return 0.0
    f = (net_edge / (1.0 - net_edge)) / 2.0
    return min(f * portfolio, MAX_POSITION_PCT * portfolio)


# ─── Polymarket Scanner ───────────────────────────────────────────────────────

class PolyScanner:
    SYMBOLS = ["BTC", "ETH"]

    def __init__(self):
        self._sess: Optional[aiohttp.ClientSession] = None
        self._cache: List[PolyMarket] = []
        self._cache_time = 0.0
        self._log = logging.getLogger("poly_scanner")

    async def _s(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._sess

    def _parse(self, m: dict) -> Optional[PolyMarket]:
        try:
            q  = m.get("question", "") or ""
            qu = q.upper()
            sym = next((s for s in self.SYMBOLS if re.search(rf'\b{s}\b', qu)), None)
            if not sym:
                return None

            if any(k in qu for k in ("ABOVE", "HIGHER", "OVER", "> ", "≥")):
                direction = "ABOVE"
            elif any(k in qu for k in ("BELOW", "LOWER", "UNDER", "< ", "≤")):
                direction = "BELOW"
            else:
                return None

            nums = [float(n.replace(",", "")) for n in re.findall(r"[\d,]+(?:\.\d+)?", q)]
            lo, hi = (1_000, 1_000_000) if sym == "BTC" else (100, 100_000)
            strike = next((n for n in sorted(nums, reverse=True) if lo < n < hi), None)
            if strike is None:
                return None

            end_str = m.get("endDate") or m.get("end_date_iso") or m.get("endDateIso", "")
            if not end_str:
                return None
            expiry = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            now = datetime.now(expiry.tzinfo)
            if expiry <= now:
                return None

            tokens = m.get("tokens") or m.get("clob_token_ids") or []
            if not tokens:
                return None
            tid = tokens[0] if isinstance(tokens[0], str) else tokens[0].get("token_id", "")

            liq = float(m.get("liquidity") or m.get("volume") or 0)
            if liq < MIN_LIQUIDITY_POLY:
                return None

            yes_ask = float(m.get("bestAsk") or m.get("best_ask") or 0.5)
            yes_bid = float(m.get("bestBid") or m.get("best_bid") or 0.5)

            return PolyMarket(
                market_id=m.get("conditionId") or m.get("condition_id") or m.get("id", ""),
                token_id=tid, question=q, symbol=sym, direction=direction,
                strike=strike, expiry=expiry, yes_ask=yes_ask,
                yes_bid=yes_bid, liquidity=liq,
            )
        except Exception as e:
            self._log.debug(f"parse error: {e}")
            return None

    async def scan(self) -> List[PolyMarket]:
        if time.time() - self._cache_time < SCAN_INTERVAL:
            return self._cache
        try:
            s = await self._s()
            async with s.get(
                f"{POLY_GAMMA}/markets",
                params={"active": "true", "closed": "false", "limit": 500},
            ) as r:
                if r.status != 200:
                    return self._cache
                raw = await r.json()
            items = raw if isinstance(raw, list) else raw.get("data", [])
            self._cache = [p for m in items if (p := self._parse(m))]
            self._cache_time = time.time()
            self._log.info(f"Poly: {len(self._cache)} BTC/ETH markets")
        except Exception as e:
            self._log.error(f"Poly scan failed: {e}")
        return self._cache

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


# ─── Kalshi Scanner ───────────────────────────────────────────────────────────

class KalshiScanner:
    SYMBOLS = ["BTC", "ETH", "BITCOIN", "ETHEREUM"]

    def __init__(self, api_key: str, api_secret: str):
        self._api_key    = api_key
        self._api_secret = api_secret
        self._sess: Optional[aiohttp.ClientSession] = None
        self._cache: List[KalshiMarket] = []
        self._cache_time = 0.0
        self._log = logging.getLogger("kalshi_scanner")

    def _headers(self) -> dict:
        ts  = str(int(time.time() * 1000))
        sig = hmac.new(
            self._api_secret.encode() if self._api_secret else b"",
            f"GET/trade-api/v2/markets{ts}".encode(),
            hashlib.sha256,
        ).hexdigest() if self._api_secret else ""
        h = {"Content-Type": "application/json"}
        if self._api_key:
            h["Authorization"] = f"Token {self._api_key}"
        return h

    async def _s(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._sess

    def _parse(self, m: dict) -> Optional[KalshiMarket]:
        try:
            title = (m.get("title") or m.get("question") or "").upper()
            sym = None
            if re.search(r'\bBTC\b', title) or "BITCOIN" in title:
                sym = "BTC"
            elif re.search(r'\bETH\b', title) or "ETHEREUM" in title:
                sym = "ETH"
            if not sym:
                return None

            if any(k in title for k in ("ABOVE", "HIGHER", "OVER", "> ", "≥", "AT OR ABOVE")):
                direction = "ABOVE"
            elif any(k in title for k in ("BELOW", "LOWER", "UNDER", "< ", "≤", "AT OR BELOW")):
                direction = "BELOW"
            else:
                return None

            nums = [float(n.replace(",", "")) for n in re.findall(r"[\d,]+(?:\.\d+)?", m.get("title", ""))]
            lo, hi = (1_000, 1_000_000) if sym == "BTC" else (100, 100_000)
            strike = next((n for n in sorted(nums, reverse=True) if lo < n < hi), None)
            if strike is None:
                return None

            close_str = m.get("close_time") or m.get("expiration_time") or ""
            if not close_str:
                return None
            expiry = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            if expiry <= datetime.now(expiry.tzinfo):
                return None

            vol = float(m.get("volume") or m.get("liquidity") or 0)
            if vol < MIN_LIQUIDITY_KALSHI:
                return None

            yes_ask = float(m.get("yes_ask") or 0.5) / 100.0
            yes_bid = float(m.get("yes_bid") or 0.5) / 100.0
            no_ask  = float(m.get("no_ask")  or 0.5) / 100.0

            return KalshiMarket(
                ticker=m.get("ticker", ""), title=m.get("title", ""),
                symbol=sym, direction=direction, strike=strike,
                expiry=expiry, yes_ask=yes_ask, yes_bid=yes_bid,
                no_ask=no_ask, liquidity=vol,
            )
        except Exception as e:
            self._log.debug(f"parse error: {e}")
            return None

    async def scan(self) -> List[KalshiMarket]:
        if time.time() - self._cache_time < SCAN_INTERVAL:
            return self._cache
        try:
            s = await self._s()
            async with s.get(
                f"{KALSHI_API}/markets",
                headers=self._headers(),
                params={"status": "open", "limit": 200},
            ) as r:
                if r.status not in (200, 401):
                    self._log.warning(f"Kalshi API {r.status}")
                    return self._cache
                if r.status == 401:
                    self._log.warning("Kalshi: unauthorised — check API key. Continuing without Kalshi data.")
                    return self._cache
                raw = await r.json()
            items = raw.get("markets", raw) if isinstance(raw, dict) else raw
            self._cache = [k for m in items if (k := self._parse(m))]
            self._cache_time = time.time()
            self._log.info(f"Kalshi: {len(self._cache)} BTC/ETH markets")
        except Exception as e:
            self._log.error(f"Kalshi scan failed: {e}")
        return self._cache

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


# ─── Market Matcher & Arb Detector ───────────────────────────────────────────

def find_arb_pairs(poly_markets: List[PolyMarket],
                   kalshi_markets: List[KalshiMarket]) -> List[ArbPair]:
    """
    Match markets across platforms and return pairs with positive net edge.
    Matching criteria: same symbol, same direction, strike within 2%, expiry within 3 days.
    """
    pairs: List[ArbPair] = []

    for p in poly_markets:
        for k in kalshi_markets:
            if p.symbol != k.symbol or p.direction != k.direction:
                continue

            # Strike match within 2%
            if abs(p.strike - k.strike) / max(p.strike, 1) > STRIKE_MATCH_PCT:
                continue

            # Expiry match within 3 days
            expiry_diff = abs((p.expiry - k.expiry).total_seconds()) / 86400
            if expiry_diff > EXPIRY_MATCH_DAYS:
                continue

            # Arb condition: buy YES on Poly (cheaper) + buy NO on Kalshi (cheaper)
            # Profitable when: poly_yes_ask + kalshi_no_ask < 1.0
            poly_yes  = p.yes_ask
            kalshi_no = k.no_ask   # = 1 - kalshi_yes_bid approximately

            total_cost = poly_yes + kalshi_no
            if total_cost >= 1.0:
                continue

            gross_edge = 1.0 - total_cost
            net_edge   = gross_edge - ESTIMATED_FEES

            if net_edge < MIN_EXECUTION_EDGE:
                continue

            pairs.append(ArbPair(
                poly=p, kalshi=k,
                poly_yes=poly_yes, kalshi_no=kalshi_no,
                gross_edge=gross_edge, net_edge=net_edge,
            ))

    # Sort by net edge descending
    pairs.sort(key=lambda x: x.net_edge, reverse=True)
    return pairs


# ─── Executor ─────────────────────────────────────────────────────────────────

class Executor:
    def __init__(self, paper: bool, db: DB, tg: Telegram, creds: Credentials):
        self.paper        = paper
        self.db           = db
        self.tg           = tg
        self._creds       = creds
        self._sess: Optional[aiohttp.ClientSession] = None
        self.open_trades: Dict[str, ArbTrade] = {}
        self._log = logging.getLogger("executor")

    async def _s(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
        return self._sess

    def _poly_headers(self) -> dict:
        ts    = str(int(time.time()))
        nonce = str(int(time.time() * 1000) % 1_000_000)
        sig   = hmac.new(
            self._creds.poly_api_secret.encode(),
            f"{ts}{nonce}".encode(), hashlib.sha256
        ).hexdigest()
        return {
            "POLY_ADDRESS":    self._creds.poly_api_key,
            "POLY_SIGNATURE":  sig,
            "POLY_TIMESTAMP":  ts,
            "POLY_NONCE":      nonce,
            "POLY_PASSPHRASE": self._creds.poly_passphrase,
            "Content-Type":    "application/json",
        }

    def _kalshi_headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self._creds.kalshi_api_key:
            h["Authorization"] = f"Token {self._creds.kalshi_api_key}"
        return h

    async def _place_poly_order(self, pair: ArbPair, size: float) -> Tuple[bool, str]:
        try:
            s = await self._s()
            async with s.post(
                f"{POLY_CLOB}/order",
                headers=self._poly_headers(),
                json={
                    "token_id": pair.poly.token_id,
                    "price":    str(round(pair.poly_yes, 4)),
                    "size":     str(round(size, 2)),
                    "side":     "BUY",
                    "type":     "FOK",
                },
            ) as r:
                if r.status == 200:
                    res = await r.json()
                    return True, res.get("orderID", "ok")
                body = await r.text()
                return False, f"HTTP {r.status}: {body[:100]}"
        except Exception as e:
            return False, str(e)[:100]

    async def _place_kalshi_order(self, pair: ArbPair, size: float) -> Tuple[bool, str]:
        try:
            s = await self._s()
            # Kalshi size is in cents (integer contracts), each worth $0.01
            contracts = max(1, int(size / pair.kalshi_no * 100))
            async with s.post(
                f"{KALSHI_API}/portfolio/orders",
                headers=self._kalshi_headers(),
                json={
                    "ticker":   pair.kalshi.ticker,
                    "side":     "no",
                    "type":     "market",
                    "count":    contracts,
                    "action":   "buy",
                },
            ) as r:
                if r.status in (200, 201):
                    res = await r.json()
                    return True, res.get("order", {}).get("id", "ok")
                body = await r.text()
                return False, f"HTTP {r.status}: {body[:100]}"
        except Exception as e:
            return False, str(e)[:100]

    async def execute(self, pair: ArbPair, size: float) -> Optional[ArbTrade]:
        trade_id = f"{'P' if self.paper else 'L'}_{pair.poly.market_id[:8]}_{int(time.time()*1000)}"
        trade = ArbTrade(
            trade_id=trade_id,
            poly_market_id=pair.poly.market_id,
            kalshi_ticker=pair.kalshi.ticker,
            question=pair.poly.question,
            poly_price=pair.poly_yes,
            kalshi_no_price=pair.kalshi_no,
            size=size,
            gross_edge=pair.gross_edge,
            net_edge=pair.net_edge,
            entry_time=time.time(),
            is_paper=self.paper,
        )

        if self.paper:
            trade.status = "OPEN"
            self.open_trades[trade_id] = trade
            self.db.save(trade)
            await self.tg.send(
                f"📄 <b>PAPER ARB</b>\n"
                f"{pair.poly.question[:80]}\n"
                f"Poly YES @ {pair.poly_yes:.3f}  |  Kalshi NO @ {pair.kalshi_no:.3f}\n"
                f"Size: ${size:.2f}/leg  Net edge: {pair.net_edge:.1%}\n"
                f"Match: {pair.kalshi.ticker}"
            )
            self._log.info(
                f"Paper arb {trade_id}  edge={pair.net_edge:.1%}  size=${size:.2f}"
            )
            return trade

        # Live: execute both legs simultaneously
        t0 = time.time()
        poly_ok, kalshi_ok = await asyncio.gather(
            self._place_poly_order(pair, size),
            self._place_kalshi_order(pair, size),
        )
        ms = (time.time() - t0) * 1000

        poly_success,   poly_ref   = poly_ok
        kalshi_success, kalshi_ref = kalshi_ok

        if poly_success and kalshi_success:
            trade.status = "OPEN"
            self.open_trades[trade_id] = trade
            self.db.save(trade)
            await self.tg.send(
                f"🟢 <b>LIVE ARB</b> ({ms:.0f}ms)\n"
                f"{pair.poly.question[:80]}\n"
                f"Poly: {poly_ref}  |  Kalshi: {kalshi_ref}\n"
                f"Net edge: {pair.net_edge:.1%}  Size: ${size:.2f}/leg"
            )
            self._log.info(f"Live arb {trade_id} in {ms:.0f}ms")
            return trade
        else:
            # Half-executed — log as error, manual intervention needed
            msg = (
                f"❌ <b>PARTIAL EXECUTION</b> — manual check required!\n"
                f"Poly: {'✅' if poly_success else '❌'} {poly_ref}\n"
                f"Kalshi: {'✅' if kalshi_success else '❌'} {kalshi_ref}"
            )
            self._log.error(msg)
            await self.tg.send(msg)
            return None

    async def settle_paper_trades(self):
        """Mark paper trades as settled after expiry."""
        now = time.time()
        for tid, t in list(self.open_trades.items()):
            if now - t.entry_time > 30 * 24 * 3600:  # 30 days max
                t.status   = "EXPIRED"
                t.exit_time = now
                t.pnl      = t.size * t.net_edge   # estimated profit
                del self.open_trades[tid]
                self.db.save(t)

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


# ─── Main Bot ─────────────────────────────────────────────────────────────────

class ArbBot:
    def __init__(self, live: bool, balance: float, creds: Credentials):
        self.paper   = not live
        self._log    = logging.getLogger("arb_bot")
        self.tg      = Telegram(creds.tg_token, creds.tg_chat_id)
        self.db      = DB()
        self.poly    = PolyScanner()
        self.kalshi  = KalshiScanner(creds.kalshi_api_key, creds.kalshi_api_secret)
        self.exec    = Executor(self.paper, self.db, self.tg, creds)
        self.risk    = RiskManager(self.tg, self.db, balance)
        self._running       = False
        self._traded: set   = set()   # pair keys already traded this day
        self._last_day: Optional[date] = None

    async def run(self):
        self._running = True
        mode = "PAPER" if self.paper else "LIVE ⚠️"
        self._log.info(f"Starting [{mode}]")
        await self.tg.send(
            f"🚀 <b>Arb Bot started [{mode}]</b>\n"
            f"Balance: ${self.risk.portfolio:.2f}\n"
            f"Min edge: {MIN_EXECUTION_EDGE:.0%}  Max pos: {MAX_POSITION_PCT:.0%}\n"
            f"Strategy: Polymarket ↔ Kalshi"
        )
        await self._loop()

    async def _loop(self):
        while self._running:
            try:
                today = date.today()
                if self._last_day != today:
                    await self.risk.daily_reset()
                    self._traded.clear()
                    self._last_day = today

                await self.risk.heartbeat()

                if await self.risk.should_halt():
                    await asyncio.sleep(5)
                    continue

                if self.paper:
                    await self.exec.settle_paper_trades()

                poly_markets, kalshi_markets = await asyncio.gather(
                    self.poly.scan(),
                    self.kalshi.scan(),
                )

                pairs = find_arb_pairs(poly_markets, kalshi_markets)

                if pairs:
                    self._log.info(f"Found {len(pairs)} arb opportunities")

                for pair in pairs:
                    pair_key = f"{pair.poly.market_id}:{pair.kalshi.ticker}"
                    if pair_key in self._traded:
                        continue

                    if await self.risk.should_halt():
                        break

                    size = kelly_size(self.risk.portfolio, pair.net_edge)
                    if size < 1.0:
                        continue

                    self._log.info(
                        f"ARB {pair.net_edge:.1%} net edge\n"
                        f"  Poly: {pair.poly.question[:60]}\n"
                        f"  YES @ {pair.poly_yes:.3f}  +  Kalshi NO @ {pair.kalshi_no:.3f}\n"
                        f"  Cost: {pair.poly_yes + pair.kalshi_no:.3f}  Profit: {pair.gross_edge:.3f}/contract"
                    )

                    trade = await self.exec.execute(pair, size)
                    if trade:
                        self._traded.add(pair_key)

                await asyncio.sleep(SCAN_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.error(f"Loop error: {e}", exc_info=True)
                await self.tg.send(f"❌ Arb bot error: {str(e)[:200]}")
                await asyncio.sleep(10)

    async def stop(self):
        self._running = False
        await self.poly.close()
        await self.kalshi.close()
        await self.exec.close()
        await self.tg.send("🔴 <b>Arb bot stopped.</b>")
        await self.tg.close()
        self._log.info("Arb bot stopped.")


# ─── Entry point ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polymarket-Kalshi Arbitrage Bot")
    p.add_argument("--live",               action="store_true")
    p.add_argument("--confirm",            action="store_true")
    p.add_argument("--i-understand-risks", action="store_true", dest="risks")
    p.add_argument("--balance", type=float, default=300.0)
    return p.parse_args()


async def main():
    setup_logging()
    args = parse_args()
    log  = logging.getLogger("main")
    live = args.live and args.confirm and args.risks

    if args.live and not live:
        log.error("Live requires: --live --confirm --i-understand-risks")
        sys.exit(1)

    if live:
        log.warning("=" * 60)
        log.warning("  LIVE TRADING — REAL MONEY AT RISK")
        log.warning("=" * 60)
    else:
        log.info("=" * 60)
        log.info("  PAPER MODE  (no real money)")
        log.info("=" * 60)

    creds = collect_credentials(live)
    _scrubber.register(
        creds.poly_api_key, creds.poly_api_secret, creds.poly_passphrase,
        creds.poly_priv_key, creds.alchemy_rpc,
        creds.kalshi_api_key, creds.kalshi_api_secret,
        creds.tg_token, creds.tg_chat_id,
    )

    bot  = ArbBot(live=live, balance=args.balance, creds=creds)
    loop = asyncio.get_event_loop()

    def _shutdown(*_):
        log.info("Shutdown signal received")
        loop.create_task(bot.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass

    try:
        await bot.run()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
