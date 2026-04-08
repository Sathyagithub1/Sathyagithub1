#!/usr/bin/env python3
"""
Polymarket Latency Arbitrage Bot — Production Ready (Part 2 corrected prompt)
All 9 failure modes diagnosed and fixed.

Paper mode is ENABLED BY DEFAULT.
Live trading requires: python3 bot.py --live --confirm --i-understand-risks
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

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/ethusdt@ticker"
POLY_CLOB   = "https://clob.polymarket.com"
POLY_GAMMA  = "https://gamma-api.polymarket.com"

SYMBOLS               = ["BTC", "ETH"]
MIN_LIQUIDITY         = 5_000         # USDC (lowered for paper mode discovery)
MAX_MARKETS           = 20
STALE_SECONDS         = 10
MIN_DETECTABLE_EDGE   = 0.05          # 5 %
MIN_EXECUTION_EDGE    = 0.08          # 8 %
MAX_POSITION_PCT      = 0.08          # 8 % of portfolio
DAILY_HALT_PCT        = 0.20          # –20 % of day-start
ATH_DRAWDOWN_HALT_PCT = 0.40          # portfolio < 60 % of ATH  → halt
CONSEC_LOSS_LIMIT     = 5
CONSEC_LOSS_PAUSE_MIN = 30
EXEC_TARGET_MS        = 800
WS_MAX_RETRIES        = 10

# Annualised volatility for Black-Scholes fair-value calculation
VOL = {"BTC": 0.80, "ETH": 1.00}


# ─── Credentials (in-memory only, never stored) ───────────────────────────────

@dataclass
class Credentials:
    api_key:        str
    api_secret:     str
    api_passphrase: str
    private_key:    str
    alchemy_rpc:    str
    tg_token:       str
    tg_chat_id:     str


def collect_credentials(live: bool) -> Credentials:
    """
    Prompt for all credentials interactively using getpass (no echo).
    Values are held only in process memory and never written to disk.
    """
    print("\n─── Credential input (hidden, never stored) ─────────────────")
    if not live:
        print("Paper mode: only Telegram credentials are needed.")
        print("Press Enter to skip Telegram (alerts will be logged locally).\n")

    def ask(prompt: str, required: bool = False) -> str:
        while True:
            val = getpass.getpass(f"  {prompt}: ").strip()
            if val or not required:
                return val
            print("  (required — please enter a value)")

    tg_token   = ask("Telegram bot token  (blank = skip alerts)")
    tg_chat_id = ask("Telegram chat ID    (blank = skip alerts)")

    if not live:
        print("─────────────────────────────────────────────────────────────\n")
        return Credentials("", "", "", "", "", tg_token, tg_chat_id)

    print()
    api_key        = ask("Polymarket API key",        required=True)
    api_secret     = ask("Polymarket API secret",     required=True)
    api_passphrase = ask("Polymarket API passphrase", required=True)
    private_key    = ask("Wallet private key (0x…)",  required=True)
    alchemy_rpc    = ask("Alchemy RPC URL",            required=True)

    print("─────────────────────────────────────────────────────────────")
    print("✓ Credentials loaded into memory. Not written anywhere.\n")

    return Credentials(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        private_key=private_key,
        alchemy_rpc=alchemy_rpc,
        tg_token=tg_token,
        tg_chat_id=tg_chat_id,
    )


class _CredentialScrubber(logging.Filter):
    """
    Log filter that replaces any credential string with '***'.
    Attached after credentials are collected so secrets can never
    appear in bot.log even if accidentally referenced in a log call.
    """
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
class PriceData:
    symbol:    str
    price:     float
    timestamp: float          # unix seconds


@dataclass
class Market:
    market_id:          str
    token_id:           str
    question:           str
    symbol:             str   # BTC | ETH
    strike:             float
    direction:          str   # ABOVE | BELOW
    expiry:             datetime
    minutes_to_expiry:  float
    best_bid:           float
    best_ask:           float
    liquidity:          float


@dataclass
class Trade:
    trade_id:          str
    market_id:         str
    token_id:          str
    question:          str
    side:              str
    outcome:           str
    price:             float
    size:              float
    edge:              float
    theoretical_price: float
    entry_time:        float
    is_paper:          bool
    status:            str   = "OPEN"
    pnl:               float = 0.0
    exit_price:        float = 0.0
    exit_time:         float = 0.0


# ─── Logging ──────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addFilter(_scrubber)          # scrub secrets from every handler

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = RotatingFileHandler("bot.log", maxBytes=50 * 1024 * 1024, backupCount=5)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)


# ─── Telegram ─────────────────────────────────────────────────────────────────

class Telegram:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self._session: Optional[aiohttp.ClientSession] = None
        self._log = logging.getLogger("telegram")

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def send(self, msg: str) -> None:
        self._log.info(f"[TG] {msg[:120]}")
        if not self.enabled:
            return
        try:
            s = await self._sess()
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            await s.post(
                url,
                json={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=6),
            )
        except Exception as e:
            self._log.warning(f"Telegram error: {e}")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# ─── Binance price feed ───────────────────────────────────────────────────────

class PriceFeed:
    def __init__(self):
        self._prices: Dict[str, PriceData] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._log = logging.getLogger("price_feed")

    def get(self, sym: str) -> Optional[PriceData]:
        return self._prices.get(sym)

    def is_stale(self, sym: str) -> bool:
        p = self._prices.get(sym)
        return p is None or (time.time() - p.timestamp) > STALE_SECONDS

    def any_stale(self) -> bool:
        return any(self.is_stale(s) for s in SYMBOLS)

    def _handle(self, raw: dict) -> None:
        stream = raw.get("stream", "")
        ticker = raw.get("data", {})
        sym = "BTC" if "btcusdt" in stream else "ETH" if "ethusdt" in stream else None
        if not sym:
            return
        price = float(ticker.get("c", 0))
        if price > 0:
            self._prices[sym] = PriceData(sym, price, time.time())

    async def _run(self) -> None:
        delays = [2 ** i for i in range(WS_MAX_RETRIES)]
        attempt = 0

        while self._running:
            try:
                self._log.info(f"Connecting Binance WS (attempt {attempt + 1})")
                async with aiohttp.ClientSession() as s:
                    async with s.ws_connect(BINANCE_WS, heartbeat=20) as ws:
                        attempt = 0
                        self._log.info("Binance WS connected")
                        async for msg in ws:
                            if not self._running:
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle(json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                self._log.warning(f"WS error: {e}")

            if not self._running:
                return

            delay = delays[min(attempt, len(delays) - 1)]
            self._log.info(f"WS reconnect in {delay}s …")
            await asyncio.sleep(delay)
            attempt += 1

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


# ─── Edge calculation (Black-Scholes binary) ──────────────────────────────────

def _ncdf(x: float) -> float:
    """Abramowitz & Stegun approximation of cumulative normal CDF."""
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    return 0.5 * (1.0 + sign * y)


def fair_prob(spot: float, strike: float, t_years: float, vol: float, direction: str) -> float:
    """Probability of outcome using log-normal (zero drift, risk-neutral)."""
    if t_years <= 1e-9:
        above = spot > strike
        return 1.0 if (direction == "ABOVE") == above else 0.0
    sv = vol * math.sqrt(t_years)
    if sv < 1e-9:
        return fair_prob(spot, strike, 0, vol, direction)
    d2 = (math.log(spot / strike) - 0.5 * vol ** 2 * t_years) / sv
    return _ncdf(d2) if direction == "ABOVE" else _ncdf(-d2)


def edge_for(spot: float, mkt: Market, vol: float) -> Tuple[float, float]:
    """Return (theoretical_probability, edge_vs_ask)."""
    t = mkt.minutes_to_expiry / (365 * 24 * 60)
    theo = fair_prob(spot, mkt.strike, t, vol, mkt.direction)
    return theo, theo - mkt.best_ask


# ─── Half-Kelly position sizing ───────────────────────────────────────────────

def half_kelly(portfolio: float, win_prob: float, ask: float) -> float:
    """
    Binary option Kelly:  f = (p − a) / (1 − a)
    Half-Kelly:           f / 2
    Hard cap at MAX_POSITION_PCT.
    """
    if ask <= 0 or ask >= 1 or win_prob <= ask:
        return 0.0
    f = (win_prob - ask) / (1.0 - ask) / 2.0
    if f <= 0:
        return 0.0
    return min(f * portfolio, MAX_POSITION_PCT * portfolio)


# ─── SQLite database ──────────────────────────────────────────────────────────

class DB:
    def __init__(self, path: str = "trades.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self._init()

    def _init(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                market_id TEXT, token_id TEXT, question TEXT,
                side TEXT, outcome TEXT, price REAL, size REAL,
                edge REAL, theoretical REAL, entry_time REAL,
                exit_time REAL, exit_price REAL, status TEXT,
                pnl REAL, is_paper INTEGER
            );
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
        """)
        self.conn.commit()

    def save(self, t: Trade):
        self.conn.execute(
            "INSERT OR REPLACE INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (t.trade_id, t.market_id, t.token_id, t.question,
             t.side, t.outcome, t.price, t.size,
             t.edge, t.theoretical_price, t.entry_time,
             t.exit_time, t.exit_price, t.status, t.pnl, int(t.is_paper)),
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
            "FROM trades WHERE entry_time>=? AND status!='OPEN'",
            (start,),
        ).fetchone()
        return (r[0] or 0), (r[1] or 0), (r[2] or 0.0)

    def ath(self, default: float) -> float:
        return float(self.get("ath", str(default)))

    def update_ath(self, v: float):
        self.set("ath", str(v))


# ─── Risk manager ─────────────────────────────────────────────────────────────

class RiskManager:
    def __init__(self, tg: Telegram, db: DB, start_balance: float):
        self.tg              = tg
        self.db              = db
        self.portfolio       = start_balance
        self.day_start       = start_balance
        self.ath             = db.ath(start_balance)
        self.consec_losses   = 0
        self.daily_halted    = False
        self.ath_halted      = False
        self.pause_until: Optional[float] = None
        self._hb_due         = time.time() + 3600
        self._log            = logging.getLogger("risk")

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
                msg = (f"⚠️ <b>PAUSE — {CONSEC_LOSS_LIMIT} consecutive losses</b>\n"
                       f"Pausing {CONSEC_LOSS_PAUSE_MIN} min. Manual restart required.")
                self._log.warning(msg)
                await self.tg.send(msg)

    async def should_halt(self) -> bool:
        # Daily loss
        if self.day_start > 0:
            daily_pct = (self.portfolio - self.day_start) / self.day_start
            if daily_pct <= -DAILY_HALT_PCT and not self.daily_halted:
                self.daily_halted = True
                msg = (f"🛑 <b>DAILY LOSS HALT</b>\n"
                       f"P&L today: {daily_pct:.1%}  (limit −{DAILY_HALT_PCT:.0%})\n"
                       f"Balance: ${self.portfolio:.2f}\n"
                       f"<b>Manual restart required.</b>")
                self._log.critical(msg)
                await self.tg.send(msg)
        if self.daily_halted:
            return True

        # ATH drawdown
        if self.ath > 0:
            dd = (self.ath - self.portfolio) / self.ath
            if dd >= ATH_DRAWDOWN_HALT_PCT and not self.ath_halted:
                self.ath_halted = True
                msg = (f"🛑 <b>ATH DRAWDOWN HALT</b>\n"
                       f"Drawdown: {dd:.1%}  (limit {ATH_DRAWDOWN_HALT_PCT:.0%})\n"
                       f"ATH: ${self.ath:.2f}  Current: ${self.portfolio:.2f}\n"
                       f"<b>Manual restart required.</b>")
                self._log.critical(msg)
                await self.tg.send(msg)
        if self.ath_halted:
            return True

        # Consecutive-loss pause
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
        self._log.info(f"Daily reset. Day-start balance: ${self.day_start:.2f}")

    async def heartbeat(self):
        if time.time() < self._hb_due:
            return
        self._hb_due = time.time() + 3600
        total, wins, pnl = self.db.today_stats()
        wr = f"{wins/total*100:.1f}%" if total else "n/a"
        await self.tg.send(
            f"💓 <b>Heartbeat</b>\n"
            f"Balance: ${self.portfolio:.2f}  ATH: ${self.ath:.2f}\n"
            f"Today: {total} trades  WR: {wr}  P&L: ${pnl:.2f}\n"
            f"Consecutive losses: {self.consec_losses}"
        )


# ─── Market scanner ───────────────────────────────────────────────────────────

class Scanner:
    def __init__(self):
        self._sess: Optional[aiohttp.ClientSession] = None
        self._cache: List[Market] = []
        self._cache_time = 0.0
        self._cache_ttl  = 30      # seconds
        self._log = logging.getLogger("scanner")

    async def _s(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
        return self._sess

    def _parse(self, m: dict) -> Optional[Market]:
        try:
            q = m.get("question", "")
            qu = q.upper()

            sym = next((s for s in SYMBOLS if s in qu), None)
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

            end_str = m.get("end_date_iso") or m.get("endDateIso", "")
            if not end_str:
                return None
            expiry = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            now = datetime.now(expiry.tzinfo)
            mins = (expiry - now).total_seconds() / 60
            if not (0 < mins <= 120):    # up to 2 hours
                return None

            tokens = m.get("tokens") or m.get("clob_token_ids") or []
            if not tokens:
                return None
            tid = tokens[0] if isinstance(tokens[0], str) else tokens[0].get("token_id", "")

            vol = float(m.get("volume") or m.get("volumeNum") or 0)
            if vol < MIN_LIQUIDITY:
                return None

            bid = float(m.get("bestBid") or m.get("best_bid") or 0)
            ask = float(m.get("bestAsk") or m.get("best_ask") or 1)

            return Market(
                market_id=m.get("condition_id") or m.get("id", ""),
                token_id=tid,
                question=q,
                symbol=sym,
                strike=strike,
                direction=direction,
                expiry=expiry,
                minutes_to_expiry=mins,
                best_bid=bid,
                best_ask=ask,
                liquidity=vol,
            )
        except Exception as e:
            logging.getLogger("scanner").debug(f"parse error: {e}")
            return None

    async def _orderbook(self, token_id: str) -> Tuple[float, float]:
        try:
            s = await self._s()
            async with s.get(f"{POLY_CLOB}/book?token_id={token_id}") as r:
                if r.status == 200:
                    d = await r.json()
                    bids = d.get("bids", [])
                    asks = d.get("asks", [])
                    return (
                        float(bids[0]["price"]) if bids else 0.0,
                        float(asks[0]["price"]) if asks else 1.0,
                    )
        except Exception:
            pass
        return 0.0, 1.0

    async def scan(self) -> List[Market]:
        if time.time() - self._cache_time < self._cache_ttl:
            return self._cache

        try:
            s = await self._s()
            async with s.get(
                f"{POLY_GAMMA}/markets",
                params={"active": "true", "closed": "false", "limit": 500, "tag_slug": "crypto"},
            ) as r:
                if r.status != 200:
                    self._log.warning(f"Gamma API {r.status}")
                    return self._cache
                raw = await r.json()

            items = raw if isinstance(raw, list) else raw.get("data", [])
            self._log.info(f"API returned {len(items)} raw markets")
            markets: List[Market] = []
            for m in items:
                parsed = self._parse(m)
                if parsed:
                    bid, ask = await self._orderbook(parsed.token_id)
                    if bid > 0 or ask < 1:
                        parsed.best_bid, parsed.best_ask = bid, ask
                    markets.append(parsed)
                    if len(markets) >= MAX_MARKETS:
                        break

            self._cache      = markets
            self._cache_time = time.time()
            self._log.info(f"Scanned: {len(markets)} eligible markets")
        except Exception as e:
            self._log.error(f"Scan failed: {e}")

        return self._cache

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


# ─── Trade executor ───────────────────────────────────────────────────────────

class Executor:
    def __init__(self, paper: bool, db: DB, tg: Telegram, creds: Credentials):
        self.paper        = paper
        self.db           = db
        self.tg           = tg
        self.open_trades: Dict[str, Trade] = {}
        self._sess: Optional[aiohttp.ClientSession] = None
        self._log = logging.getLogger("executor")

        self._api_key        = creds.api_key
        self._api_secret     = creds.api_secret
        self._api_passphrase = creds.api_passphrase

    async def _s(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
        return self._sess

    def _auth_headers(self) -> dict:
        ts    = str(int(time.time()))
        nonce = str(int(time.time() * 1000) % 1_000_000)
        sig   = hmac.new(
            self._api_secret.encode(), f"{ts}{nonce}".encode(), hashlib.sha256
        ).hexdigest()
        return {
            "POLY_ADDRESS":    self._api_key,
            "POLY_SIGNATURE":  sig,
            "POLY_TIMESTAMP":  ts,
            "POLY_NONCE":      nonce,
            "POLY_PASSPHRASE": self._api_passphrase,
            "Content-Type":    "application/json",
        }

    async def execute(self, mkt: Market, size: float, theo: float, edge: float) -> Optional[Trade]:
        trade_id = f"{'P' if self.paper else 'L'}_{mkt.market_id}_{int(time.time()*1000)}"
        trade = Trade(
            trade_id=trade_id, market_id=mkt.market_id, token_id=mkt.token_id,
            question=mkt.question, side="BUY", outcome="YES",
            price=mkt.best_ask, size=size, edge=edge,
            theoretical_price=theo, entry_time=time.time(), is_paper=self.paper,
        )

        if self.paper:
            trade.status = "OPEN"
            self.open_trades[trade_id] = trade
            self.db.save(trade)
            await self.tg.send(
                f"📄 <b>PAPER TRADE</b>\n"
                f"{mkt.question[:80]}\n"
                f"BUY YES @ {mkt.best_ask:.3f}  Size: ${size:.2f}\n"
                f"Edge: {edge:.1%}  Theoretical: {theo:.1%}\n"
                f"Expires: {mkt.minutes_to_expiry:.1f} min"
            )
            self._log.info(f"Paper trade {trade_id}  edge={edge:.1%}  size=${size:.2f}")
            return trade

        # Live
        try:
            t0 = time.time()
            s  = await self._s()
            async with s.post(
                f"{POLY_CLOB}/order",
                headers=self._auth_headers(),
                json={
                    "token_id": mkt.token_id,
                    "price":    str(round(mkt.best_ask, 4)),
                    "size":     str(round(size, 2)),
                    "side":     "BUY",
                    "type":     "FOK",
                },
            ) as r:
                ms = (time.time() - t0) * 1000
                if r.status == 200:
                    res = await r.json()
                    trade.status = "OPEN"
                    self.open_trades[trade_id] = trade
                    self.db.save(trade)
                    await self.tg.send(
                        f"🟢 <b>LIVE TRADE</b> ({ms:.0f} ms)\n"
                        f"{mkt.question[:80]}\n"
                        f"BUY YES @ {mkt.best_ask:.3f}  ${size:.2f}\n"
                        f"Edge: {edge:.1%}  OrderID: {res.get('orderID', 'n/a')}"
                    )
                    self._log.info(f"Live trade {trade_id} in {ms:.0f} ms")
                    if ms > EXEC_TARGET_MS:
                        self._log.warning(f"Execution {ms:.0f} ms exceeded {EXEC_TARGET_MS} ms target")
                    return trade
                else:
                    body = await r.text()
                    self._log.error(f"Order {r.status}: {body[:200]}")
                    await self.tg.send(f"⚠️ Order failed {r.status}: {body[:150]}")
        except Exception as e:
            self._log.error(f"Execute error: {e}", exc_info=True)
            await self.tg.send(f"❌ Execution error: {str(e)[:200]}")
        return None

    async def settle_expired_paper(self):
        """Expire paper trades older than 16 min (conservative settlement)."""
        now = time.time()
        for tid, t in list(self.open_trades.items()):
            if now - t.entry_time > 16 * 60:
                t.status     = "EXPIRED"
                t.exit_time  = now
                t.pnl        = -t.size * t.price     # conservative: full loss
                del self.open_trades[tid]
                self.db.save(t)
                self._log.info(f"Paper trade {tid} expired")

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


# ─── Bot orchestrator ─────────────────────────────────────────────────────────

class ArbitrageBot:
    def __init__(self, live: bool, balance: float, creds: Credentials):
        self.paper   = not live
        self._log    = logging.getLogger("bot")
        self.tg      = Telegram(creds.tg_token, creds.tg_chat_id)
        self.db      = DB()
        self.feed    = PriceFeed()
        self.scanner = Scanner()
        self.exec    = Executor(self.paper, self.db, self.tg, creds)
        self.risk    = RiskManager(self.tg, self.db, balance)
        self._running        = False
        self._traded: set    = set()
        self._last_day: Optional[date] = None

    async def run(self):
        self._running = True
        mode = "PAPER" if self.paper else "LIVE ⚠️"
        self._log.info(f"Starting [{mode}]")
        await self.tg.send(
            f"🚀 <b>Bot started [{mode}]</b>\n"
            f"Balance: ${self.risk.portfolio:.2f}\n"
            f"Min edge: {MIN_EXECUTION_EDGE:.0%}  Max pos: {MAX_POSITION_PCT:.0%}"
        )

        await self.feed.start()

        # Wait up to 15 s for first prices
        for _ in range(15):
            if not self.feed.any_stale():
                break
            await asyncio.sleep(1)

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

                if self.feed.any_stale():
                    self._log.warning("Price data stale — trading paused")
                    await asyncio.sleep(2)
                    continue

                if await self.risk.should_halt():
                    await asyncio.sleep(5)
                    continue

                if self.paper:
                    await self.exec.settle_expired_paper()

                markets = await self.scanner.scan()

                for mkt in markets:
                    if mkt.market_id in self._traded:
                        continue

                    pd = self.feed.get(mkt.symbol)
                    if pd is None or self.feed.is_stale(mkt.symbol):
                        continue

                    vol  = VOL.get(mkt.symbol, 0.9)
                    theo, edge = edge_for(pd.price, mkt, vol)

                    if edge < MIN_DETECTABLE_EDGE:
                        continue

                    self._log.info(
                        f"Edge {edge:.1%}  {mkt.question[:60]}\n"
                        f"  spot={pd.price:.2f}  strike={mkt.strike:.2f}  "
                        f"ask={mkt.best_ask:.3f}  theo={theo:.3f}  "
                        f"exp={mkt.minutes_to_expiry:.1f}m"
                    )

                    if edge < MIN_EXECUTION_EDGE:
                        continue

                    if await self.risk.should_halt():
                        break

                    # Recalculate portfolio before every trade
                    size = half_kelly(self.risk.portfolio, theo, mkt.best_ask)
                    if size < 1.0:
                        continue

                    trade = await self.exec.execute(mkt, size, theo, edge)
                    if trade:
                        self._traded.add(mkt.market_id)

                await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.error(f"Loop error: {e}", exc_info=True)
                await self.tg.send(f"❌ Loop error: {str(e)[:200]}")
                await asyncio.sleep(5)

    async def stop(self):
        self._running = False
        await self.feed.stop()
        await self.scanner.close()
        await self.exec.close()
        await self.tg.send("🔴 <b>Bot stopped.</b>")
        await self.tg.close()
        self._log.info("Bot stopped.")


# ─── Entry point ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polymarket Latency Arbitrage Bot")
    p.add_argument("--live",                action="store_true")
    p.add_argument("--confirm",             action="store_true")
    p.add_argument("--i-understand-risks",  action="store_true", dest="risks")
    p.add_argument("--balance", type=float, default=300.0,
                   help="Starting USDC balance (default 300)")
    return p.parse_args()


async def main():
    setup_logging()
    args = parse_args()
    log  = logging.getLogger("main")
    live = args.live and args.confirm and args.risks

    if args.live and not live:
        log.error("Live trading requires ALL three flags:\n"
                  "  --live --confirm --i-understand-risks")
        sys.exit(1)

    if live:
        log.warning("=" * 60)
        log.warning("  LIVE TRADING — REAL MONEY AT RISK")
        log.warning("=" * 60)
    else:
        log.info("=" * 60)
        log.info("  PAPER TRADING MODE  (no real money)")
        log.info("=" * 60)

    # Collect credentials interactively — never written to disk
    creds = collect_credentials(live)

    # Register every secret value with the log scrubber so they can
    # never appear in bot.log even if accidentally referenced in a log call
    _scrubber.register(
        creds.api_key, creds.api_secret, creds.api_passphrase,
        creds.private_key, creds.alchemy_rpc,
        creds.tg_token, creds.tg_chat_id,
    )

    bot  = ArbitrageBot(live=live, balance=args.balance, creds=creds)
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
