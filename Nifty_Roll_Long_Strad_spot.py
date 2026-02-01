#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – SAFE ATM ROLLING (TOLERANCE BASED) + STOCKO LIVE
---------------------------------------------------------------------
✔ Entry when spot touches ATM ± ENTRY_TOL
✔ LONG straddle: BUY CE + BUY PE on entry
✔ Roll only on NEW 50pt strike ± ENTRY_TOL (exit old -> enter new)
✔ No midpoint flip-flop
✔ FUT-based ATR
✔ VIX logging
✔ Per-minute M2M snapshot logging (exactly once per minute while OPEN)
✔ Entry + Exit prices persisted (ROLL + FINAL EXIT)
✔ PostgreSQL (Railway safe, schema auto-migration)

🟢 STOCKO integration (using your proven logic)
- Symbol -> Stocko token via /api/v1/search (NFO)
- Numeric-only user_order_id (<=15 digits) with offsets to ensure uniqueness
- Places MARKET orders via /api/v1/orders

ENV REQUIRED (Railway):
- STOCKO_BASE_URL (default https://api.stocko.in)
- STOCKO_ACCESS_TOKEN
- STOCKO_CLIENT_ID
Optional:
- STOCKO_PRODUCT (default NRML)
- STOCKO_ORDER_TYPE (default MARKET)
- STOCKO_VALIDITY (default DAY)

NOTE:
- This script executes LIVE orders when STOCKO_* env vars are set.
"""

import os, time, math, pytz, requests
from datetime import datetime, time as dt_time
import psycopg2
from kiteconnect import KiteConnect

# =========================================================
# CONFIG (ENV)
# =========================================================

API_KEY      = os.getenv("KITE_API_KEY", "")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL")

ENTRY_TOL     = int(os.getenv("ENTRY_TOL", 4))
QTY_PER_LEG   = int(os.getenv("QTY_PER_LEG", 65))
PROFIT_TARGET = float(os.getenv("PROFIT_TARGET", 1500))
STOP_LOSS     = float(os.getenv("STOP_LOSS", 1500))

MARKET_START_TIME = os.getenv("MARKET_START_TIME", "09:15")
MARKET_END_TIME   = os.getenv("MARKET_END_TIME", "15:30")

INDEX_SYMBOL = "NSE:NIFTY 50"
STRIKE_STEP  = 50
ATR_PERIOD   = 14
TICK_INTERVAL = 1

TABLE_NAME = "nifty_long_strang_roll"
MARKET_TZ = pytz.timezone("Asia/Kolkata")

# --- STOCKO (proven logic settings) ---
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").rstrip("/")
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "")
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "")

STOCKO_PRODUCT   = os.getenv("STOCKO_PRODUCT", "NRML")
STOCKO_ORDER_TYPE = os.getenv("STOCKO_ORDER_TYPE", "MARKET")
STOCKO_VALIDITY  = os.getenv("STOCKO_VALIDITY", "DAY")

# =========================================================
# VALIDATION
# =========================================================

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("Missing Kite credentials (KITE_API_KEY / KITE_ACCESS_TOKEN)")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# Stocko must be present for live placement
if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
    raise RuntimeError("Missing Stocko credentials (STOCKO_ACCESS_TOKEN / STOCKO_CLIENT_ID)")

# =========================================================
# TIME HELPERS
# =========================================================

def parse_time(t):
    h, m = map(int, t.split(":"))
    return dt_time(h, m)

MARKET_START = parse_time(MARKET_START_TIME)
MARKET_END   = parse_time(MARKET_END_TIME)

def in_market_hours(now):
    return MARKET_START <= now.time() <= MARKET_END

def nowts():
    return datetime.now(MARKET_TZ).strftime("%Y-%m-%d %H:%M:%S")

# =========================================================
# KITE
# =========================================================

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# =========================================================
# VIX
# =========================================================

def get_vix():
    q = kite.quote(["NSE:INDIA VIX"])
    d = q["NSE:INDIA VIX"]
    return float(d["ohlc"]["close"]), float(d["last_price"])

# =========================================================
# INSTRUMENT RESOLUTION
# =========================================================

NFO = kite.instruments("NFO")

def get_nearest_option(strike, opt_type):
    opts = [
        i for i in NFO
        if i["name"] == "NIFTY"
        and i["instrument_type"] == opt_type
        and i["strike"] == strike
    ]
    if not opts:
        return None
    opts.sort(key=lambda x: x["expiry"])
    return "NFO:" + opts[0]["tradingsymbol"]

def get_nearest_nifty_fut():
    futs = [i for i in NFO if i["name"] == "NIFTY" and i["instrument_type"] == "FUT"]
    futs.sort(key=lambda x: x["expiry"])
    return "NFO:" + futs[0]["tradingsymbol"]

FUT_SYMBOL = get_nearest_nifty_fut()

# =========================================================
# ATR BUILDER (FUT)
# =========================================================

class FutAtrBuilder:
    def __init__(self, period):
        self.period = period
        self.trs = []
        self.last_min = None
        self.h = self.l = self.c = self.prev_c = None
        self.atr = None

    def update(self, now, ltp_):
        mk = now.replace(second=0, microsecond=0)

        if self.last_min is None:
            self.last_min = mk
            self.h = self.l = self.c = self.prev_c = ltp_
            return None

        if mk == self.last_min:
            self.h = max(self.h, ltp_)
            self.l = min(self.l, ltp_)
            self.c = ltp_
            return self.atr

        tr = max(
            self.h - self.l,
            abs(self.h - self.prev_c),
            abs(self.l - self.prev_c),
        )
        self.trs.append(tr)

        if len(self.trs) >= self.period:
            self.atr = round(sum(self.trs[-self.period:]) / self.period, 2)

        self.prev_c = self.c
        self.last_min = mk
        self.h = self.l = self.c = ltp_
        return self.atr

# =========================================================
# DB
# =========================================================

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def ensure_table():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            timestamp TIMESTAMPTZ,
            status TEXT,
            event TEXT,
            reason TEXT,
            spot DOUBLE PRECISION,
            atm INTEGER,
            ce_symbol TEXT,
            pe_symbol TEXT,
            ce_entry DOUBLE PRECISION,
            pe_entry DOUBLE PRECISION,
            ce_exit DOUBLE PRECISION,
            pe_exit DOUBLE PRECISION,
            ce_ltp DOUBLE PRECISION,
            pe_ltp DOUBLE PRECISION,
            unreal_pnl DOUBLE PRECISION,
            realized_pnl DOUBLE PRECISION,
            atr DOUBLE PRECISION,
            vix_prev DOUBLE PRECISION,
            vix DOUBLE PRECISION
        );
        """)
        conn.commit()

        # Auto-migrate (safe)
        cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS ce_exit DOUBLE PRECISION;")
        cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS pe_exit DOUBLE PRECISION;")
        conn.commit()

def log_db(**row):
    cols = ",".join(row.keys())
    vals = tuple(row.values())
    ph = ",".join(["%s"] * len(vals))
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({ph})", vals)
        conn.commit()

# =========================================================
# HELPERS
# =========================================================

def ltp(symbol):
    return kite.ltp([symbol])[symbol]["last_price"]

def round_to_strike(price):
    return int(math.floor(price / STRIKE_STEP + 0.5) * STRIKE_STEP)

# =========================================================
# STOCKO – ORDER PLACEMENT (EXACT LOGIC YOU TRUST)
# =========================================================

def _stocko_headers():
    return {"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}", "Content-Type": "application/json"}

def stocko_search_token(keyword: str) -> int:
    url = f"{STOCKO_BASE_URL}/api/v1/search"
    r = requests.get(url, params={"key": keyword}, headers=_stocko_headers(), timeout=10)
    r.raise_for_status()
    data = r.json()
    result = data.get("result") or data.get("data", {}).get("result", [])
    for rec in result:
        if rec.get("exchange") == "NFO":
            return int(rec["token"])
    raise ValueError(f"No NFO token found for {keyword}")

def generate_numeric_order_id(offset=0):
    base = int(time.time() * 1000)  # ms
    return str(base + offset)[-15:]

def stocko_place_order_token(token: int, side: str, qty: int,
                             exchange="NFO", order_type=None,
                             product=None, validity=None,
                             price=0, trigger_price=0, offset=0):
    url = f"{STOCKO_BASE_URL}/api/v1/orders"
    order_id = generate_numeric_order_id(offset)
    payload = {
        "exchange": exchange,
        "order_type": (order_type or STOCKO_ORDER_TYPE),
        "instrument_token": int(token),
        "quantity": int(qty),
        "disclosed_quantity": 0,
        "order_side": side.upper(),
        "price": price,
        "trigger_price": trigger_price,
        "validity": (validity or STOCKO_VALIDITY),
        "product": (product or STOCKO_PRODUCT),
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": order_id,
        "market_protection_percentage": 0,
        "device": "WEB"
    }

    print(f"[{nowts()}] 🆔 Stocko {side.upper()} token={token} user_order_id={order_id}")
    r = requests.post(url, json=payload, headers=_stocko_headers(), timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Stocko {side.upper()} failed: {r.status_code} → {r.text}")
    return r.json()

def stocko_place_by_symbol(trading_symbol: str, side: str, qty: int, offset: int):
    """
    trading_symbol may be 'NFO:NIFTY....' or 'NIFTY....'
    Stocko search expects keyword like 'NIFTY....' (without 'NFO:').
    """
    keyword = trading_symbol.split(":", 1)[1] if ":" in trading_symbol else trading_symbol
    token = stocko_search_token(keyword)
    return stocko_place_order_token(token, side, qty, offset=offset)

def stocko_open_long_straddle(ce_symbol: str, pe_symbol: str):
    # LONG: BUY CE + BUY PE
    stocko_place_by_symbol(ce_symbol, "BUY", QTY_PER_LEG, offset=0)
    stocko_place_by_symbol(pe_symbol, "BUY", QTY_PER_LEG, offset=1)

def stocko_close_long_straddle(ce_symbol: str, pe_symbol: str):
    # EXIT: SELL CE + SELL PE
    stocko_place_by_symbol(ce_symbol, "SELL", QTY_PER_LEG, offset=2)
    stocko_place_by_symbol(pe_symbol, "SELL", QTY_PER_LEG, offset=3)

# =========================================================
# STATE
# =========================================================

position_open = False
active_atm = None
ce_symbol = pe_symbol = None
ce_entry = pe_entry = None
realized_pnl = 0.0

atr_builder = FutAtrBuilder(ATR_PERIOD)
last_snapshot_min = None

# =========================================================
# MAIN LOOP
# =========================================================

ensure_table()
print("🚀 NIFTY LONG STRADDLE STARTED (LIVE via STOCKO)")

while True:
    try:
        now = datetime.now(MARKET_TZ)

        if not in_market_hours(now):
            time.sleep(5)
            continue

        spot = ltp(INDEX_SYMBOL)
        fut_price = ltp(FUT_SYMBOL)
        atr = atr_builder.update(now, fut_price)
        vix_prev, vix = get_vix()

        atm = round_to_strike(spot)
        dist = abs(spot - atm)

        # ================= ENTRY =================
        if not position_open and dist <= ENTRY_TOL:
            ce_symbol = get_nearest_option(atm, "CE")
            pe_symbol = get_nearest_option(atm, "PE")
            if not ce_symbol or not pe_symbol:
                time.sleep(1)
                continue

            # LIVE ENTRY: BUY CE + BUY PE
            stocko_open_long_straddle(ce_symbol, pe_symbol)

            # Record entry prices (MARKET -> use LTP proxy)
            ce_entry = ltp(ce_symbol)
            pe_entry = ltp(pe_symbol)

            position_open = True
            active_atm = atm
            last_snapshot_min = None

            log_db(
                timestamp=now,
                status="OPEN",
                event="ENTRY",
                reason="ATM_TOUCH",
                spot=spot,
                atm=active_atm,
                ce_symbol=ce_symbol,
                pe_symbol=pe_symbol,
                ce_entry=ce_entry,
                pe_entry=pe_entry,
                ce_exit=None,
                pe_exit=None,
                ce_ltp=ce_entry,
                pe_ltp=pe_entry,
                unreal_pnl=0,
                realized_pnl=realized_pnl,
                atr=atr,
                vix_prev=vix_prev,
                vix=vix
            )

        # ================= SAFE ROLL =================
        if position_open and atm != active_atm and dist <= ENTRY_TOL:
            old_ce_ltp = ltp(ce_symbol)
            old_pe_ltp = ltp(pe_symbol)

            # Realize PnL on old legs (LONG)
            roll_pnl = (old_ce_ltp - ce_entry + old_pe_ltp - pe_entry) * QTY_PER_LEG
            realized_pnl += roll_pnl

            # LIVE ROLL EXIT: SELL old CE/PE
            stocko_close_long_straddle(ce_symbol, pe_symbol)

            log_db(
                timestamp=now,
                status="OPEN",
                event="ROLL",
                reason="ATM_TOUCH",
                spot=spot,
                atm=active_atm,
                ce_symbol=ce_symbol,
                pe_symbol=pe_symbol,
                ce_entry=ce_entry,
                pe_entry=pe_entry,
                ce_exit=old_ce_ltp,
                pe_exit=old_pe_ltp,
                ce_ltp=old_ce_ltp,
                pe_ltp=old_pe_ltp,
                unreal_pnl=0,
                realized_pnl=realized_pnl,
                atr=atr,
                vix_prev=vix_prev,
                vix=vix
            )

            # New option symbols at new ATM
            new_ce = get_nearest_option(atm, "CE")
            new_pe = get_nearest_option(atm, "PE")
            if not new_ce or not new_pe:
                # If cannot re-enter safely, go flat
                position_open = False
                active_atm = None
                ce_symbol = pe_symbol = None
                ce_entry = pe_entry = None
                last_snapshot_min = None
                time.sleep(1)
                continue

            ce_symbol, pe_symbol = new_ce, new_pe

            # LIVE ROLL ENTRY: BUY new CE/PE
            stocko_open_long_straddle(ce_symbol, pe_symbol)

            ce_entry = ltp(ce_symbol)
            pe_entry = ltp(pe_symbol)
            active_atm = atm
            last_snapshot_min = None

        # ================= POSITION MGMT =================
        if position_open:
            ce_ltp = ltp(ce_symbol)
            pe_ltp = ltp(pe_symbol)
            unreal = (ce_ltp - ce_entry + pe_ltp - pe_entry) * QTY_PER_LEG

            this_min = now.replace(second=0, microsecond=0)
            if last_snapshot_min is None or this_min > last_snapshot_min:
                last_snapshot_min = this_min
                log_db(
                    timestamp=now,
                    status="OPEN",
                    event="M2M",
                    reason="SNAPSHOT",
                    spot=spot,
                    atm=active_atm,
                    ce_symbol=ce_symbol,
                    pe_symbol=pe_symbol,
                    ce_entry=ce_entry,
                    pe_entry=pe_entry,
                    ce_exit=None,
                    pe_exit=None,
                    ce_ltp=ce_ltp,
                    pe_ltp=pe_ltp,
                    unreal_pnl=unreal,
                    realized_pnl=realized_pnl,
                    atr=atr,
                    vix_prev=vix_prev,
                    vix=vix
                )

            if unreal >= PROFIT_TARGET or unreal <= -STOP_LOSS:
                # LIVE FINAL EXIT: SELL CE/PE
                stocko_close_long_straddle(ce_symbol, pe_symbol)

                realized_pnl += unreal
                position_open = False

                log_db(
                    timestamp=now,
                    status="EXIT",
                    event="FINAL_EXIT",
                    reason="TARGET" if unreal >= PROFIT_TARGET else "SL",
                    spot=spot,
                    atm=active_atm,
                    ce_symbol=ce_symbol,
                    pe_symbol=pe_symbol,
                    ce_entry=ce_entry,
                    pe_entry=pe_entry,
                    ce_exit=ce_ltp,
                    pe_exit=pe_ltp,
                    ce_ltp=ce_ltp,
                    pe_ltp=pe_ltp,
                    unreal_pnl=unreal,
                    realized_pnl=realized_pnl,
                    atr=atr,
                    vix_prev=vix_prev,
                    vix=vix
                )

                active_atm = None
                ce_symbol = pe_symbol = None
                ce_entry = pe_entry = None
                last_snapshot_min = None

        time.sleep(TICK_INTERVAL)

    except Exception as e:
        print("❌ ERROR:", e)
        time.sleep(5)
