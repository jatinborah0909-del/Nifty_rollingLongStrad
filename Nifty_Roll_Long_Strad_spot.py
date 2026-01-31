#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – 50PT ATM ROLLING + PER-MIN M2M + TRADE FLAG + LIVE EXEC
-------------------------------------------------------------------------------
✅ Entry when spot touches ATM ± ENTRY_TOL
✅ Roll ONLY when spot is near a NEW 50pt strike (± ENTRY_TOL) AND strike != current
✅ Per-minute M2M snapshots persisted to DB (status="M2M")
✅ Exit prices (CE/PE) stored on:
   - Profit / SL (FINAL_EXIT)
   - Kill-switch (KILL_SWITCH_EXIT)
   - Strike roll (ROLL_EXIT)
✅ Explicit exit/entry rows persisted in DB (ROLL_EXIT + ROLL_ENTRY)
✅ FUT-based ATR builder (minute aggregation)
✅ VIX (prev close + live) stored
✅ LIVE / PAPER mode
✅ Stocko (optional) or Kite order placement
✅ Trade flag kill-switch (trade_flag.live_ls_nifty_spot) checked every minute (fail-safe)

NOTE:
- In PAPER mode: no real orders are placed; entry/exit prices are simulated using LTP.
"""

import os
import time
import math
import pytz
import requests
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

TRADE_MODE = os.getenv("TRADE_MODE", "PAPER").upper()           # PAPER / LIVE
EXECUTION_ENGINE = os.getenv("EXECUTION_ENGINE", "AUTO").upper() # AUTO / STOCKO / KITE

STOCKO_ORDER_URL  = os.getenv("STOCKO_ORDER_URL", "")
STOCKO_AUTH_TOKEN = os.getenv("STOCKO_AUTH_TOKEN", "")

INDEX_SYMBOL = "NSE:NIFTY 50"
STRIKE_STEP  = 50
ATR_PERIOD   = 14
TICK_INTERVAL = float(os.getenv("TICK_INTERVAL", 1))

TABLE_NAME = "nifty_long_strang_roll"
FLAG_TABLE = "trade_flag"
FLAG_COL   = "live_ls_nifty_spot"

MARKET_TZ = pytz.timezone("Asia/Kolkata")

# =========================================================
# VALIDATION
# =========================================================

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("Missing Kite credentials (KITE_API_KEY / KITE_ACCESS_TOKEN)")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

if TRADE_MODE not in ("PAPER", "LIVE"):
    raise RuntimeError("TRADE_MODE must be PAPER or LIVE")

# =========================================================
# TIME HELPERS
# =========================================================

def parse_time(t: str) -> dt_time:
    h, m = map(int, t.split(":"))
    return dt_time(h, m)

MARKET_START = parse_time(MARKET_START_TIME)
MARKET_END   = parse_time(MARKET_END_TIME)

def in_market_hours(now: datetime) -> bool:
    return MARKET_START <= now.time() <= MARKET_END

# =========================================================
# KITE
# =========================================================

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

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
            vix DOUBLE PRECISION,
            order_engine TEXT
        );
        """)
        conn.commit()

def log_db(**row):
    cols = ",".join(row.keys())
    vals = tuple(row.values())
    ph = ",".join(["%s"] * len(vals))
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({ph})", vals)
        conn.commit()

# =========================================================
# TRADE FLAG (READ ONLY, FAIL SAFE)
# =========================================================

def is_trade_allowed() -> bool:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT {FLAG_COL} FROM {FLAG_TABLE} WHERE id = 1")
            r = cur.fetchone()
            return bool(r[0]) if r else False
    except Exception as e:
        print("⚠️ FLAG READ ERROR:", e)
        return False  # FAIL SAFE: block trading

# =========================================================
# MARKET DATA
# =========================================================

def ltp(symbol: str) -> float:
    return float(kite.ltp([symbol])[symbol]["last_price"])

def round_to_strike(price: float) -> int:
    return int(math.floor(price / STRIKE_STEP + 0.5) * STRIKE_STEP)

def get_vix():
    q = kite.quote(["NSE:INDIA VIX"])
    d = q["NSE:INDIA VIX"]
    return float(d["ohlc"]["close"]), float(d["last_price"])

# =========================================================
# INSTRUMENT RESOLUTION
# =========================================================

def _safe_expiry_date(x):
    # Kite instruments() returns expiry as date or string in some environments
    try:
        return x["expiry"] if hasattr(x["expiry"], "year") else datetime.strptime(str(x["expiry"]), "%Y-%m-%d").date()
    except Exception:
        return None

print("📥 Loading NFO instruments (one-time)...")
NFO = kite.instruments("NFO")

def get_nearest_option(strike: int, opt_type: str, now_dt: datetime) -> str:
    today = now_dt.date()
    opts = []
    for i in NFO:
        if i.get("name") != "NIFTY":
            continue
        if i.get("instrument_type") != opt_type:
            continue
        if float(i.get("strike", 0)) != float(strike):
            continue
        exp = _safe_expiry_date(i)
        if exp is None:
            continue
        if exp < today:
            continue
        opts.append(i)
    if not opts:
        return None
    opts.sort(key=lambda x: _safe_expiry_date(x))
    return "NFO:" + opts[0]["tradingsymbol"]

def get_nearest_nifty_fut(now_dt: datetime) -> str:
    today = now_dt.date()
    futs = []
    for i in NFO:
        if i.get("name") != "NIFTY":
            continue
        if i.get("instrument_type") != "FUT":
            continue
        exp = _safe_expiry_date(i)
        if exp is None:
            continue
        if exp < today:
            continue
        futs.append(i)
    if not futs:
        return None
    futs.sort(key=lambda x: _safe_expiry_date(x))
    return "NFO:" + futs[0]["tradingsymbol"]

# =========================================================
# ATR BUILDER (FUT minute aggregation)
# =========================================================

class FutAtrBuilder:
    def __init__(self, period: int):
        self.period = period
        self.trs = []
        self.last_min = None
        self.h = self.l = self.c = self.prev_c = None
        self.atr = None

    def update(self, now: datetime, ltp_: float):
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

        # close out previous minute candle
        tr = max(
            self.h - self.l,
            abs(self.h - self.prev_c),
            abs(self.l - self.prev_c)
        )
        self.trs.append(tr)

        if len(self.trs) >= self.period:
            self.atr = round(sum(self.trs[-self.period:]) / self.period, 2)

        self.prev_c = self.c
        self.last_min = mk
        self.h = self.l = self.c = ltp_
        return self.atr

# =========================================================
# ORDER EXECUTION
# =========================================================

def place_leg(symbol: str, side: str, qty: int):
    """
    side: BUY or SELL
    returns: (engine_used, fill_price)
    """
    if not symbol:
        raise RuntimeError("place_leg called with empty symbol")

    if TRADE_MODE == "PAPER":
        return "PAPER", ltp(symbol)

    exch, tsym = symbol.split(":", 1)

    # Stocko path (if configured and selected)
    if EXECUTION_ENGINE in ("AUTO", "STOCKO") and STOCKO_ORDER_URL:
        headers = {
            "Authorization": f"Bearer {STOCKO_AUTH_TOKEN}",
            "Content-Type": "application/json"
        }
        payload = {
            "exchange": exch,
            "tradingsymbol": tsym,
            "transaction_type": side,
            "quantity": int(qty),
            "order_type": "MARKET",
            "product": "MIS"
        }
        r = requests.post(STOCKO_ORDER_URL, json=payload, headers=headers, timeout=10)
        r.raise_for_status()
        return "STOCKO", ltp(symbol)

    # Kite path (explicit or fallback)
    if EXECUTION_ENGINE in ("AUTO", "KITE", "KITEONLY"):
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exch,
            tradingsymbol=tsym,
            transaction_type=side,
            quantity=int(qty),
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET
        )
        return "KITE", ltp(symbol)

    # If user misconfigured EXECUTION_ENGINE
    raise RuntimeError(f"EXECUTION_ENGINE={EXECUTION_ENGINE} but STOCKO not configured and KITE not allowed")

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
last_flag_min = None
trade_allowed = False

# FUT symbol resolved at runtime start
now0 = datetime.now(MARKET_TZ)
FUT_SYMBOL = get_nearest_nifty_fut(now0)
if not FUT_SYMBOL:
    raise RuntimeError("Could not resolve nearest NIFTY FUT symbol from NFO instruments")

# =========================================================
# MAIN LOOP
# =========================================================

ensure_table()
print("🚀 LONG STRADDLE STARTED | MODE:", TRADE_MODE, "| ENGINE:", EXECUTION_ENGINE, "| FUT:", FUT_SYMBOL)

while True:
    try:
        now = datetime.now(MARKET_TZ)

        if not in_market_hours(now):
            time.sleep(5)
            continue

        # -------- FLAG CHECK (PER MINUTE) --------
        this_min = now.replace(second=0, microsecond=0)
        if last_flag_min is None or this_min > last_flag_min:
            last_flag_min = this_min
            trade_allowed = is_trade_allowed()

        spot = ltp(INDEX_SYMBOL)
        fut = ltp(FUT_SYMBOL)
        atr = atr_builder.update(now, fut)
        vix_prev, vix = get_vix()

        atm = round_to_strike(spot)
        dist = abs(spot - atm)

        # -------- KILL SWITCH --------
        if position_open and not trade_allowed:
            eng, ce_exit = place_leg(ce_symbol, "SELL", QTY_PER_LEG)
            _,   pe_exit = place_leg(pe_symbol, "SELL", QTY_PER_LEG)

            pnl = (ce_exit - ce_entry + pe_exit - pe_entry) * QTY_PER_LEG
            realized_pnl += pnl

            log_db(
                timestamp=now, status="EXIT", event="KILL_SWITCH_EXIT",
                reason="FLAG_DISABLED", spot=spot, atm=active_atm,
                ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                ce_entry=ce_entry, pe_entry=pe_entry,
                ce_exit=ce_exit, pe_exit=pe_exit,
                ce_ltp=ce_exit, pe_ltp=pe_exit,
                unreal_pnl=pnl, realized_pnl=realized_pnl,
                atr=atr, vix_prev=vix_prev, vix=vix,
                order_engine=eng
            )

            position_open = False
            active_atm = None
            ce_symbol = pe_symbol = None
            ce_entry = pe_entry = None
            last_snapshot_min = None
            time.sleep(TICK_INTERVAL)
            continue

        # -------- ENTRY --------
        if trade_allowed and (not position_open) and dist <= ENTRY_TOL:
            ce_symbol = get_nearest_option(atm, "CE", now)
            pe_symbol = get_nearest_option(atm, "PE", now)
            if not ce_symbol or not pe_symbol:
                raise RuntimeError(f"Could not resolve CE/PE symbols for ATM={atm}")

            eng, ce_entry = place_leg(ce_symbol, "BUY", QTY_PER_LEG)
            _,   pe_entry = place_leg(pe_symbol, "BUY", QTY_PER_LEG)

            position_open = True
            active_atm = atm

            log_db(
                timestamp=now, status="OPEN", event="ENTRY",
                reason="ATM_TOUCH", spot=spot, atm=atm,
                ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                ce_entry=ce_entry, pe_entry=pe_entry,
                ce_exit=None, pe_exit=None,
                ce_ltp=ce_entry, pe_ltp=pe_entry,
                unreal_pnl=0.0, realized_pnl=realized_pnl,
                atr=atr, vix_prev=vix_prev, vix=vix,
                order_engine=eng
            )

            last_snapshot_min = None  # reset snapshot minute on new entry

        # -------- POSITION MANAGEMENT: M2M + ROLL + FINAL EXIT --------
        if position_open:
            ce_ltp = ltp(ce_symbol)
            pe_ltp = ltp(pe_symbol)
            unreal = (ce_ltp - ce_entry + pe_ltp - pe_entry) * QTY_PER_LEG

            # ---- PER-MINUTE M2M SNAPSHOT ----
            snap_min = now.replace(second=0, microsecond=0)
            if last_snapshot_min is None or snap_min > last_snapshot_min:
                last_snapshot_min = snap_min
                log_db(
                    timestamp=now, status="M2M", event="SNAPSHOT",
                    reason="PER_MINUTE", spot=spot, atm=active_atm,
                    ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                    ce_entry=ce_entry, pe_entry=pe_entry,
                    ce_exit=None, pe_exit=None,
                    ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                    unreal_pnl=unreal, realized_pnl=realized_pnl,
                    atr=atr, vix_prev=vix_prev, vix=vix,
                    order_engine=EXECUTION_ENGINE
                )

            # ---- ATM ROLL: NEW 50PT STRIKE ± TOL ----
            new_atm = round_to_strike(spot)
            if new_atm != active_atm and abs(spot - new_atm) <= ENTRY_TOL and trade_allowed:
                # Exit old legs (ROLL_EXIT)
                eng, ce_exit = place_leg(ce_symbol, "SELL", QTY_PER_LEG)
                _,   pe_exit = place_leg(pe_symbol, "SELL", QTY_PER_LEG)

                roll_pnl = (ce_exit - ce_entry + pe_exit - pe_entry) * QTY_PER_LEG
                realized_pnl += roll_pnl

                log_db(
                    timestamp=now, status="EXIT", event="ROLL_EXIT",
                    reason=f"STRIKE_SHIFT {active_atm}->{new_atm}",
                    spot=spot, atm=active_atm,
                    ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                    ce_entry=ce_entry, pe_entry=pe_entry,
                    ce_exit=ce_exit, pe_exit=pe_exit,
                    ce_ltp=ce_exit, pe_ltp=pe_exit,
                    unreal_pnl=roll_pnl, realized_pnl=realized_pnl,
                    atr=atr, vix_prev=vix_prev, vix=vix,
                    order_engine=eng
                )

                # Enter new legs (ROLL_ENTRY)
                ce_symbol = get_nearest_option(new_atm, "CE", now)
                pe_symbol = get_nearest_option(new_atm, "PE", now)
                if not ce_symbol or not pe_symbol:
                    raise RuntimeError(f"Could not resolve CE/PE symbols for NEW_ATM={new_atm}")

                eng, ce_entry = place_leg(ce_symbol, "BUY", QTY_PER_LEG)
                _,   pe_entry = place_leg(pe_symbol, "BUY", QTY_PER_LEG)

                active_atm = new_atm
                last_snapshot_min = None  # reset so snapshot logs immediately on new legs

                log_db(
                    timestamp=now, status="OPEN", event="ROLL_ENTRY",
                    reason="ATM_ROLL", spot=spot, atm=new_atm,
                    ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                    ce_entry=ce_entry, pe_entry=pe_entry,
                    ce_exit=None, pe_exit=None,
                    ce_ltp=ce_entry, pe_ltp=pe_entry,
                    unreal_pnl=0.0, realized_pnl=realized_pnl,
                    atr=atr, vix_prev=vix_prev, vix=vix,
                    order_engine=eng
                )

                time.sleep(TICK_INTERVAL)
                continue

            # ---- FINAL EXIT ON TARGET / SL ----
            if unreal >= PROFIT_TARGET or unreal <= -STOP_LOSS:
                eng, ce_exit = place_leg(ce_symbol, "SELL", QTY_PER_LEG)
                _,   pe_exit = place_leg(pe_symbol, "SELL", QTY_PER_LEG)

                realized_pnl += unreal

                log_db(
                    timestamp=now, status="EXIT", event="FINAL_EXIT",
                    reason="TARGET" if unreal >= PROFIT_TARGET else "SL",
                    spot=spot, atm=active_atm,
                    ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                    ce_entry=ce_entry, pe_entry=pe_entry,
                    ce_exit=ce_exit, pe_exit=pe_exit,
                    ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                    unreal_pnl=unreal, realized_pnl=realized_pnl,
                    atr=atr, vix_prev=vix_prev, vix=vix,
                    order_engine=eng
                )

                position_open = False
                active_atm = None
                ce_symbol = pe_symbol = None
                ce_entry = pe_entry = None
                last_snapshot_min = None

        time.sleep(TICK_INTERVAL)

    except Exception as e:
        print("❌ ERROR:", e)
        time.sleep(5)
