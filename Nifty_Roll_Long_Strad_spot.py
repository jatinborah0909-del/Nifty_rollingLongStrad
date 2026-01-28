#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – SAFE ATM ROLLING (TOLERANCE BASED)
-------------------------------------------------------
✔ Entry only when spot touches ATM ± ENTRY_TOL
✔ Roll only when spot touches a NEW 50pt strike ± ENTRY_TOL
✔ No midpoint flip-flop
✔ No multiple rolls on same strike
✔ FUT-based ATR
✔ VIX logging
✔ Railway compatible
✔ Logs ONLY to nifty_long_strang_roll
"""

import os, time, math, pytz
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

# =========================================================
# VALIDATION
# =========================================================

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("Missing Kite credentials")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

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

    def update(self, now, ltp):
        mk = now.replace(second=0, microsecond=0)

        if self.last_min is None:
            self.last_min = mk
            self.h = self.l = self.c = self.prev_c = ltp
            return None

        if mk == self.last_min:
            self.h = max(self.h, ltp)
            self.l = min(self.l, ltp)
            self.c = ltp
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
        self.h = self.l = self.c = ltp
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
    # NO banker rounding
    return int(math.floor(price / STRIKE_STEP + 0.5) * STRIKE_STEP)

# =========================================================
# STATE
# =========================================================

position_open = False
active_atm = None
ce_symbol = pe_symbol = None
ce_entry = pe_entry = None
realized_pnl = 0.0

atr_builder = FutAtrBuilder(ATR_PERIOD)

# =========================================================
# MAIN LOOP
# =========================================================

ensure_table()
print("🚀 SAFE ATM ROLLING STRADDLE STARTED")

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

            ce_entry = ltp(ce_symbol)
            pe_entry = ltp(pe_symbol)
            active_atm = atm
            position_open = True

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
                ce_ltp=ce_entry,
                pe_ltp=pe_entry,
                unreal_pnl=0,
                realized_pnl=realized_pnl,
                atr=atr,
                vix_prev=vix_prev,
                vix=vix
            )

        # ================= SAFE ROLL =================
        if (
            position_open
            and atm != active_atm
            and dist <= ENTRY_TOL
        ):
            old_ce_ltp = ltp(ce_symbol)
            old_pe_ltp = ltp(pe_symbol)

            roll_pnl = (old_ce_ltp - ce_entry + old_pe_ltp - pe_entry) * QTY_PER_LEG
            realized_pnl += roll_pnl

            ce_symbol = get_nearest_option(atm, "CE")
            pe_symbol = get_nearest_option(atm, "PE")
            ce_entry = ltp(ce_symbol)
            pe_entry = ltp(pe_symbol)
            active_atm = atm

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
                ce_ltp=ce_entry,
                pe_ltp=pe_entry,
                unreal_pnl=0,
                realized_pnl=realized_pnl,
                atr=atr,
                vix_prev=vix_prev,
                vix=vix
            )

        # ================= EXIT =================
        if position_open:
            ce_ltp = ltp(ce_symbol)
            pe_ltp = ltp(pe_symbol)
            unreal = (ce_ltp - ce_entry + pe_ltp - pe_entry) * QTY_PER_LEG

            if unreal >= PROFIT_TARGET or unreal <= -STOP_LOSS:
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

        time.sleep(TICK_INTERVAL)

    except Exception as e:
        print("❌ ERROR:", e)
        time.sleep(5)
