#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, requests, pandas as pd, pytz, psycopg2
from datetime import datetime, time as dt_time
from kiteconnect import KiteConnect

# =========================================================
# CONFIG
# =========================================================

TRADE_MODE = os.getenv("TRADE_MODE", "PAPER")

STOCKO_BASE_URL     = "https://api.stocko.in"
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN")
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID")

API_KEY      = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# -------- NIFTY CONFIG --------
INDEX_SYMBOL = "NSE:NIFTY 50"
EXCH_OPT     = "NFO"
UNDERLYING   = "NIFTY"
STRIKE_STEP  = 50

ENTRY_TOL    = 4
QTY_PER_LEG  = 65

TICK_INTERVAL  = 1
WRITE_INTERVAL = 30

MARKET_TZ   = pytz.timezone("Asia/Kolkata")
TRADE_START = dt_time(9, 15)
TRADE_END   = dt_time(15, 30)

PROFIT_TARGET = 1400

USE_VIX_FILTER = False
VIX_SYMBOL     = "NSE:INDIA VIX"

TABLE_NAME      = "niftylong_strangle"
ROLL_TABLE_NAME = "nifty_long_strang_roll"

# =========================================================
# SAFETY CHECKS
# =========================================================

if TRADE_MODE == "LIVE":
    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE mode requires Stocko credentials")
else:
    print("[INFO] PAPER mode → Stocko disabled")

if not API_KEY or not ACCESS_TOKEN:
    raise RuntimeError("Missing Kite credentials")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

print(f"""
====================================
🚀 BOT STARTING
MODE        : {TRADE_MODE}
STOCKO      : {"ENABLED" if TRADE_MODE=="LIVE" else "DISABLED"}
====================================
""")

# =========================================================
# DB INIT (AUTO CREATE BOTH TABLES)
# =========================================================

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True

with conn.cursor() as cur:
    for table in (TABLE_NAME, ROLL_TABLE_NAME):
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                timestamp timestamptz,
                spot numeric,
                atm integer,
                ce_strike integer,
                pe_strike integer,
                ce_symbol text,
                pe_symbol text,
                ce_entry numeric,
                pe_entry numeric,
                ce_ltp numeric,
                pe_ltp numeric,
                straddle numeric,
                m2m numeric,
                status text,
                event text,
                reason text,
                vix numeric
            )
        """)

# =========================================================
# KITE INIT
# =========================================================

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# =========================================================
# UTILS
# =========================================================

def nowts():
    return datetime.now(MARKET_TZ)

def market_open():
    t = nowts().time()
    return TRADE_START <= t <= TRADE_END

def safe_quote(keys):
    while True:
        try:
            return kite.quote(keys)
        except Exception as e:
            print(f"[{nowts()}] quote error: {e}")
            time.sleep(1)

# =========================================================
# MARKET HELPERS
# =========================================================

def get_spot_and_atm():
    q = safe_quote([INDEX_SYMBOL])
    spot = q[INDEX_SYMBOL]["last_price"]
    atm  = round(spot / STRIKE_STEP) * STRIKE_STEP
    return spot, atm

def load_weekly_instruments():
    df = pd.DataFrame(kite.instruments(EXCH_OPT))
    df = df[df["name"] == UNDERLYING]
    df["expiry"] = pd.to_datetime(df["expiry"])
    expiry = df["expiry"].min()
    return df[df["expiry"] == expiry], expiry

def get_option_symbols(df, ce, pe):
    ce_sym = df[(df.strike == ce) & (df.instrument_type == "CE")].iloc[0].tradingsymbol
    pe_sym = df[(df.strike == pe) & (df.instrument_type == "PE")].iloc[0].tradingsymbol
    return ce_sym, pe_sym

def get_vix():
    return safe_quote([VIX_SYMBOL])[VIX_SYMBOL]["last_price"]

# =========================================================
# STOCKO (LIVE ONLY)
# =========================================================

def _headers():
    if TRADE_MODE != "LIVE":
        raise RuntimeError("Stocko called in PAPER mode")
    return {
        "Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

def stocko_search_token(symbol):
    if TRADE_MODE != "LIVE":
        raise RuntimeError("Stocko lookup in PAPER mode")

    r = requests.get(
        f"{STOCKO_BASE_URL}/api/v1/search",
        params={"key": symbol},
        headers=_headers(),
        timeout=10
    )
    r.raise_for_status()

    for rec in r.json().get("result", []):
        if rec.get("exchange") == EXCH_OPT:
            return rec["token"]

    raise ValueError("Token not found")

def place_order(symbol, side, qty, offset):
    if TRADE_MODE == "PAPER":
        print(f"[{nowts()}] 🧪 PAPER {side} {symbol}")
        return

    token = stocko_search_token(symbol)

    payload = {
        "exchange": EXCH_OPT,
        "order_type": "MARKET",
        "instrument_token": int(token),
        "quantity": qty,
        "order_side": side,
        "product": "NRML",
        "validity": "DAY",
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": str(int(time.time()*1000)+offset)[-15:]
    }

    requests.post(
        f"{STOCKO_BASE_URL}/api/v1/orders",
        json=payload,
        headers=_headers(),
        timeout=10
    )

# =========================================================
# DB LOGGING (WRITE TO BOTH TABLES)
# =========================================================

def log_row(**r):
    cols = ",".join(r.keys())
    vals = tuple(r.values())
    placeholders = ",".join(["%s"] * len(vals))

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders})",
            vals
        )
        cur.execute(
            f"INSERT INTO {ROLL_TABLE_NAME} ({cols}) VALUES ({placeholders})",
            vals
        )

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    df_expiry, expiry = load_weekly_instruments()
    print(f"[{nowts()}] WEEKLY EXPIRY → {expiry.date()}")

    mode = "IDLE"
    current_atm = None
    last_write = 0

    while True:
        try:
            if not market_open():
                time.sleep(5)
                continue

            spot, atm = get_spot_and_atm()
            diff = abs(spot - atm)

            # ================= ENTRY =================
            if mode in ("IDLE", "WAITING_REENTRY") and diff <= ENTRY_TOL:

                ce_sym, pe_sym = get_option_symbols(df_expiry, atm, atm)
                q = safe_quote([f"{EXCH_OPT}:{ce_sym}", f"{EXCH_OPT}:{pe_sym}"])

                ce_entry = q[f"{EXCH_OPT}:{ce_sym}"]["last_price"]
                pe_entry = q[f"{EXCH_OPT}:{pe_sym}"]["last_price"]

                place_order(ce_sym, "BUY", QTY_PER_LEG, 0)
                place_order(pe_sym, "BUY", QTY_PER_LEG, 1)

                current_atm = atm
                mode = "OPEN"

                log_row(
                    timestamp=nowts(),
                    spot=spot,
                    atm=atm,
                    ce_strike=atm,
                    pe_strike=atm,
                    ce_symbol=ce_sym,
                    pe_symbol=pe_sym,
                    ce_entry=ce_entry,
                    pe_entry=pe_entry,
                    ce_ltp=ce_entry,
                    pe_ltp=pe_entry,
                    straddle=ce_entry + pe_entry,
                    m2m=0,
                    status="OPEN",
                    event="ENTRY",
                    reason="ATM_TOUCH",
                    vix=get_vix() if USE_VIX_FILTER else None
                )

            # ================= OPEN =================
            if mode == "OPEN":

                q = safe_quote([f"{EXCH_OPT}:{ce_sym}", f"{EXCH_OPT}:{pe_sym}"])
                ce_ltp = q[f"{EXCH_OPT}:{ce_sym}"]["last_price"]
                pe_ltp = q[f"{EXCH_OPT}:{pe_sym}"]["last_price"]

                m2m = ((ce_ltp - ce_entry) + (pe_ltp - pe_entry)) * QTY_PER_LEG

                if m2m >= PROFIT_TARGET:
                    place_order(ce_sym, "SELL", QTY_PER_LEG, 2)
                    place_order(pe_sym, "SELL", QTY_PER_LEG, 3)
                    mode = "WAITING_REENTRY"

                if time.time() - last_write >= WRITE_INTERVAL:
                    log_row(
                        timestamp=nowts(),
                        spot=spot,
                        atm=current_atm,
                        ce_strike=current_atm,
                        pe_strike=current_atm,
                        ce_symbol=ce_sym,
                        pe_symbol=pe_sym,
                        ce_entry=ce_entry,
                        pe_entry=pe_entry,
                        ce_ltp=ce_ltp,
                        pe_ltp=pe_ltp,
                        straddle=ce_ltp + pe_ltp,
                        m2m=m2m,
                        status="OPEN",
                        event="SNAPSHOT",
                        reason="",
                        vix=get_vix() if USE_VIX_FILTER else None
                    )
                    last_write = time.time()

            time.sleep(TICK_INTERVAL)

        except Exception as e:
            print(f"[{nowts()}] ERROR → {e}")
            time.sleep(2)
