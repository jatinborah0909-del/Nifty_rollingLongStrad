#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – SPOT (LIVE SAFE, STOCKO FIXED)
===================================================
✔ BUY ATM CE + BUY ATM PE
✔ SNAPSHOT every minute
✔ Kill-switch: trade_flag.live_ls_nifty_spot
✔ PAPER + LIVE safe
✔ India VIX logged
✔ Stocko instrument_token based execution (FIXED)
✔ No fake PnL (pos only if LIVE orders succeed)
"""

import os, time, pytz, requests
from datetime import datetime, time as dt_time
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from kiteconnect import KiteConnect

# =========================================================
# CONFIG
# =========================================================

BOT_NAME = "LS_NIFTY_SPOT"
MARKET_TZ = pytz.timezone("Asia/Kolkata")

MARKET_OPEN  = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
ENTRY_START  = dt_time(9, 30)
SQUARE_OFF   = dt_time(15, 25)

STRIKE_STEP = int(os.getenv("STRIKE_STEP", 50))
ENTRY_TOL   = int(os.getenv("ENTRY_TOL", 25))
QTY         = int(os.getenv("QTY_PER_LEG", 50))

SNAPSHOT_SEC = 60
POLL_SEC = 1

LIVE_MODE = os.getenv("LIVE_MODE", "false").strip().lower() in ("1", "true", "yes")

TABLE_NAME = "nifty_long_strang_roll"
FLAG_TABLE = "trade_flag"
FLAG_COL   = "live_ls_nifty_spot"

SPOT_INSTRUMENT = "NSE:NIFTY 50"
VIX_INSTRUMENT  = "NSE:INDIA VIX"

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

STOCKO_BASE_URL = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in")
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN")
STOCKO_CLIENT_ID = os.getenv("STOCKO_CLIENT_ID")

# =========================================================
# DB
# =========================================================

def db_conn():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require",
        cursor_factory=RealDictCursor
    )

def create_table_fresh(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            bot_name TEXT,
            event TEXT,
            reason TEXT,
            spot NUMERIC,
            vix_prev NUMERIC,
            vix NUMERIC,
            unreal_pnl NUMERIC,
            total_pnl NUMERIC
        );
        """).format(t=sql.Identifier(TABLE_NAME)))
    conn.commit()

def log_db(conn, **k):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        INSERT INTO {t} (
            timestamp, bot_name, event, reason,
            spot, vix_prev, vix,
            unreal_pnl, total_pnl
        )
        VALUES (
            NOW(), %(bot)s, %(event)s, %(reason)s,
            %(spot)s, %(vix_prev)s, %(vix)s,
            %(unreal)s, %(total)s
        )
        """).format(t=sql.Identifier(TABLE_NAME)), k)
    conn.commit()

def trade_allowed(conn):
    with conn.cursor() as c:
        c.execute(
            sql.SQL("SELECT {c} FROM {t} LIMIT 1").format(
                c=sql.Identifier(FLAG_COL),
                t=sql.Identifier(FLAG_TABLE)
            )
        )
        r = c.fetchone()
        return bool(r[FLAG_COL]) if r else False

# =========================================================
# KITE
# =========================================================

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

def ltp(insts):
    try:
        q = kite.ltp(insts)
        return {k: v["last_price"] for k, v in q.items()}
    except Exception:
        return {}

def get_vix():
    try:
        q = kite.quote([VIX_INSTRUMENT])
        d = q[VIX_INSTRUMENT]
        return float(d["ohlc"]["close"]), float(d["last_price"])
    except Exception:
        return None, None

# =========================================================
# STOCKO (FIXED)
# =========================================================

def stocko_token(symbol):
    r = requests.get(
        f"{STOCKO_BASE_URL}/api/v1/search",
        params={"query": symbol, "exchange": "NFO"},
        headers={"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}"},
        timeout=10
    )
    if r.status_code != 200:
        return None
    data = r.json().get("data", [])
    return data[0]["instrument_token"] if data else None

def stocko_order(symbol, side, qty):
    if not LIVE_MODE:
        return True

    token = stocko_token(symbol)
    if not token:
        print("❌ STOCKO TOKEN FAIL:", symbol)
        return False

    payload = {
        "exchange": "NFO",
        "instrument_token": token,
        "order_type": "MARKET",
        "order_side": side,
        "quantity": qty,
        "product": "MIS",
        "client_id": STOCKO_CLIENT_ID
    }

    r = requests.post(
        f"{STOCKO_BASE_URL}/api/v1/orders",
        json=payload,
        headers={"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}"},
        timeout=10
    )

    print("🧾 STOCKO", side, symbol, r.status_code, r.text)
    return r.status_code == 200

# =========================================================
# MAIN
# =========================================================

def main():
    conn = db_conn()
    create_table_fresh(conn)

    nfo = pd.DataFrame(kite.instruments("NFO"))
    nfo = nfo[nfo["name"] == "NIFTY"]

    pos = {}
    ce_ts = pe_ts = None
    last_snap = 0

    print(f"🚀 STARTED | LIVE_MODE={LIVE_MODE}")

    while True:
        now = datetime.now(MARKET_TZ)

        if now.time() < MARKET_OPEN or now.time() > MARKET_CLOSE:
            time.sleep(30)
            continue

        spot = ltp([SPOT_INSTRUMENT]).get(SPOT_INSTRUMENT)
        if not spot:
            time.sleep(POLL_SEC)
            continue

        vix_prev, vix = get_vix()
        atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)

        # ---------- SNAPSHOT ----------
        if time.time() - last_snap >= SNAPSHOT_SEC:
            unreal = 0.0
            if pos:
                l = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                unreal = (
                    (l[f"NFO:{ce_ts}"] - pos["CE"]) +
                    (l[f"NFO:{pe_ts}"] - pos["PE"])
                ) * QTY

            log_db(
                conn,
                bot=BOT_NAME,
                event="SNAPSHOT",
                reason=f"FLAG={trade_allowed(conn)}",
                spot=spot,
                vix_prev=vix_prev,
                vix=vix,
                unreal=unreal,
                total=unreal
            )
            last_snap = time.time()

        # ---------- EXIT ----------
        if now.time() >= SQUARE_OFF and pos:
            stocko_order(ce_ts, "SELL", QTY)
            stocko_order(pe_ts, "SELL", QTY)
            break

        # ---------- ENTRY ----------
        if not pos and trade_allowed(conn) and abs(spot - atm) <= ENTRY_TOL:
            opt = nfo[
                (nfo["strike"] == atm) &
                (nfo["instrument_type"].isin(["CE", "PE"]))
            ]

            expiry = min(pd.to_datetime(opt["expiry"]).dt.date)
            ce_ts = opt[(opt["instrument_type"] == "CE") &
                        (pd.to_datetime(opt["expiry"]).dt.date == expiry)].iloc[0]["tradingsymbol"]
            pe_ts = opt[(opt["instrument_type"] == "PE") &
                        (pd.to_datetime(opt["expiry"]).dt.date == expiry)].iloc[0]["tradingsymbol"]

            l = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])

            ok1 = stocko_order(ce_ts, "BUY", QTY)
            ok2 = stocko_order(pe_ts, "BUY", QTY)

            if ok1 and ok2:
                pos["CE"] = l[f"NFO:{ce_ts}"]
                pos["PE"] = l[f"NFO:{pe_ts}"]
                print("✅ LIVE ENTRY CONFIRMED")
            else:
                print("🚫 ENTRY ABORTED (STOCKO FAIL)")

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
