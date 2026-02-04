#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – SPOT (LIVE SAFE, STOCKO FIXED + KILL-SWITCH EXIT)
======================================================================
✔ BUY ATM CE + BUY ATM PE
✔ SNAPSHOT every minute
✔ Kill-switch: trade_flag.live_ls_nifty_spot
    - If FALSE while position OPEN -> immediate square-off + halt
    - Resume when TRUE again (and no positions)
✔ PAPER + LIVE safe
✔ NO fake PnL (pos created only if Stocko succeeds)
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
QTY         = int(os.getenv("QTY_PER_LEG", 65))

SNAPSHOT_SEC = 60
POLL_SEC = 1

LIVE_MODE = os.getenv("LIVE_MODE", "false").strip().lower() in ("1", "true", "yes")

TABLE_NAME = "nifty_long_strang_roll"
FLAG_TABLE = "trade_flag"
FLAG_COL   = "live_ls_nifty_spot"

SPOT_INSTRUMENT = "NSE:NIFTY 50"

KITE_API_KEY = os.getenv("KITE_API_KEY", "").strip()
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

STOCKO_BASE_URL = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID = os.getenv("STOCKO_CLIENT_ID", "").strip()

# =========================================================
# DB
# =========================================================

def db_conn():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require",
        cursor_factory=RealDictCursor
    )

def create_table(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ,
            bot_name TEXT,
            event TEXT,
            reason TEXT,
            spot NUMERIC,
            unreal_pnl NUMERIC,
            ce_entry NUMERIC,
            pe_entry NUMERIC
        );
        """).format(t=sql.Identifier(TABLE_NAME)))
    conn.commit()

def log_db(conn, **k):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        INSERT INTO {t}
        (ts, bot_name, event, reason, spot, unreal_pnl, ce_entry, pe_entry)
        VALUES (NOW(), %(bot)s, %(event)s, %(reason)s, %(spot)s, %(unreal)s, %(ce)s, %(pe)s)
        """).format(t=sql.Identifier(TABLE_NAME)), k)
    conn.commit()

def trade_allowed(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("SELECT {c} FROM {t} LIMIT 1").format(
            c=sql.Identifier(FLAG_COL),
            t=sql.Identifier(FLAG_TABLE)
        ))
        r = c.fetchone()
        return bool(r[FLAG_COL]) if r else False

# =========================================================
# KITE
# =========================================================

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

def ltp(insts):
    q = kite.ltp(insts)
    return {k: v["last_price"] for k, v in q.items()}

# =========================================================
# STOCKO (WORKING LOGIC)
# =========================================================

def _stocko_headers():
    return {"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}"}

def stocko_place_by_tradingsymbol(tradingsymbol: str, side: str, qty: int, offset=0):
    """
    Places MARKET order in LIVE_MODE using Stocko:
      - search token via /search?key=<tsym>
      - place order via instrument_token with required fields
    """
    if not LIVE_MODE:
        return {"simulated": True}

    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE_MODE=True but STOCKO creds missing.")

    # search token
    r = requests.get(
        f"{STOCKO_BASE_URL}/api/v1/search",
        params={"key": tradingsymbol},
        headers=_stocko_headers(),
        timeout=10
    )
    r.raise_for_status()
    data = r.json()
    result = data.get("result") or data.get("data", {}).get("result", [])

    token = None
    for rec in result:
        if rec.get("exchange") == "NFO":
            token = rec.get("token")
            break
    if token is None:
        raise RuntimeError(f"Stocko token not found for {tradingsymbol}")

    payload = {
        "exchange": "NFO",
        "order_type": "MARKET",
        "instrument_token": int(token),
        "quantity": int(qty),
        "disclosed_quantity": 0,
        "order_side": side.upper(),
        "price": 0,
        "trigger_price": 0,
        "validity": "DAY",
        "product": "MIS",
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": str(int(time.time() * 1000) + offset)[-15:],
        "market_protection_percentage": 0,
        "device": "WEB"
    }

    r = requests.post(
        f"{STOCKO_BASE_URL}/api/v1/orders",
        json=payload,
        headers=_stocko_headers(),
        timeout=10
    )
    print("🧾 STOCKO", side.upper(), tradingsymbol, r.status_code, r.text)
    if r.status_code != 200:
        raise RuntimeError(r.text)

    return r.json()

# =========================================================
# MAIN
# =========================================================

def main():
    conn = db_conn()
    create_table(conn)

    nfo = pd.DataFrame(kite.instruments("NFO"))
    nfo = nfo[nfo["name"] == "NIFTY"]

    pos = {}                 # {"CE": entry_price, "PE": entry_price}
    ce_ts = pe_ts = None
    last_snap = 0

    halted = False           # halt entries after FLAG_FALSE until flag becomes True

    print(f"🚀 STARTED | LIVE_MODE={LIVE_MODE}")

    while True:
        now = datetime.now(MARKET_TZ)

        # session guard
        if now.time() < MARKET_OPEN or now.time() > MARKET_CLOSE:
            time.sleep(30)
            continue

        spot = ltp([SPOT_INSTRUMENT]).get(SPOT_INSTRUMENT)
        if not spot:
            time.sleep(POLL_SEC)
            continue

        atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)

        # ---------- SNAPSHOT + FLAG ENFORCEMENT ----------
        if time.time() - last_snap >= SNAPSHOT_SEC:
            allowed = trade_allowed(conn)

            unreal = 0.0
            if pos and ce_ts and pe_ts:
                l = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                unreal = (
                    (l[f"NFO:{ce_ts}"] - pos["CE"]) +
                    (l[f"NFO:{pe_ts}"] - pos["PE"])
                ) * QTY

            log_db(
                conn,
                bot=BOT_NAME,
                event="SNAPSHOT",
                reason=f"FLAG={allowed},HALT={halted}",
                spot=spot,
                unreal=unreal,
                ce=pos.get("CE"),
                pe=pos.get("PE"),
            )

            # ✅ Kill-switch enforcement: if FALSE and position exists -> square off now
            if (not allowed) and pos and ce_ts and pe_ts:
                try:
                    stocko_place_by_tradingsymbol(ce_ts, "SELL", QTY, 901)
                    stocko_place_by_tradingsymbol(pe_ts, "SELL", QTY, 902)
                    print("🛑 FLAG FALSE -> SQUARED OFF")
                except Exception as e:
                    print("❌ FLAG EXIT FAILED:", e)

                # log exit event (best-effort)
                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="EXIT",
                    reason="FLAG_FALSE_SQUAREOFF",
                    spot=spot,
                    unreal=unreal,
                    ce=pos.get("CE"),
                    pe=pos.get("PE"),
                )

                pos.clear()
                ce_ts = pe_ts = None
                halted = True  # don’t re-enter until flag becomes True again

            # resume when flag becomes true again
            if allowed and halted and not pos:
                halted = False
                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="RESUME",
                    reason="FLAG_TRUE_RESUME",
                    spot=spot,
                    unreal=0.0,
                    ce=None,
                    pe=None,
                )

            last_snap = time.time()

        # ---------- TIME EXIT ----------
        if now.time() >= SQUARE_OFF and pos and ce_ts and pe_ts:
            stocko_place_by_tradingsymbol(ce_ts, "SELL", QTY, 101)
            stocko_place_by_tradingsymbol(pe_ts, "SELL", QTY, 102)
            print("⏹ 15:25 EXIT")
            break

        # ---------- ENTRY ----------
        if halted:
            time.sleep(POLL_SEC)
            continue

        if (not pos) and (now.time() >= ENTRY_START):
            allowed_now = trade_allowed(conn)
            if allowed_now and abs(spot - atm) <= ENTRY_TOL:
                opt = nfo[(nfo["strike"] == atm) & (nfo["instrument_type"].isin(["CE", "PE"]))]
                if opt.empty:
                    time.sleep(POLL_SEC)
                    continue

                expiry = min(pd.to_datetime(opt["expiry"]).dt.date)

                ce_ts = opt[(opt["instrument_type"] == "CE") & (pd.to_datetime(opt["expiry"]).dt.date == expiry)].iloc[0]["tradingsymbol"]
                pe_ts = opt[(opt["instrument_type"] == "PE") & (pd.to_datetime(opt["expiry"]).dt.date == expiry)].iloc[0]["tradingsymbol"]

                l = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                ce_p, pe_p = l[f"NFO:{ce_ts}"], l[f"NFO:{pe_ts}"]

                try:
                    stocko_place_by_tradingsymbol(ce_ts, "BUY", QTY, 1)
                    stocko_place_by_tradingsymbol(pe_ts, "BUY", QTY, 2)
                except Exception as e:
                    print("❌ ENTRY FAILED:", e)
                    ce_ts = pe_ts = None
                    time.sleep(POLL_SEC)
                    continue

                pos["CE"] = ce_p
                pos["PE"] = pe_p
                print("✅ LIVE ENTRY EXECUTED")

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
