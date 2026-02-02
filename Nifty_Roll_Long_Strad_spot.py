#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – SPOT – FINAL (SCHEMA SAFE)
==============================================
✔ BUY ATM CE + BUY ATM PE
✔ SNAPSHOT every minute
✔ Table: nifty_long_strang_roll
✔ timestamp column: timestamp
✔ Kill-switch: trade_flag.live_ls_nifty_spot
✔ AUTO-FIXES wrong NUMERIC text columns
✔ PAPER + LIVE safe
"""

import os, time, math, pytz, requests
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

LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() in ("1", "true", "yes")

TABLE_NAME = "nifty_long_strang_roll"
FLAG_TABLE = "trade_flag"
FLAG_COL   = "live_ls_nifty_spot"

SPOT_INSTRUMENT = "NSE:NIFTY 50"

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

def ensure_table(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            bot_name TEXT,
            event TEXT,
            reason TEXT,
            symbol TEXT,
            side TEXT,
            qty INTEGER,
            price NUMERIC,
            spot NUMERIC,
            atr NUMERIC,
            unreal_pnl NUMERIC,
            total_pnl NUMERIC,
            ce_entry_price NUMERIC,
            pe_entry_price NUMERIC,
            ce_ltp NUMERIC,
            pe_ltp NUMERIC,
            ce_exit_price NUMERIC,
            pe_exit_price NUMERIC
        )
        """).format(t=sql.Identifier(TABLE_NAME)))

        typed_columns = {
            "bot_name": "TEXT",
            "event": "TEXT",
            "reason": "TEXT",
            "symbol": "TEXT",
            "side": "TEXT",
            "qty": "INTEGER",
            "price": "NUMERIC",
            "spot": "NUMERIC",
            "atr": "NUMERIC",
            "unreal_pnl": "NUMERIC",
            "total_pnl": "NUMERIC",
            "ce_entry_price": "NUMERIC",
            "pe_entry_price": "NUMERIC",
            "ce_ltp": "NUMERIC",
            "pe_ltp": "NUMERIC",
            "ce_exit_price": "NUMERIC",
            "pe_exit_price": "NUMERIC",
        }

        for col, typ in typed_columns.items():
            c.execute(sql.SQL("""
                ALTER TABLE {t}
                ADD COLUMN IF NOT EXISTS {c} {typ}
            """).format(
                t=sql.Identifier(TABLE_NAME),
                c=sql.Identifier(col),
                typ=sql.SQL(typ)
            ))

    conn.commit()

def force_text_columns(conn):
    """FIX for legacy schema where text columns were wrongly NUMERIC"""
    with conn.cursor() as c:
        for col in ["bot_name", "event", "reason", "symbol", "side"]:
            c.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = %s
                      AND column_name = %s
                      AND data_type <> 'text'
                ) THEN
                    EXECUTE format(
                        'ALTER TABLE %I ALTER COLUMN %I TYPE TEXT USING %I::TEXT',
                        %s, %s, %s
                    );
                END IF;
            END $$;
            """, (TABLE_NAME, col, TABLE_NAME, col, col))
    conn.commit()

def log_db(conn, **k):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        INSERT INTO {t} (
            timestamp, bot_name, event, reason, symbol, side,
            qty, price, spot, atr, unreal_pnl, total_pnl,
            ce_entry_price, pe_entry_price,
            ce_ltp, pe_ltp,
            ce_exit_price, pe_exit_price
        )
        VALUES (
            NOW(), %(bot)s, %(event)s, %(reason)s, %(symbol)s, %(side)s,
            %(qty)s, %(price)s, %(spot)s, %(atr)s, %(unreal)s, %(total)s,
            %(ce_entry)s, %(pe_entry)s,
            %(ce_ltp)s, %(pe_ltp)s,
            %(ce_exit)s, %(pe_exit)s
        )
        """).format(t=sql.Identifier(TABLE_NAME)), k)
    conn.commit()

def trade_allowed(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL(
            "SELECT {c} FROM {t} LIMIT 1"
        ).format(
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
    try:
        q = kite.ltp(insts)
        return {k: v["last_price"] for k, v in q.items()}
    except:
        return {}

# =========================================================
# STOCKO
# =========================================================

def stocko(symbol, side, qty):
    if not LIVE_MODE:
        return
    requests.post(
        f"{STOCKO_BASE_URL}/api/v1/orders",
        json={
            "exchange": "NFO",
            "order_type": "MARKET",
            "tradingsymbol": symbol,
            "order_side": side,
            "quantity": qty,
            "product": "NRML",
            "client_id": STOCKO_CLIENT_ID
        },
        headers={"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}"},
        timeout=10
    )

# =========================================================
# MAIN
# =========================================================

def main():
    conn = db_conn()
    ensure_table(conn)
    force_text_columns(conn)   # 🔥 THE FIX

    nfo = pd.DataFrame(kite.instruments("NFO"))
    nfo = nfo[nfo["name"] == "NIFTY"]

    pos = {}
    ce_ts = pe_ts = None
    last_snap = 0

    while True:
        now = datetime.now(MARKET_TZ)

        if now.time() < MARKET_OPEN or now.time() > MARKET_CLOSE:
            time.sleep(30)
            continue

        spot = ltp([SPOT_INSTRUMENT]).get(SPOT_INSTRUMENT)
        if not spot:
            time.sleep(POLL_SEC)
            continue

        atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)

        # ---- SNAPSHOT ----
        if time.time() - last_snap >= SNAPSHOT_SEC:
            allowed = trade_allowed(conn)
            unreal = 0.0

            if pos:
                l = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                unreal = (
                    (l[f"NFO:{ce_ts}"] - pos["CE"]["entry"]) +
                    (l[f"NFO:{pe_ts}"] - pos["PE"]["entry"])
                ) * QTY

            log_db(
                conn,
                bot=BOT_NAME,
                event="SNAPSHOT",
                reason=f"FLAG={allowed}",
                symbol="NIFTY",
                side="NA",
                qty=0,
                price=0,
                spot=spot,
                atr=None,
                unreal=unreal,
                total=unreal,
                ce_entry=pos.get("CE", {}).get("entry"),
                pe_entry=pos.get("PE", {}).get("entry"),
                ce_ltp=None,
                pe_ltp=None,
                ce_exit=None,
                pe_exit=None
            )

            if not allowed and pos:
                stocko(ce_ts, "SELL", QTY)
                stocko(pe_ts, "SELL", QTY)
                pos.clear()

            last_snap = time.time()

        # ---- TIME EXIT ----
        if now.time() >= SQUARE_OFF and pos:
            stocko(ce_ts, "SELL", QTY)
            stocko(pe_ts, "SELL", QTY)
            break

        # ---- ENTRY ----
        if not pos and abs(spot - atm) <= ENTRY_TOL and trade_allowed(conn):
            opt = nfo[
                (nfo["strike"] == atm) &
                (nfo["instrument_type"].isin(["CE", "PE"]))
            ]

            expiry = min(pd.to_datetime(opt["expiry"]).dt.date)

            ce_ts = opt[
                (opt["instrument_type"] == "CE") &
                (pd.to_datetime(opt["expiry"]).dt.date == expiry)
            ].iloc[0]["tradingsymbol"]

            pe_ts = opt[
                (opt["instrument_type"] == "PE") &
                (pd.to_datetime(opt["expiry"]).dt.date == expiry)
            ].iloc[0]["tradingsymbol"]

            l = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])

            stocko(ce_ts, "BUY", QTY)
            stocko(pe_ts, "BUY", QTY)

            pos["CE"] = {"entry": l[f"NFO:{ce_ts}"], "strike": atm}
            pos["PE"] = {"entry": l[f"NFO:{pe_ts}"], "strike": atm}

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
