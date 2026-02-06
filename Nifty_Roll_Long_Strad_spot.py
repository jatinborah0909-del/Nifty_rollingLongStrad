#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – SPOT (LIVE SAFE, STOCKO + KILL-SWITCH + ATM ROLLING)
==========================================================================
✔ BUY ATM CE + BUY ATM PE
✔ SNAPSHOT every minute
✔ Kill-switch: trade_flag.live_ls_nifty_spot
    - If FALSE while position OPEN -> immediate square-off + halt
    - Resume when TRUE again (and no positions)
✔ PAPER + LIVE safe
✔ NO fake PnL (pos created only if Stocko succeeds)

🆕 FIXED / ADDED (as requested)
------------------------------
✅ Proper ATM rolling:
   - Track active_atm once position is open
   - If spot moves to next 50pt strike (atm changes) AND spot is within ±ENTRY_TOL of new atm:
        -> exit old legs (capture exit LTP)
        -> enter new ATM legs (capture entry LTP)
✅ Store CE/PE exit prices in DB for:
   - ROLL_EXIT
   - FINAL EXIT (15:25)
   - FLAG_FALSE_SQUAREOFF
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
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL env var not found")
    return psycopg2.connect(url, sslmode="require", cursor_factory=RealDictCursor)

def ensure_schema(conn):
    """
    Creates table if missing, and adds required columns if table already exists.
    """
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ,
            bot_name TEXT,
            event TEXT,
            reason TEXT,
            spot NUMERIC,
            atm INTEGER,
            ce_symbol TEXT,
            pe_symbol TEXT,
            unreal_pnl NUMERIC,
            ce_entry NUMERIC,
            pe_entry NUMERIC,
            ce_exit NUMERIC,
            pe_exit NUMERIC
        );
        """).format(t=sql.Identifier(TABLE_NAME)))
    conn.commit()

    # light auto-migration (safe if columns already exist)
    cols = [
        ("atm", "INTEGER"),
        ("ce_symbol", "TEXT"),
        ("pe_symbol", "TEXT"),
        ("ce_exit", "NUMERIC"),
        ("pe_exit", "NUMERIC"),
    ]
    with conn.cursor() as c:
        for col, typ in cols:
            c.execute(sql.SQL("ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c} " + typ).format(
                t=sql.Identifier(TABLE_NAME),
                c=sql.Identifier(col)
            ))
    conn.commit()

def log_db(conn, **k):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        INSERT INTO {t}
        (ts, bot_name, event, reason, spot, atm, ce_symbol, pe_symbol, unreal_pnl, ce_entry, pe_entry, ce_exit, pe_exit)
        VALUES
        (NOW(), %(bot)s, %(event)s, %(reason)s, %(spot)s, %(atm)s, %(ce_sym)s, %(pe_sym)s,
         %(unreal)s, %(ce_entry)s, %(pe_entry)s, %(ce_exit)s, %(pe_exit)s)
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
# STOCKO
# =========================================================

def _stocko_headers():
    return {"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}"}

def stocko_place_by_tradingsymbol(tradingsymbol: str, side: str, qty: int, offset=0):
    """
    MARKET order. In PAPER mode returns simulated success.
    """
    if not LIVE_MODE:
        return {"simulated": True}

    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE_MODE=True but STOCKO creds missing.")

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
# OPTION PICKER
# =========================================================

def pick_atm_symbols(nfo_df: pd.DataFrame, atm: int):
    """
    Picks nearest expiry CE/PE for a given strike from NFO instruments snapshot.
    """
    opt = nfo_df[(nfo_df["strike"] == atm) & (nfo_df["instrument_type"].isin(["CE", "PE"]))]
    if opt.empty:
        return None, None

    expiry = min(pd.to_datetime(opt["expiry"]).dt.date)

    ce = opt[(opt["instrument_type"] == "CE") & (pd.to_datetime(opt["expiry"]).dt.date == expiry)].iloc[0]["tradingsymbol"]
    pe = opt[(opt["instrument_type"] == "PE") & (pd.to_datetime(opt["expiry"]).dt.date == expiry)].iloc[0]["tradingsymbol"]
    return ce, pe

# =========================================================
# MAIN
# =========================================================

def main():
    conn = db_conn()
    ensure_schema(conn)

    nfo = pd.DataFrame(kite.instruments("NFO"))
    nfo = nfo[nfo["name"] == "NIFTY"].copy()

    # Position state
    pos = {}                 # {"CE": entry_price, "PE": entry_price}
    ce_ts = pe_ts = None
    active_atm = None

    last_snap = 0
    halted = False

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
                atm=active_atm if active_atm is not None else atm,
                ce_sym=ce_ts,
                pe_sym=pe_ts,
                unreal=unreal,
                ce_entry=pos.get("CE"),
                pe_entry=pos.get("PE"),
                ce_exit=None,
                pe_exit=None,
            )

            # Kill-switch enforcement: if FALSE and position exists -> square off now + log exit prices
            if (not allowed) and pos and ce_ts and pe_ts:
                ce_exit = pe_exit = None
                try:
                    prices = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                    ce_exit = prices.get(f"NFO:{ce_ts}")
                    pe_exit = prices.get(f"NFO:{pe_ts}")
                except Exception:
                    pass

                try:
                    stocko_place_by_tradingsymbol(ce_ts, "SELL", QTY, 901)
                    stocko_place_by_tradingsymbol(pe_ts, "SELL", QTY, 902)
                    print("🛑 FLAG FALSE -> SQUARED OFF")
                except Exception as e:
                    print("❌ FLAG EXIT FAILED:", e)

                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="EXIT",
                    reason="FLAG_FALSE_SQUAREOFF",
                    spot=spot,
                    atm=active_atm,
                    ce_sym=ce_ts,
                    pe_sym=pe_ts,
                    unreal=unreal,
                    ce_entry=pos.get("CE"),
                    pe_entry=pos.get("PE"),
                    ce_exit=ce_exit,
                    pe_exit=pe_exit,
                )

                pos.clear()
                ce_ts = pe_ts = None
                active_atm = None
                halted = True

            # resume when flag becomes true again
            if allowed and halted and not pos:
                halted = False
                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="RESUME",
                    reason="FLAG_TRUE_RESUME",
                    spot=spot,
                    atm=atm,
                    ce_sym=None,
                    pe_sym=None,
                    unreal=0.0,
                    ce_entry=None,
                    pe_entry=None,
                    ce_exit=None,
                    pe_exit=None,
                )

            last_snap = time.time()

        # ---------- TIME EXIT ----------
        if now.time() >= SQUARE_OFF and pos and ce_ts and pe_ts:
            ce_exit = pe_exit = None
            try:
                prices = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                ce_exit = prices.get(f"NFO:{ce_ts}")
                pe_exit = prices.get(f"NFO:{pe_ts}")
            except Exception:
                pass

            try:
                stocko_place_by_tradingsymbol(ce_ts, "SELL", QTY, 101)
                stocko_place_by_tradingsymbol(pe_ts, "SELL", QTY, 102)
            except Exception as e:
                print("❌ 15:25 EXIT FAILED:", e)

            log_db(
                conn,
                bot=BOT_NAME,
                event="EXIT",
                reason="TIME_SQUAREOFF_1525",
                spot=spot,
                atm=active_atm,
                ce_sym=ce_ts,
                pe_sym=pe_ts,
                unreal=None,
                ce_entry=pos.get("CE"),
                pe_entry=pos.get("PE"),
                ce_exit=ce_exit,
                pe_exit=pe_exit,
            )
            print("⏹ 15:25 EXIT")
            break

        # ---------- HALT ----------
        if halted:
            time.sleep(POLL_SEC)
            continue

        # ---------- ENTRY (first entry) ----------
        if (not pos) and (now.time() >= ENTRY_START):
            allowed_now = trade_allowed(conn)
            if allowed_now and abs(spot - atm) <= ENTRY_TOL:
                ce_new, pe_new = pick_atm_symbols(nfo, atm)
                if not ce_new or not pe_new:
                    time.sleep(POLL_SEC)
                    continue

                l = ltp([f"NFO:{ce_new}", f"NFO:{pe_new}"])
                ce_p, pe_p = l.get(f"NFO:{ce_new}"), l.get(f"NFO:{pe_new}")
                if ce_p is None or pe_p is None:
                    time.sleep(POLL_SEC)
                    continue

                try:
                    stocko_place_by_tradingsymbol(ce_new, "BUY", QTY, 1)
                    stocko_place_by_tradingsymbol(pe_new, "BUY", QTY, 2)
                except Exception as e:
                    print("❌ ENTRY FAILED:", e)
                    time.sleep(POLL_SEC)
                    continue

                # commit state only after success
                ce_ts, pe_ts = ce_new, pe_new
                pos["CE"], pos["PE"] = ce_p, pe_p
                active_atm = atm

                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="ENTRY",
                    reason="FIRST_ENTRY",
                    spot=spot,
                    atm=active_atm,
                    ce_sym=ce_ts,
                    pe_sym=pe_ts,
                    unreal=0.0,
                    ce_entry=pos["CE"],
                    pe_entry=pos["PE"],
                    ce_exit=None,
                    pe_exit=None,
                )

                print(f"✅ ENTRY @ ATM={active_atm} | {ce_ts} & {pe_ts}")

        # ---------- ROLLING LOGIC ----------
        # Roll only when:
        #   - position exists
        #   - ATM has changed (next 50pt zone)
        #   - spot is within ±ENTRY_TOL of new ATM (prevents flip-flop / noisy rolls)
        if pos and ce_ts and pe_ts and (active_atm is not None):
            if atm != active_atm and abs(spot - atm) <= ENTRY_TOL:
                # capture exit prices first
                ce_exit = pe_exit = None
                try:
                    prices = ltp([f"NFO:{ce_ts}", f"NFO:{pe_ts}"])
                    ce_exit = prices.get(f"NFO:{ce_ts}")
                    pe_exit = prices.get(f"NFO:{pe_ts}")
                except Exception:
                    pass

                # square-off old legs
                try:
                    stocko_place_by_tradingsymbol(ce_ts, "SELL", QTY, 201)
                    stocko_place_by_tradingsymbol(pe_ts, "SELL", QTY, 202)
                except Exception as e:
                    print("❌ ROLL EXIT FAILED:", e)
                    # If exit fails, do not proceed to new entry (safer)
                    time.sleep(POLL_SEC)
                    continue

                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="ROLL_EXIT",
                    reason=f"ATM_SHIFT {active_atm} -> {atm}",
                    spot=spot,
                    atm=active_atm,
                    ce_sym=ce_ts,
                    pe_sym=pe_ts,
                    unreal=None,
                    ce_entry=pos.get("CE"),
                    pe_entry=pos.get("PE"),
                    ce_exit=ce_exit,
                    pe_exit=pe_exit,
                )

                # pick new ATM symbols
                ce_new, pe_new = pick_atm_symbols(nfo, atm)
                if not ce_new or not pe_new:
                    # exited but cannot re-enter; clear state safely
                    pos.clear()
                    ce_ts = pe_ts = None
                    active_atm = None
                    log_db(
                        conn,
                        bot=BOT_NAME,
                        event="ERROR",
                        reason=f"ROLL_REENTRY_SYMBOL_NOT_FOUND atm={atm}",
                        spot=spot,
                        atm=atm,
                        ce_sym=None,
                        pe_sym=None,
                        unreal=None,
                        ce_entry=None,
                        pe_entry=None,
                        ce_exit=None,
                        pe_exit=None,
                    )
                    time.sleep(POLL_SEC)
                    continue

                l2 = ltp([f"NFO:{ce_new}", f"NFO:{pe_new}"])
                ce_p2, pe_p2 = l2.get(f"NFO:{ce_new}"), l2.get(f"NFO:{pe_new}")
                if ce_p2 is None or pe_p2 is None:
                    pos.clear()
                    ce_ts = pe_ts = None
                    active_atm = None
                    log_db(
                        conn,
                        bot=BOT_NAME,
                        event="ERROR",
                        reason=f"ROLL_REENTRY_LTP_MISSING atm={atm}",
                        spot=spot,
                        atm=atm,
                        ce_sym=ce_new,
                        pe_sym=pe_new,
                        unreal=None,
                        ce_entry=None,
                        pe_entry=None,
                        ce_exit=None,
                        pe_exit=None,
                    )
                    time.sleep(POLL_SEC)
                    continue

                # enter new legs
                try:
                    stocko_place_by_tradingsymbol(ce_new, "BUY", QTY, 203)
                    stocko_place_by_tradingsymbol(pe_new, "BUY", QTY, 204)
                except Exception as e:
                    print("❌ ROLL ENTRY FAILED:", e)
                    # exited already; clear state to avoid phantom position
                    pos.clear()
                    ce_ts = pe_ts = None
                    active_atm = None
                    log_db(
                        conn,
                        bot=BOT_NAME,
                        event="ERROR",
                        reason=f"ROLL_ENTRY_FAILED atm={atm} err={e}",
                        spot=spot,
                        atm=atm,
                        ce_sym=ce_new,
                        pe_sym=pe_new,
                        unreal=None,
                        ce_entry=None,
                        pe_entry=None,
                        ce_exit=None,
                        pe_exit=None,
                    )
                    time.sleep(POLL_SEC)
                    continue

                # commit new position state
                ce_ts, pe_ts = ce_new, pe_new
                pos["CE"], pos["PE"] = ce_p2, pe_p2
                active_atm = atm

                log_db(
                    conn,
                    bot=BOT_NAME,
                    event="ROLL_ENTRY",
                    reason=f"ROLLED_TO_ATM={active_atm}",
                    spot=spot,
                    atm=active_atm,
                    ce_sym=ce_ts,
                    pe_sym=pe_ts,
                    unreal=0.0,
                    ce_entry=pos["CE"],
                    pe_entry=pos["PE"],
                    ce_exit=None,
                    pe_exit=None,
                )

                print(f"🔁 ROLLED to ATM={active_atm} | {ce_ts} & {pe_ts}")

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
