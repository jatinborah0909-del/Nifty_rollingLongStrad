#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – 50PT ATM ROLLING + PER-MIN M2M + TRADE FLAG + STOCKO LIVE EXEC
-----------------------------------------------------------------------------------
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
✅ Kite used ONLY for market data + instruments
✅ LIVE mode places orders ONLY via Stocko (no Kite execution fallback)
✅ Trade flag kill-switch (trade_flag.live_ls_nifty_spot) checked every minute (fail-safe)

ENV (minimum):
- DATABASE_URL
- KITE_API_KEY, KITE_ACCESS_TOKEN
- LIVE_MODE=true/false
- STOCKO_BASE_URL (default https://api.stocko.in)
- STOCKO_ACCESS_TOKEN (required if LIVE_MODE=true)
- STOCKO_CLIENT_ID (required if LIVE_MODE=true)

Optional:
- ENTRY_TOL, QTY_PER_LEG, PROFIT_TARGET, STOP_LOSS, STRIKE_STEP, ATR_PERIOD, TICK_INTERVAL
- MARKET_START_TIME, MARKET_END_TIME
- TABLE_NAME, FLAG_TABLE, FLAG_COL
"""

import os
import re
import time
import math
import pytz
import requests
from datetime import datetime, time as dt_time, date
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from kiteconnect import KiteConnect

# =========================================================
# ENV HELPERS
# =========================================================

def env_bool(k, d=False):
    return os.getenv(k, str(d)).strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(k, d):
    try:
        return int(os.getenv(k, str(d)).strip())
    except Exception:
        return d

def env_float(k, d):
    try:
        return float(os.getenv(k, str(d)).strip())
    except Exception:
        return d

def parse_hhmm(key, default):
    s = os.getenv(key, default).strip()
    hh, mm = s.split(":")
    return dt_time(int(hh), int(mm))

def safe_ident(name: str, fallback: str) -> str:
    name = (name or "").strip()
    if not name:
        return fallback
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        return fallback
    return name

# =========================================================
# CONFIG
# =========================================================

MARKET_TZ = pytz.timezone(os.getenv("MARKET_TZ", "Asia/Kolkata").strip())

MARKET_START_TIME = parse_hhmm("MARKET_START_TIME", "09:15")
MARKET_END_TIME   = parse_hhmm("MARKET_END_TIME",   "15:30")

INDEX_SYMBOL = os.getenv("INDEX_SYMBOL", "NSE:NIFTY 50").strip()
STRIKE_STEP  = env_int("STRIKE_STEP", 50)

ENTRY_TOL     = env_int("ENTRY_TOL", 4)
QTY_PER_LEG   = env_int("QTY_PER_LEG", 65)
PROFIT_TARGET = env_float("PROFIT_TARGET", 1500)
STOP_LOSS     = env_float("STOP_LOSS", 1500)

ATR_PERIOD    = env_int("ATR_PERIOD", 14)
TICK_INTERVAL = env_float("TICK_INTERVAL", 1.0)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

TABLE_NAME = safe_ident(os.getenv("TABLE_NAME", "nifty_long_strang_roll"), "nifty_long_strang_roll")
FLAG_TABLE = safe_ident(os.getenv("FLAG_TABLE", "trade_flag"), "trade_flag")
FLAG_COL   = safe_ident(os.getenv("FLAG_COL", "live_ls_nifty_spot"), "live_ls_nifty_spot")

LIVE_MODE = env_bool("LIVE_MODE", False)

# Kite (data-only)
KITE_API_KEY      = os.getenv("KITE_API_KEY", "").strip()
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

# Stocko (execution-only in LIVE_MODE)
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "").strip()

# =========================================================
# VALIDATION
# =========================================================

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
    raise RuntimeError("Missing Kite credentials (KITE_API_KEY / KITE_ACCESS_TOKEN)")

if LIVE_MODE:
    if not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID:
        raise RuntimeError("LIVE_MODE=True requires STOCKO_ACCESS_TOKEN and STOCKO_CLIENT_ID (Stocko-only execution).")

# =========================================================
# DB
# =========================================================

def db_connect():
    return psycopg2.connect(DATABASE_URL, sslmode="require", cursor_factory=RealDictCursor)

def ensure_table(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {t} (
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
        """).format(t=sql.Identifier(TABLE_NAME)))
    conn.commit()

def log_db(conn, **row):
    cols = list(row.keys())
    vals = [row[k] for k in cols]
    with conn.cursor() as c:
        c.execute(
            sql.SQL("INSERT INTO {t} ({cols}) VALUES ({ph})").format(
                t=sql.Identifier(TABLE_NAME),
                cols=sql.SQL(",").join(map(sql.Identifier, cols)),
                ph=sql.SQL(",").join(sql.Placeholder() * len(cols))
            ),
            vals
        )
    conn.commit()

def read_trade_flag(conn) -> bool:
    try:
        with conn.cursor() as c:
            c.execute(sql.SQL("SELECT {col} FROM {tbl} WHERE id=1").format(
                col=sql.Identifier(FLAG_COL),
                tbl=sql.Identifier(FLAG_TABLE),
            ))
            r = c.fetchone()
            return bool(r[FLAG_COL]) if r and (FLAG_COL in r) else False
    except Exception:
        return False  # fail-safe

# =========================================================
# KITE (DATA ONLY)
# =========================================================

def kite_connect() -> KiteConnect:
    k = KiteConnect(api_key=KITE_API_KEY)
    k.set_access_token(KITE_ACCESS_TOKEN)
    return k

def ltp(kite: KiteConnect, symbol: str) -> float:
    return float(kite.ltp([symbol])[symbol]["last_price"])

def safe_ltp_many(kite: KiteConnect, instruments: list[str]) -> dict[str, float]:
    out = {i: float("nan") for i in instruments}
    try:
        q = kite.ltp(instruments)
        for i in instruments:
            if i in q and "last_price" in q[i]:
                out[i] = float(q[i]["last_price"])
    except Exception:
        pass
    return out

def round_to_strike(price: float) -> int:
    return int(math.floor(price / STRIKE_STEP + 0.5) * STRIKE_STEP)

def get_vix(kite: KiteConnect):
    q = kite.quote(["NSE:INDIA VIX"])
    d = q["NSE:INDIA VIX"]
    return float(d["ohlc"]["close"]), float(d["last_price"])

# =========================================================
# STOCKO (EXECUTION ONLY IN LIVE_MODE)
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
    raise RuntimeError(f"Stocko search: no NFO token found for {keyword}")

def _gen_user_order_id(offset=0) -> str:
    base = int(time.time() * 1000) + int(offset)
    return str(base)[-15:]

def stocko_place_order_token(token: int, side: str, qty: int, offset=0):
    url = f"{STOCKO_BASE_URL}/api/v1/orders"
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
        "product": "NRML",
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": _gen_user_order_id(offset),
        "market_protection_percentage": 0,
        "device": "WEB",
    }
    r = requests.post(url, json=payload, headers=_stocko_headers(), timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Stocko order failed: {r.status_code} {r.text}")
    return r.json()

def stocko_place_by_tradingsymbol(tradingsymbol: str, side: str, qty: int, offset=0):
    if not LIVE_MODE:
        return {"simulated": True}
    tok = stocko_search_token(tradingsymbol)
    return stocko_place_order_token(tok, side, qty, offset=offset)

# =========================================================
# INSTRUMENT RESOLUTION (Options + FUT)
# =========================================================

def load_nfo_once(kite: KiteConnect):
    # list[dict], lightweight enough once at startup
    return kite.instruments("NFO")

def get_nearest_nifty_fut_symbol(nfo: list[dict], today: date) -> str:
    futs = []
    for i in nfo:
        if i.get("name") != "NIFTY":
            continue
        if i.get("instrument_type") != "FUT":
            continue
        exp = i.get("expiry")
        if exp and hasattr(exp, "year") and exp >= today:
            futs.append(i)
    if not futs:
        # fallback: sort anyway
        futs = [i for i in nfo if i.get("name") == "NIFTY" and i.get("instrument_type") == "FUT"]
    futs.sort(key=lambda x: x.get("expiry"))
    if not futs:
        raise RuntimeError("No NIFTY FUT found in instruments.")
    return "NFO:" + futs[0]["tradingsymbol"]

def get_nearest_option_symbol(nfo: list[dict], strike: int, opt_type: str, today: date) -> str:
    opts = []
    for i in nfo:
        if i.get("name") != "NIFTY":
            continue
        if i.get("instrument_type") != opt_type:
            continue
        if float(i.get("strike", 0.0)) != float(strike):
            continue
        exp = i.get("expiry")
        if exp and hasattr(exp, "year") and exp >= today:
            opts.append(i)
    if not opts:
        raise RuntimeError(f"No {opt_type} option found for strike {strike}")
    opts.sort(key=lambda x: x.get("expiry"))
    return "NFO:" + opts[0]["tradingsymbol"]

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

    def update(self, now: datetime, fut_ltp: float):
        mk = now.replace(second=0, microsecond=0)
        if self.last_min is None:
            self.last_min = mk
            self.h = self.l = self.c = self.prev_c = fut_ltp
            return None

        if mk == self.last_min:
            self.h = max(self.h, fut_ltp)
            self.l = min(self.l, fut_ltp)
            self.c = fut_ltp
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
        self.h = self.l = self.c = fut_ltp
        return self.atr

# =========================================================
# SESSION HELPERS
# =========================================================

def in_market_hours(now: datetime) -> bool:
    t = now.time()
    return (MARKET_START_TIME <= t <= MARKET_END_TIME)

# =========================================================
# MAIN
# =========================================================

def main():
    conn = db_connect()
    ensure_table(conn)

    kite = kite_connect()
    nfo = load_nfo_once(kite)

    today = datetime.now(MARKET_TZ).date()
    FUT_SYMBOL = get_nearest_nifty_fut_symbol(nfo, today)

    atr_builder = FutAtrBuilder(ATR_PERIOD)

    # State
    position_open = False
    active_atm = None
    ce_symbol = pe_symbol = None
    ce_entry = pe_entry = None
    realized_pnl = 0.0

    last_snapshot_min = None
    last_flag_min = None
    trade_allowed = False

    print(f"🚀 START | LIVE_MODE={LIVE_MODE} | Orders=STOCKO_ONLY (LIVE) | Data=KITE | FUT={FUT_SYMBOL}")

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
                trade_allowed = read_trade_flag(conn)

            # -------- DATA FETCH --------
            spot = ltp(kite, INDEX_SYMBOL)
            fut  = ltp(kite, FUT_SYMBOL)
            atr  = atr_builder.update(now, fut)
            vix_prev, vix = get_vix(kite)

            atm = round_to_strike(spot)
            dist = abs(spot - atm)

            # -------- KILL SWITCH --------
            if position_open and not trade_allowed:
                # exits are STOCKO in LIVE, simulated in PAPER
                ce_inst = ce_symbol
                pe_inst = pe_symbol

                ce_exit = ltp(kite, ce_inst)
                pe_exit = ltp(kite, pe_inst)

                if LIVE_MODE:
                    stocko_place_by_tradingsymbol(ce_inst.split(":", 1)[1], "SELL", QTY_PER_LEG, offset=1001)
                    stocko_place_by_tradingsymbol(pe_inst.split(":", 1)[1], "SELL", QTY_PER_LEG, offset=1002)

                pnl = (ce_exit - ce_entry + pe_exit - pe_entry) * QTY_PER_LEG
                realized_pnl += pnl

                log_db(
                    conn,
                    timestamp=now, status="EXIT", event="KILL_SWITCH_EXIT",
                    reason="FLAG_DISABLED", spot=spot, atm=active_atm,
                    ce_symbol=ce_inst, pe_symbol=pe_inst,
                    ce_entry=ce_entry, pe_entry=pe_entry,
                    ce_exit=ce_exit, pe_exit=pe_exit,
                    ce_ltp=ce_exit, pe_ltp=pe_exit,
                    unreal_pnl=pnl, realized_pnl=realized_pnl,
                    atr=atr, vix_prev=vix_prev, vix=vix,
                    order_engine="STOCKO" if LIVE_MODE else "PAPER"
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
                today = now.date()
                ce_symbol = get_nearest_option_symbol(nfo, atm, "CE", today)
                pe_symbol = get_nearest_option_symbol(nfo, atm, "PE", today)

                # Simulated fills use LTP; LIVE sends STOCKO orders (market) and we still record LTP as "fill"
                ce_entry = ltp(kite, ce_symbol)
                pe_entry = ltp(kite, pe_symbol)

                if LIVE_MODE:
                    stocko_place_by_tradingsymbol(ce_symbol.split(":", 1)[1], "BUY", QTY_PER_LEG, offset=1)
                    stocko_place_by_tradingsymbol(pe_symbol.split(":", 1)[1], "BUY", QTY_PER_LEG, offset=2)

                position_open = True
                active_atm = atm
                last_snapshot_min = None

                log_db(
                    conn,
                    timestamp=now, status="OPEN", event="ENTRY",
                    reason="ATM_TOUCH", spot=spot, atm=atm,
                    ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                    ce_entry=ce_entry, pe_entry=pe_entry,
                    ce_exit=None, pe_exit=None,
                    ce_ltp=ce_entry, pe_ltp=pe_entry,
                    unreal_pnl=0.0, realized_pnl=realized_pnl,
                    atr=atr, vix_prev=vix_prev, vix=vix,
                    order_engine="STOCKO" if LIVE_MODE else "PAPER"
                )

            # -------- POSITION MANAGEMENT --------
            if position_open:
                ce_ltp = ltp(kite, ce_symbol)
                pe_ltp = ltp(kite, pe_symbol)
                unreal = (ce_ltp - ce_entry + pe_ltp - pe_entry) * QTY_PER_LEG

                # ---- PER-MINUTE M2M SNAPSHOT ----
                snap_min = now.replace(second=0, microsecond=0)
                if last_snapshot_min is None or snap_min > last_snapshot_min:
                    last_snapshot_min = snap_min
                    log_db(
                        conn,
                        timestamp=now, status="M2M", event="SNAPSHOT",
                        reason=f"FLAG={trade_allowed}", spot=spot, atm=active_atm,
                        ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                        ce_entry=ce_entry, pe_entry=pe_entry,
                        ce_exit=None, pe_exit=None,
                        ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                        unreal_pnl=unreal, realized_pnl=realized_pnl,
                        atr=atr, vix_prev=vix_prev, vix=vix,
                        order_engine="STOCKO" if LIVE_MODE else "PAPER"
                    )

                # ---- ATM ROLL (NEW 50PT STRIKE ± TOL) ----
                new_atm = round_to_strike(spot)
                if trade_allowed and new_atm != active_atm and abs(spot - new_atm) <= ENTRY_TOL:
                    # Exit old legs
                    ce_exit = ltp(kite, ce_symbol)
                    pe_exit = ltp(kite, pe_symbol)

                    if LIVE_MODE:
                        stocko_place_by_tradingsymbol(ce_symbol.split(":", 1)[1], "SELL", QTY_PER_LEG, offset=2001)
                        stocko_place_by_tradingsymbol(pe_symbol.split(":", 1)[1], "SELL", QTY_PER_LEG, offset=2002)

                    roll_pnl = (ce_exit - ce_entry + pe_exit - pe_entry) * QTY_PER_LEG
                    realized_pnl += roll_pnl

                    log_db(
                        conn,
                        timestamp=now, status="EXIT", event="ROLL_EXIT",
                        reason=f"STRIKE_SHIFT {active_atm}->{new_atm}",
                        spot=spot, atm=active_atm,
                        ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                        ce_entry=ce_entry, pe_entry=pe_entry,
                        ce_exit=ce_exit, pe_exit=pe_exit,
                        ce_ltp=ce_exit, pe_ltp=pe_exit,
                        unreal_pnl=roll_pnl, realized_pnl=realized_pnl,
                        atr=atr, vix_prev=vix_prev, vix=vix,
                        order_engine="STOCKO" if LIVE_MODE else "PAPER"
                    )

                    # Enter new legs
                    today = now.date()
                    ce_symbol = get_nearest_option_symbol(nfo, new_atm, "CE", today)
                    pe_symbol = get_nearest_option_symbol(nfo, new_atm, "PE", today)

                    ce_entry = ltp(kite, ce_symbol)
                    pe_entry = ltp(kite, pe_symbol)

                    if LIVE_MODE:
                        stocko_place_by_tradingsymbol(ce_symbol.split(":", 1)[1], "BUY", QTY_PER_LEG, offset=3001)
                        stocko_place_by_tradingsymbol(pe_symbol.split(":", 1)[1], "BUY", QTY_PER_LEG, offset=3002)

                    active_atm = new_atm
                    last_snapshot_min = None

                    log_db(
                        conn,
                        timestamp=now, status="OPEN", event="ROLL_ENTRY",
                        reason="ATM_ROLL", spot=spot, atm=new_atm,
                        ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                        ce_entry=ce_entry, pe_entry=pe_entry,
                        ce_exit=None, pe_exit=None,
                        ce_ltp=ce_entry, pe_ltp=pe_entry,
                        unreal_pnl=0.0, realized_pnl=realized_pnl,
                        atr=atr, vix_prev=vix_prev, vix=vix,
                        order_engine="STOCKO" if LIVE_MODE else "PAPER"
                    )

                    time.sleep(TICK_INTERVAL)
                    continue

                # ---- FINAL EXIT ON TARGET / SL ----
                if unreal >= PROFIT_TARGET or unreal <= -STOP_LOSS:
                    ce_exit = ltp(kite, ce_symbol)
                    pe_exit = ltp(kite, pe_symbol)

                    if LIVE_MODE:
                        stocko_place_by_tradingsymbol(ce_symbol.split(":", 1)[1], "SELL", QTY_PER_LEG, offset=4001)
                        stocko_place_by_tradingsymbol(pe_symbol.split(":", 1)[1], "SELL", QTY_PER_LEG, offset=4002)

                    realized_pnl += unreal

                    log_db(
                        conn,
                        timestamp=now, status="EXIT", event="FINAL_EXIT",
                        reason="TARGET" if unreal >= PROFIT_TARGET else "SL",
                        spot=spot, atm=active_atm,
                        ce_symbol=ce_symbol, pe_symbol=pe_symbol,
                        ce_entry=ce_entry, pe_entry=pe_entry,
                        ce_exit=ce_exit, pe_exit=pe_exit,
                        ce_ltp=ce_ltp, pe_ltp=pe_ltp,
                        unreal_pnl=unreal, realized_pnl=realized_pnl,
                        atr=atr, vix_prev=vix_prev, vix=vix,
                        order_engine="STOCKO" if LIVE_MODE else "PAPER"
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

if __name__ == "__main__":
    main()
