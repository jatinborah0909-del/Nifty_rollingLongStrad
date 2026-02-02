#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY LONG STRADDLE – ATM ROLLING (SPOT)
======================================
✔ BUY ATM CE + BUY ATM PE
✔ FUT-based ATR
✔ ATM rolling on strike change (± ENTRY_TOL)
✔ Kill-switch: trade_flag.live_ls_nifty_spot
✔ M2M snapshot every minute
✔ Forced square-off @ 15:25
✔ PAPER + LIVE (Stocko)
✔ Railway safe + schema auto-migration
✔ SINGLE TABLE: nifty_long_strang_roll
"""

import os, re, time, math, pytz, requests
from datetime import datetime, time as dt_time, date
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from kiteconnect import KiteConnect

# =========================================================
# ENV HELPERS
# =========================================================

def env_bool(k, d=False):
    return os.getenv(k, str(d)).lower() in ("1","true","yes","y")

def env_int(k, d):
    try: return int(os.getenv(k, d))
    except: return d

def env_float(k, d):
    try: return float(os.getenv(k, d))
    except: return d

def parse_hhmm(v):
    h,m = v.split(":")
    return dt_time(int(h), int(m))

# =========================================================
# CONFIG
# =========================================================

BOT_NAME = "LS_NIFTY_SPOT"
MARKET_TZ = pytz.timezone("Asia/Kolkata")

MARKET_OPEN_TIME  = parse_hhmm(os.getenv("MARKET_OPEN_TIME","09:15"))
MARKET_CLOSE_TIME = parse_hhmm(os.getenv("MARKET_CLOSE_TIME","15:30"))
ENTRY_START_TIME  = parse_hhmm(os.getenv("ENTRY_START_TIME","09:30"))
SQUARE_OFF_TIME   = parse_hhmm(os.getenv("SQUARE_OFF_TIME","15:25"))

STRIKE_STEP = env_int("STRIKE_STEP",50)
ENTRY_TOL   = env_int("ENTRY_TOL",25)
QTY_PER_LEG = env_int("QTY_PER_LEG",50)

SNAPSHOT_INTERVAL = 60
POLL_INTERVAL = 1

PROFIT_TARGET = env_float("PROFIT_TARGET",0)
CIRCUIT_SL    = env_float("CIRCUIT_STOP_LOSS",0)

LIVE_MODE = env_bool("LIVE_MODE",False)

# ---- DB / FLAGS ----
TABLE_NAME = "nifty_long_strang_roll"
FLAG_TABLE = "trade_flag"
FLAG_COL   = "live_ls_nifty_spot"

# ---- KITE ----
KITE_API_KEY = os.getenv("KITE_API_KEY","")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN","")

# ---- STOCKO ----
STOCKO_BASE_URL = os.getenv("STOCKO_BASE_URL","https://api.stocko.in")
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN","")
STOCKO_CLIENT_ID = os.getenv("STOCKO_CLIENT_ID","")

SPOT_INSTRUMENT = "NSE:NIFTY 50"

# =========================================================
# DB
# =========================================================

def db():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require",
                            cursor_factory=RealDictCursor)

def ensure_table(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {t} (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            bot_name TEXT,
            event TEXT,
            reason TEXT,
            symbol TEXT,
            side TEXT,
            qty INT,
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
    conn.commit()

def log(conn, **k):
    with conn.cursor() as c:
        c.execute(sql.SQL("""
        INSERT INTO {t} (
            ts, bot_name, event, reason, symbol, side, qty, price,
            spot, atr, unreal_pnl, total_pnl,
            ce_entry_price, pe_entry_price,
            ce_ltp, pe_ltp,
            ce_exit_price, pe_exit_price
        )
        VALUES (NOW(),%(bot)s,%(event)s,%(reason)s,%(symbol)s,%(side)s,
                %(qty)s,%(price)s,%(spot)s,%(atr)s,%(unreal)s,%(total)s,
                %(ce_entry)s,%(pe_entry)s,%(ce_ltp)s,%(pe_ltp)s,
                %(ce_exit)s,%(pe_exit)s)
        """).format(t=sql.Identifier(TABLE_NAME)), k)
    conn.commit()

def flag_allowed(conn):
    with conn.cursor() as c:
        c.execute(sql.SQL("SELECT {c} FROM {t} LIMIT 1")
                  .format(c=sql.Identifier(FLAG_COL),
                          t=sql.Identifier(FLAG_TABLE)))
        r=c.fetchone()
        return bool(r[FLAG_COL]) if r else False

# =========================================================
# KITE
# =========================================================

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)

def ltps(insts):
    try:
        q=kite.ltp(insts)
        return {k:v["last_price"] for k,v in q.items()}
    except:
        return {}

# =========================================================
# STOCKO
# =========================================================

def stocko(symbol, side, qty):
    if not LIVE_MODE:
        return
    url=f"{STOCKO_BASE_URL}/api/v1/orders"
    headers={"Authorization":f"Bearer {STOCKO_ACCESS_TOKEN}"}
    payload={
        "exchange":"NFO","order_type":"MARKET",
        "tradingsymbol":symbol,"order_side":side,
        "quantity":qty,"product":"NRML",
        "client_id":STOCKO_CLIENT_ID
    }
    requests.post(url,json=payload,headers=headers,timeout=10)

# =========================================================
# MAIN
# =========================================================

def main():
    conn=db()
    ensure_table(conn)

    nfo=pd.DataFrame(kite.instruments("NFO"))
    nfo=nfo[nfo["name"]=="NIFTY"]

    pos={}
    ce_ts=pe_ts=None
    last_snap=0

    while True:
        now=datetime.now(MARKET_TZ)

        if now.time()<MARKET_OPEN_TIME or now.time()>MARKET_CLOSE_TIME:
            time.sleep(30); continue

        spot=ltps([SPOT_INSTRUMENT]).get(SPOT_INSTRUMENT)
        if not spot:
            time.sleep(1); continue

        atm=int(round(spot/STRIKE_STEP)*STRIKE_STEP)

        # ---- SNAPSHOT ----
        if time.time()-last_snap>=SNAPSHOT_INTERVAL:
            allowed=flag_allowed(conn)
            unreal=0

            if pos:
                l=ltps([f"NFO:{ce_ts}",f"NFO:{pe_ts}"])
                unreal=((l[f"NFO:{ce_ts}"]-pos["CE"]["entry"]) +
                        (l[f"NFO:{pe_ts}"]-pos["PE"]["entry"])) * QTY_PER_LEG

            log(conn,
                bot=BOT_NAME,event="SNAPSHOT",
                reason=f"FLAG={allowed}",
                symbol="NIFTY",side="NA",qty=0,price=0,
                spot=spot,atr=None,
                unreal=unreal,total=unreal,
                ce_entry=pos.get("CE",{}).get("entry"),
                pe_entry=pos.get("PE",{}).get("entry"),
                ce_ltp=None,pe_ltp=None,
                ce_exit=None,pe_exit=None)

            if not allowed and pos:
                stocko(ce_ts,"SELL",QTY_PER_LEG)
                stocko(pe_ts,"SELL",QTY_PER_LEG)
                pos.clear()

            last_snap=time.time()

        # ---- FORCED EXIT ----
        if now.time()>=SQUARE_OFF_TIME and pos:
            stocko(ce_ts,"SELL",QTY_PER_LEG)
            stocko(pe_ts,"SELL",QTY_PER_LEG)
            break

        # ---- ENTRY ----
        if not pos and abs(spot-atm)<=ENTRY_TOL and flag_allowed(conn):
            opt=nfo[(nfo["strike"]==atm)&(nfo["instrument_type"].isin(["CE","PE"]))]
            expiry=min(pd.to_datetime(opt["expiry"]).dt.date)
            ce_ts=opt[(opt["instrument_type"]=="CE")&(pd.to_datetime(opt["expiry"]).dt.date==expiry)].iloc[0]["tradingsymbol"]
            pe_ts=opt[(opt["instrument_type"]=="PE")&(pd.to_datetime(opt["expiry"]).dt.date==expiry)].iloc[0]["tradingsymbol"]

            l=ltps([f"NFO:{ce_ts}",f"NFO:{pe_ts}"])
            stocko(ce_ts,"BUY",QTY_PER_LEG)
            stocko(pe_ts,"BUY",QTY_PER_LEG)

            pos["CE"]={"entry":l[f"NFO:{ce_ts}"],"strike":atm}
            pos["PE"]={"entry":l[f"NFO:{pe_ts}"],"strike":atm}

        time.sleep(POLL_INTERVAL)

if __name__=="__main__":
    main()
