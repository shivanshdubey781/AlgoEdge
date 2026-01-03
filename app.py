import datetime
import math
import os
import queue
import threading
import time
import traceback
import json
import sys
import importlib
from pathlib import Path
from collections import deque
from functools import wraps
from typing import Any, Dict, Optional, Tuple, List
from flask import render_template



try:
    if "signal" not in sys.modules:
        import signal as _signal_stdlib  # type: ignore
        sys.modules["signal"] = _signal_stdlib
except Exception:
    pass


try:
    import pyarrow as pa
    import pyarrow.lib as palib
except Exception as e:
    pa = None  # type: ignore
    palib = None  # type: ignore
    print(f"[WARN] pyarrow import failed or unavailable: {e}")

import re
import bcrypt
import jwt
import numpy as np
import pandas as pd
import pyotp
import requests
from flask import Flask, Response, jsonify, request, stream_with_context
from flask_cors import CORS, cross_origin
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# --- GOOGLE SHEETS IMPORTS ---
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None


# Import CSV blueprint (keeps CSV functionality modular)
try:
    from astro_uploads import bp as astro_uploads_bp
    import astro_uploads as astro_uploads_module
except Exception:
    from flask import Blueprint
    astro_uploads_bp = Blueprint('astro_uploads_bp', __name__)
    astro_uploads_module = None

# SmartApi placeholder if not installed (keeps local testing possible)
try:
    from SmartApi.smartConnect import SmartConnect
except Exception:
    class SmartConnect:
        def __init__(self, api_key=None): pass
        def generateSession(self, client_id, password, totp):
            return {'status': False, 'message': 'SmartApi not loaded (placeholder)'}
        def ltpData(self, *args, **kwargs): return {"data": {"ltp": 0.0}, "status": True}
        def placeOrder(self, params): return {"data": {"orderid": "DUMMY123"}, "status": True}
        def orderBook(self): return {"data": []}
        def position(self): return {"data": []}


# -----------------------
# ENVIRONMENT & CONFIG
# -----------------------
load_dotenv()

MASTER_URL: str = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
STRIKE_STEP: int = 50
NIFTY_TOKEN: str = "26000"
NIFTY_LOT_SIZE: int = 25
CAE_BUY_THRESHOLD = 35
CAE_SELL_THRESHOLD = 50
DEFAULT_QTY = 75
INDEX_SYMBOL = "NIFTY"

# -----------------------
# TRADE EXIT & GLOBAL STATE
# -----------------------
TARGET_POINTS = 25
STOPLOSS_POINTS = 15
global_trade_enabled = True

current_trade: Optional[Dict[str, Any]] = None
trade_lock = threading.Lock()

# Alternation variables
last_signal_side = None
last_signal_lock = threading.Lock()

# -----------------------
# STATE & DATA STRUCTURES
# -----------------------
PRICE_HISTORY: deque[float] = deque(maxlen=20)

admin_smart_obj: Any = None
signal_thread: Optional[threading.Thread] = None
signal_stop_event: Optional[threading.Event] = None
main_log_queue: queue.Queue = queue.Queue()
bot_running: bool = False

managed_users: Dict[str, Dict[str, Any]] = {}
managed_users_lock: threading.Lock = threading.Lock()

# -----------------------
# LOGGING
# -----------------------
LOG_DIR = os.environ.get("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

SUCCESS_LOG_FILE = os.path.join(LOG_DIR, "executed_users.log")
EXIT_LOG_FILE = os.path.join(LOG_DIR, "exit_users.log")
REJECTED_LOG_FILE = os.path.join(LOG_DIR, "rejected_orders.log")
POSITIONS_LOG_FILE = os.path.join(LOG_DIR, "positions.log")

os.makedirs(LOG_DIR, exist_ok=True)

def enqueue_log(line: str) -> None:
    try:
        main_log_queue.put_nowait(line)
    except queue.Full:
        pass

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{ts}] [{level}] {msg}"
    try:
        enqueue_log(formatted)
    except Exception:
        pass
    try:
        print(formatted)
    except Exception:
        pass
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(formatted + "\n")
    except Exception:
        pass

def log_success_users(users_list, order_ids=None):
    try:
        with open(SUCCESS_LOG_FILE, "a", encoding="utf-8") as f:
            line = f"{datetime.datetime.now()} | ENTRY_SUCCESS | {users_list}"
            if order_ids:
                line += f" | ORDERS={order_ids}"
            f.write(line + "\n")
    except:
        pass

def log_exit_users(users_list):
    try:
        with open(EXIT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{datetime.datetime.now()} | EXIT_SIGNAL | {users_list}\n")
    except:
        pass

def log_rejected_order(client_id, symbol, qty, response):
    try:
        with open(REJECTED_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(
                f"{datetime.datetime.now()} | ORDER_REJECTED | USER={client_id} "
                f"| SYMBOL={symbol} | QTY={qty} | RESPONSE={response}\n"
            )
    except:
        pass

def log_existing_position(client_id, symbol, qty, side, price):
    try:
        with open(POSITIONS_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(
                f"{datetime.datetime.now()} | EXISTING_POSITION | USER={client_id} "
                f"| SYMBOL={symbol} | SIDE={side} | QTY={qty} | PRICE={price}\n"
            )
    except:
        pass

def log_message(q, msg, level="INFO"):
    log(msg, level)


# -----------------------
# MONGO CONNECTION
# -----------------------
RSI_MONGO_URI = os.environ.get(
    "RSI_MONGO_URI",
    "mongodb+srv://Crestview:Shivansh%40123@cluster0.sqcsrv2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
rsi_mongo_client = MongoClient(RSI_MONGO_URI, server_api=ServerApi("1"))
rsi_db = rsi_mongo_client["trading_bot_db"]
rsi_collection = rsi_db["RSI_LIVE"]

MAIN_MONGO_URI = os.environ.get(
    "MAIN_MONGO_URI",
    "mongodb+srv://Crestview:Shivansh%40123@cluster0.sqcsrv2.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
main_mongo_client = MongoClient(MAIN_MONGO_URI, server_api=ServerApi("1"))
main_db = main_mongo_client["trailing_bot_db"]

users_collection = main_db["users"]
admin_collection = main_db["admin_users"]
csv_uploads_collection = main_db["csv_uploads"]


# -----------------------
# FLASK APP SETUP
# -----------------------
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'super_secret_key_change_me')
app.config["CSV_UPLOADS_COLLECTION"] = csv_uploads_collection

try:
    from astro_uploads import bp as astro_uploads_bp
    app.register_blueprint(astro_uploads_bp, url_prefix="/admin")
except Exception:
    pass

CORS(app, supports_credentials=True, resources={
    r"/*": {
        "origins": "*",
        "allow_headers": ["Content-Type", "Authorization"],
        "methods": ["GET", "POST", "OPTIONS"]
    }
})

# -----------------------
# JWT HELPERS
# -----------------------
def create_jwt(payload: Dict[str, Any]) -> str:
    token = jwt.encode(
        {
            **payload,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        },
        app.config["SECRET_KEY"],
        algorithm="HS256"
    )
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

def decode_jwt(token: str) -> Dict[str, Any]:
    return jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])


# -----------------------
# AUTH DECORATOR
# -----------------------
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split()[1]
        if not token:
            token = request.args.get("token")
        if not token:
            return jsonify({"message": "Token missing"}), 401
        try:
            payload = decode_jwt(token)
            if payload.get("role") != "admin":
                return jsonify({"message": "Unauthorized: Must be Admin"}), 401
        except Exception:
            return jsonify({"message": "Invalid or expired token"}), 401
        return f(*args, **kwargs)
    return decorated


# -----------------------
# USER & DB MANAGEMENT
# -----------------------
def load_users_from_db() -> None:
    global managed_users
    with managed_users_lock:
        managed_users.clear()
        for u in users_collection.find({}):
            try:
                client_id = u.get("client_id")
                if not client_id:
                    continue
                managed_users[client_id] = {
                    "api_key": u.get("api_key"),
                    "password": u.get("password"),
                    "totp_secret": u.get("totp_secret"),
                    "lot_multiplier": u.get("lot_multiplier", 1),
                    "auto_trading": u.get("auto_trading", "OFF"),
                    "smart_obj": None,
                    "status": "Stopped",
                }
            except Exception:
                continue
        log(f"Loaded {len(managed_users)} users from DB", "INFO")


def smart_login(client_id, api_key, password, totp_secret, who="User") -> Any:
    try:
        totp = pyotp.TOTP(totp_secret).now() if totp_secret else None
        obj = SmartConnect(api_key=api_key)
        data = obj.generateSession(client_id, password, totp)
        if data and data.get('status'):
            log(f"Login success for {who} {client_id}", "SUCCESS")
            return obj
        else:
            message = data.get('message', 'Unknown Error') if isinstance(data, dict) else str(data)
            log(f"Login failed for {who} {client_id}: {message}", "ERROR")
            return None
    except Exception as e:
        log(f"Login failed for {who} {client_id}: {str(e)}", "ERROR")
        return None


# -----------------------
# SCRIP MASTER HELPERS
# -----------------------
MASTER_SCRIP_URL = MASTER_URL
SCRIP_CACHE_FILE = os.environ.get("SCRIP_MASTER_CACHE", "scrip_master_cache.json")
SCRIP_LOCK = threading.Lock()
SCRIP_MASTER = None
SCRIP_DF = None

def _normalize_col_name(c):
    return re.sub(r'[^a-z0-9_]', '', c.strip().lower().replace(" ", "_"))

def _normalize_symbol_string(s):
    if s is None: return ""
    s = str(s)
    s = re.sub(r'[\s\-\—\u00A0]+', '', s)
    s = re.sub(r'[^A-Za-z0-9/]', '', s)
    return s.upper()

def _detect_field_names(columns):
    cols = {c: _normalize_col_name(c) for c in columns}
    colset = set(cols.values())
    def pick(*candidates):
        for cand in candidates:
            cand_n = _normalize_col_name(cand)
            if cand_n in colset:
                for orig in columns:
                    if _normalize_col_name(orig) == cand_n:
                        return orig
        return None
    mapping = {
        "symbol": pick("symbol", "tradingsymbol", "trading_symbol", "tokenname"),
        "token": pick("symboltoken", "token", "instrument_token", "instrumentToken"),
        "exchange": pick("exchange", "exch", "segment"),
        "lotsize": pick("lotsize", "lot_size", "lotSize"),
        "expiry": pick("expiry", "expiry_date", "expirydate", "exp"),
    }
    return mapping

def _save_cache_to_disk(obj):
    try:
        Path(SCRIP_CACHE_FILE).write_text(json.dumps(obj))
    except Exception as e:
        log(f"Could not write scrip cache to disk: {e}", "WARN")

def _load_cache_from_disk():
    try:
        p = Path(SCRIP_CACHE_FILE)
        if not p.exists(): return None
        txt = p.read_text()
        return json.loads(txt)
    except Exception as e:
        log(f"Could not read scrip cache from disk: {e}", "WARN")
        return None

def load_scrip_master(log_q, force_reload=False):
    global SCRIP_MASTER, SCRIP_DF
    with SCRIP_LOCK:
        if SCRIP_DF is not None and not force_reload:
            return SCRIP_DF
        log_message(log_q, "Loading/Refreshing Scrip Master...", "INFO")
        last_exc = None
        for attempt in range(1, 4):
            try:
                r = requests.get(MASTER_SCRIP_URL, timeout=60)
                r.raise_for_status()
                SCRIP_MASTER = r.json()
                _save_cache_to_disk(SCRIP_MASTER)
                log_message(log_q, f"Fetched scrip master from remote (attempt {attempt}).", "INFO")
                break
            except Exception as e:
                last_exc = e
                log_message(log_q, f"Remote fetch attempt {attempt} failed: {e}", "WARN")
                time.sleep(1)
        else:
            cached = _load_cache_from_disk()
            if cached:
                SCRIP_MASTER = cached
                log_message(log_q, "Using local cached scrip master (remote fetch failed).", "WARN")
            else:
                log_message(log_q, f"Failed to fetch scrip master and no cache available: {last_exc}", "ERROR")
                SCRIP_MASTER = None
                SCRIP_DF = None
                return None

        try:
            if isinstance(SCRIP_MASTER, dict):
                possible = None
                for v in SCRIP_MASTER.values():
                    if isinstance(v, list):
                        possible = v
                        break
                if possible is not None:
                    data = possible
                else:
                    data = [{"key": k, **(v if isinstance(v, dict) else {})} for k, v in SCRIP_MASTER.items()]
            elif isinstance(SCRIP_MASTER, list):
                data = SCRIP_MASTER
            else:
                log_message(log_q, "Unknown scrip master format.", "ERROR")
                SCRIP_DF = None
                return None
            df = pd.DataFrame(data)
            if df.empty:
                log_message(log_q, "Scrip master data is empty.", "ERROR")
                SCRIP_DF = None
                return None
            mapping = _detect_field_names(list(df.columns))
            df = df.copy()
            if mapping["symbol"]:
                df["symbol_raw"] = df[mapping["symbol"]].astype(str)
            else:
                df["symbol_raw"] = df.apply(lambda r: next((str(r[c]) for c in df.columns if 'symbol' in _normalize_col_name(c) or 'trad' in _normalize_col_name(c)), ""), axis=1)
            if mapping["token"]:
                df["symboltoken"] = df[mapping["token"]].astype(str)
            else:
                df["symboltoken"] = df.apply(lambda r: next((str(r[c]) for c in df.columns if 'token' in _normalize_col_name(c) or 'instrument' in _normalize_col_name(c)), ""), axis=1)
            if mapping["exchange"]:
                df["exchange"] = df[mapping["exchange"]].astype(str)
            else:
                df["exchange"] = df.apply(lambda r: next((str(r[c]) for c in df.columns if 'exch' in _normalize_col_name(c) or 'segment' in _normalize_col_name(c)), "NFO"), axis=1)
            lots_col = mapping["lotsize"]
            if lots_col:
                df["lotsize"] = df[lots_col]
            else:
                df["lotsize"] = pd.NA
            exp_col = mapping["expiry"]
            if exp_col:
                df["expiry"] = df[exp_col].astype(str)
            else:
                df["expiry"] = pd.NA
            df["symbol_norm"] = df["symbol_raw"].apply(_normalize_symbol_string)
            df["symboltoken"] = df["symboltoken"].fillna("").astype(str).str.strip()
            df["exchange"] = df["exchange"].fillna("NFO").astype(str).str.strip()
            def _parse_lot(v):
                try:
                    return int(float(v))
                except Exception:
                    return pd.NA
            df["lotsize"] = df["lotsize"].apply(_parse_lot)
            df.columns = [c.lower() for c in df.columns]
            
            if "symbol" not in df.columns:
                df["symbol"] = df["symbol_raw"]

            SCRIP_DF = df
            log_message(log_q, f"Loaded scrip master into DataFrame ({len(SCRIP_DF)} rows).", "INFO")
            return SCRIP_DF
        except Exception as e:
            log_message(log_q, f"Failed to normalize scrip master into DataFrame: {e}", "ERROR")
            SCRIP_DF = None
            return None


def find_scrip(log_q, tradingsymbol):
    df = load_scrip_master(log_q)
    if df is None:
        log_message(log_q, f"Scrip master unavailable when looking for {tradingsymbol}", "ERROR")
        return None
    if not tradingsymbol:
        return None
    sym = _normalize_symbol_string(tradingsymbol)
    exact = df[df["symbol_norm"] == sym]
    if not exact.empty:
        row = exact.iloc[0].to_dict()
    else:
        starts = df[df["symbol_norm"].str.startswith(sym)]
        if not starts.empty:
            row = starts.iloc[0].to_dict()
        else:
            contains = df[df["symbol_norm"].str.contains(sym)]
            if not contains.empty:
                row = contains.iloc[0].to_dict()
            else:
                candidates = df[df["symbol_norm"].str.contains(sym)]
                if candidates.empty:
                    log_message(log_q, f"⚠ No scrip found for {tradingsymbol}", "WARN")
                    return None
                row = candidates.iloc[0].to_dict()
    token = (str(row.get("symboltoken", "")).strip() or str(row.get("token", "")).strip())
    exch = str(row.get("exchange", "NFO")).strip()
    symbol_name = row.get("symbol_raw") or row.get("symbol") or tradingsymbol
    if not token:
        log_message(log_q, f"⚠ No token found for {tradingsymbol} (resolved to {symbol_name})", "WARN")
        return None
    return {"symbol": symbol_name, "symboltoken": token, "exchange": exch}



def get_lot_size(log_q, tradingsymbol, default=25):
    """
    ✅ AUTO-DETECT LIVE LOT SIZE
    - Reads lot size from CURRENT OPTION CONTRACT
    - Works across expiry changes (65 / 75 / future)
    - No hardcoding
    """

    if not tradingsymbol:
        return default

    df = load_scrip_master(log_q)
    if df is None or df.empty:
        log("⚠️ Scrip master unavailable, using default lot size", "WARN")
        return default

    sym = _normalize_symbol_string(tradingsymbol)

    # Exact match first
    rows = df[df["symbol_norm"] == sym]

    # Fallback (rare)
    if rows.empty:
        rows = df[df["symbol_norm"].str.contains(sym)]

    if rows.empty:
        log(f"⚠️ Lot size not found for {tradingsymbol}, using default", "WARN")
        return default

    try:
        lot = int(rows.iloc[0]["lotsize"])
        if lot > 0:
            log(f"✅ AUTO LOT SIZE DETECTED → {tradingsymbol} = {lot}", "INFO")
            return lot
    except Exception:
        pass

    log(f"⚠️ Invalid lot size for {tradingsymbol}, using default", "WARN")
    return default


# def get_lot_size(log_q, tradingsymbol, default=25):
#     """
#     🔒 HARD-LOCK LOT SIZE FOR NIFTY OPTIONS ONLY
#     Avoids legacy lot sizes (50/65) from scrip master.
#     """

#     if not tradingsymbol:
#         return default

#     # ✅ ONLY NIFTY IS ALLOWED
#     if tradingsymbol.upper().startswith("NIFTY"):
#         return 75   # Current NIFTY lot size

#     # 🚫 Anything else should never happen
#     log(f"🚨 NON-NIFTY SYMBOL RECEIVED IN get_lot_size: {tradingsymbol}", "CRITICAL")
#     return default






def find_nearest_strikes(ltp, step=50):
    ce = math.ceil(ltp / step) * step
    pe = math.floor(ltp / step) * step
    return int(ce), int(pe)



def find_option_symbol(log_q, base_index, strike, option_type="CE"):
    df = load_scrip_master(log_q)
    if df is None:
        return None

    df_search = df.copy()
    df_search["symbol"] = df_search["symbol"].astype(str).str.replace(" ", "").str.upper()

    search_key = str(strike) + option_type.upper()

    # ✅ STRICT NIFTY-ONLY FILTER (THIS IS THE FIX)
    candidates = df_search[
        df_search["symbol"].str.startswith("NIFTY") &          # must start with NIFTY
        ~df_search["symbol"].str.startswith("FINNIFTY") &      # exclude FINNIFTY
        ~df_search["symbol"].str.startswith("BANKNIFTY") &     # exclude BANKNIFTY
        ~df_search["symbol"].str.startswith("MIDCPNIFTY") &    # exclude MIDCPNIFTY
        df_search["symbol"].str.contains(search_key)
    ]

    if candidates.empty:
        log_message(log_q, f"No NIFTY option found for {strike}{option_type}", "WARN")
        return None

    if "expiry" in candidates.columns:
        try:
            candidates = candidates.copy()
            candidates["expiry_dt"] = pd.to_datetime(candidates["expiry"], errors="coerce")
            candidates = candidates.sort_values(by="expiry_dt", ascending=True)
        except Exception:
            pass

    symbol = candidates.iloc[0]["symbol"]
    log_message(log_q, f"✅ STRICT NIFTY OPTION RESOLVED: {symbol}", "INFO")
    return symbol





def get_ltp(smart_obj: Any, symbol: str = "NIFTY") -> float:
    try:
        if symbol == "NIFTY":
            resp = smart_obj.ltpData("NSE", "NIFTY", "26000")
            return float(resp.get("data", {}).get("ltp", 0.0))
        return 0.0
    except Exception as e:
        log(f"Failed to fetch LTP: {str(e)}", "ERROR")
        return 0.0


def get_option_details(ltp: float, option_type: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        if not ltp or ltp <= 0:
            log_message(main_log_queue, f"Invalid LTP for strike calculation: {ltp}", "WARN")
            return None, None
            
        ce_strike, pe_strike = find_nearest_strikes(ltp, STRIKE_STEP)
        atm_strike = ce_strike if option_type.upper() == "CE" else pe_strike
        
        trading_symbol = find_option_symbol(main_log_queue, "NIFTY", atm_strike, option_type)
        if not trading_symbol:
            return None, None
            
        scrip_data = find_scrip(main_log_queue, trading_symbol)
        if not scrip_data:
            return None, None
        token = scrip_data.get('symboltoken')
        return trading_symbol, token
    except Exception as e:
        log_message(main_log_queue, f"get_option_details error: {e}", "ERROR")
        return None, None

def find_existing_position(smart_obj, symbol):
    """
    Check if user already has a net position for this symbol.
    Returns dict {qty, avg_price, side} OR None.
    """
    try:
        pos = smart_obj.position()
        if not pos or "data" not in pos:
            return None

        for row in pos["data"]:
            ts = str(row.get("tradingsymbol", "")).strip()
            netqty = int(row.get("netqty", 0))

            if ts.upper() == symbol.upper() and netqty != 0:
                side = "BUY" if netqty > 0 else "SELL"
                avg_price = float(row.get("avgprice", 0))
                return {
                    "qty": abs(netqty),
                    "avg_price": avg_price,
                    "side": side
                }

        return None

    except Exception as e:
        log(f"find_existing_position error: {e}", "ERROR")
        return None


# -----------------------
# CORE STRATEGY LOGIC (ASTRO/RSI/SIGNAL)
# -----------------------
def get_live_rsi():
    try:
        # Fetch latest document from MongoDB
        doc = rsi_collection.find_one({}, sort=[("_id", -1)])
        if not doc:
            log("No RSI record found in MongoDB", "WARN")
            return None

        # Accept both rsi and RSI keys
        rsi_value = float(doc.get("rsi") or doc.get("RSI"))

        log(f"Latest RSI from MongoDB: {rsi_value}", "INFO")
        return rsi_value

    except Exception as e:
        log(f"MongoDB RSI fetch failed: {e}", "ERROR")
        return None

def read_astro_csv() -> pd.DataFrame | None:
    try:
        path = getattr(astro_uploads_module, "CURRENT_ASTRO_PATH", None)
        if path is None:
            path = "astro_1.csv"
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path)
        df.columns = [c.lower().strip() for c in df.columns]
        df.rename(columns={'u/d logic': 'direction', 'logic': 'direction'}, inplace=True)
        df["dt"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors='coerce')
        df["direction"] = df["direction"].astype(str).str.lower().str.strip()
        return df.dropna(subset=['dt', 'direction'])
    except Exception as e:
        log(f"ASTRO CSV load failed: {str(e)}", "ERROR")
        return None

def get_astro(df: pd.DataFrame | None) -> str | None:
    now = datetime.datetime.now()
    if df is None or df.empty:
        return None
    df_today = df[df['dt'].dt.date == now.date()]
    if df_today.empty:
        return None
    past_events = df_today[df_today['dt'] <= now]
    if past_events.empty:
        return None
    latest_event = past_events.sort_values(by='dt').iloc[-1]
    return latest_event['direction']

def calc_cae(ltp: float) -> float:
    PRICE_HISTORY.append(ltp)
    if len(PRICE_HISTORY) < 5:
        return 50.0
    arr = np.array(PRICE_HISTORY)
    rng = arr.max() - arr.min()
    atr = np.abs(np.diff(arr)).sum()
    if rng == 0.0 or atr == 0.0:
        return 50.0
    cae = 100 * math.log10(atr / rng) / math.log10(len(arr))
    return round(cae, 2)

def generate_signal(rsi: float, astro: Optional[str]) -> Optional[str]:
    if rsi is None or astro is None:
        return None

    try:
        if rsi <= 28.0 and astro == "upside":
            return "BUY CE"
        if rsi >= 72.0 and astro == "downside":
            return "BUY PE"
    except Exception:
        return None
    return None


# -----------------------
# ORDER PLACEMENT
# -----------------------
def place_order(smart_obj: Any, trading_symbol: str, symbol_token: str, qty: int) -> Optional[str]:
    try:
        log(f"🔍 DEBUG ORDER: {trading_symbol} | Token: {symbol_token} | Qty: {qty}", "INFO")

        if not smart_obj:
            log(f"❌ place_order called WITHOUT valid smart_obj for {trading_symbol}", "ERROR")
            log_rejected_order("UNKNOWN", trading_symbol, qty, "No SmartObj")
            return None

        if not trading_symbol or not symbol_token:
            log(f"❌ Invalid symbol/token for order: {trading_symbol} / {symbol_token}", "ERROR")
            log_rejected_order("UNKNOWN", trading_symbol, qty, "Bad symbol/token")
            return None

        if qty <= 0:
            log(f"❌ Invalid quantity for {trading_symbol}: {qty}", "ERROR")
            log_rejected_order("UNKNOWN", trading_symbol, qty, "Bad Qty")
            return None

        params: Dict[str, Any] = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol,
            "symboltoken": symbol_token,
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": "0",
            "quantity": str(qty),
        }

        resp = smart_obj.placeOrder(params)
        log(f"RAW_ORDER_RESPONSE for {trading_symbol}: {resp}", "INFO")

        if isinstance(resp, (str, int)):
            order_id = str(resp).strip()
            if order_id.isdigit():
                return order_id
            else:
                log(f"❌ RAW response not valid orderID: {resp}", "ERROR")
                log_rejected_order("UNKNOWN", trading_symbol, qty, resp)
                return None

        if isinstance(resp, dict):
            if resp.get("status") is True:
                order_id = (
                    resp.get("data", {}).get("orderid")
                    or resp.get("orderid")
                )
                if order_id:
                    return str(order_id).strip()

                log(f"⚠ STATUS TRUE but NO orderid. RAW={resp}", "WARN")
                log_rejected_order("UNKNOWN", trading_symbol, qty, resp)
                return None

            log(f"❌ ORDER REJECTED | {trading_symbol} | Qty={qty} | {resp}", "ERROR")
            log_rejected_order("UNKNOWN", trading_symbol, qty, resp)
            return None

        log(f"❌ Unknown SmartAPI response type: {resp}", "ERROR")
        log_rejected_order("UNKNOWN", trading_symbol, qty, resp)
        return None

    except Exception as e:
        log(f"place_order unexpected exception: {e}", "ERROR")
        log_rejected_order("UNKNOWN", trading_symbol, qty, str(e))
        return None




def place_exit_order(smart_obj: Any, trading_symbol: str, symbol_token: str, qty: int, transactiontype: str) -> bool:
    if not smart_obj:
        log(f"place_exit_order called without a valid smart_obj for {trading_symbol}", "ERROR")
        return False
    try:
        params = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol,
            "symboltoken": symbol_token,
            "transactiontype": transactiontype,
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": "0",
            "quantity": str(qty),
        }
        resp = smart_obj.placeOrder(params)
        log(f"EXIT RESPONSE {trading_symbol}: {resp}", "INFO")

        if resp and isinstance(resp, dict) and resp.get("status"):
            order_id = None
            try:
                order_id = resp.get("data", {}).get("orderid")
            except Exception:
                order_id = None
            log(f"Exit order success: {trading_symbol} Qty={qty} Order ID: {order_id or 'N/A'}", "TRADE")
            return True
        else:
            log(f"Exit order rejected for {trading_symbol}: {resp}", "ERROR")
            log_rejected_order(
                client_id="UNKNOWN",  # Exit loop will replace this
                symbol=trading_symbol,
                qty=qty,
                response=resp
            )
            return False
    except Exception as e:
        log(f"Exit order exception for {trading_symbol}: {e}", "ERROR")
        return False

# -----------------------
# FORCE EXIT HELPER
# -----------------------
def force_exit_all_positions():
    """
    Hard exit all open positions for all managed users,
    stop current trade monitor, and prevent new trades.
    """
    global current_trade, global_trade_enabled

    log("⚠️ FORCE EXIT TRIGGERED BY ADMIN — Closing all active positions NOW", "CRITICAL")

    global_trade_enabled = False

    exited_users = []

    with managed_users_lock:
        for cid, u in list(managed_users.items()):
            smart_obj = u.get("smart_obj")
            if not smart_obj:
                continue

            try:
                pos = smart_obj.position()
                rows = pos.get("data", []) if pos else []
            except Exception as e:
                log(f"Failed to fetch positions for {cid}: {e}", "ERROR")
                continue

            for row in rows:
                ts = str(row.get("tradingsymbol", "")).strip()
                try:
                    netqty_raw = row.get("netqty", 0)
                    netqty = int(float(netqty_raw))
                except Exception:
                    netqty = 0

                transactiontype = "SELL" if netqty > 0 else "BUY"

                if netqty == 0:
                    continue

                qty = abs(netqty)
                token = str(row.get("symboltoken", "")).strip() or None

                if not ts or not token:
                    log(f"Skipping exit for {cid} — missing tradingsymbol/token", "WARN")
                    continue

                success = place_exit_order(smart_obj, ts, token, qty, transactiontype)

                if success:
                    exited_users.append(cid)
                    log(f"🔥 FORCE EXIT SUCCESS — {cid} | {ts} | Qty={qty}", "TRADE")
                else:
                    log(f"❌ FORCE EXIT FAILED — {cid} | {ts}", "ERROR")
                    log_rejected_order(cid, ts, qty, "Force exit rejected")

    with trade_lock:
        current_trade = None

    if exited_users:
        log_exit_users(exited_users)

    log("✅ ALL FORCE EXIT OPERATIONS COMPLETED", "SUCCESS")

    return exited_users


# ✅ MASTER MONITOR LOGIC
def monitor_trade(trade_info):
    """
    ✅ FINAL MONITORING LOGIC (MASTER-SLAVE)
    - USES ONLY ADMIN LTP
    - ADMIN DECIDES ENTRY/EXIT FOR EVERYONE
    - NO INDIVIDUAL USER CHECKS
    """
    global current_trade
    trading_symbol = trade_info["trading_symbol"]
    symbol_token = trade_info["symbol_token"]
    participants = trade_info["participants"]

    # 🔑 ADMIN PARTICIPANT
    try:
        admin = next(p for p in participants if p.get("is_admin"))
        admin_smart = admin["smart_obj"]
    except StopIteration:
        log("❌ CRITICAL: No Admin participant found in trade monitor! Aborting.", "CRITICAL")
        with trade_lock:
            current_trade = None
        return

    # ✅ ENTRY PRICE (ADMIN)
    entry_price = trade_info["entry_price"]
    target_price = round(entry_price + TARGET_POINTS, 2)
    sl_price = round(entry_price - STOPLOSS_POINTS, 2)

    trail_active = False
    trail_sl = sl_price

    log(
        f"📌 MASTER ENTRY | Symbol={trading_symbol} | Entry={entry_price} | TP={target_price} | SL={sl_price}",
        "TRADE"
    )

    while True:
        try:
            # 🔥 ONLY ADMIN LTP
            resp = admin_smart.ltpData("NFO", trading_symbol, symbol_token)
            ltp = float(resp.get("data", {}).get("ltp", 0.0))

            if ltp <= 0:
                time.sleep(1)
                continue

            # 🔔 TRAILING LOGIC (UNCHANGED)
            if not trail_active and ltp >= entry_price + 8:
                trail_active = True
                trail_sl = round(entry_price + 2, 2)
                log(f"🔔 TRAILING ACTIVATED → New SL={trail_sl} (Current LTP: {ltp})", "INFO")

            active_sl = trail_sl if trail_active else sl_price

            # 🎯 EXIT CONDITIONS (UNCHANGED)
            reason = None
            if ltp >= target_price:
                reason = "TARGET HIT"
            elif ltp <= active_sl:
                reason = "STOPLOSS HIT"

            if reason:
                log(f"🚨 MASTER EXIT TRIGGERED → {reason} (LTP: {ltp})", "CRITICAL")

                # 🔥 EXIT ALL USERS AT ONCE
                exited_users = []
                for p in participants:
                    if not p.get("has_position"):
                        continue
                    
                    # Skip actual API call for the virtual Admin participant if qty is 0
                    if p.get("is_admin") and p.get("qty") == 0:
                        continue
                    
                    # ✅ CORRECT EXIT SIDE (DYNAMIC)
                    entry_side = p.get("side", "BUY")
                    exit_side = "SELL" if entry_side == "BUY" else "BUY"

                    place_exit_order(
                        p["smart_obj"],
                        trading_symbol,
                        symbol_token,
                        p["qty"],
                        exit_side
                    )
                    log(f"EXIT SENT → {p['client_id']} | {reason}", "TRADE")
                    exited_users.append(p['client_id'])

                log_exit_users(exited_users)
                break
            
            # Wait a bit before next check to avoid rate limits
            time.sleep(1)

        except Exception as e:
            log(f"Monitor error: {e}", "ERROR")
            time.sleep(1)

    with trade_lock:
        current_trade = None
    log("✅ TRADE CLOSED — ADMIN MASTER MODE", "SUCCESS")


# -----------------------
# SIGNAL THREAD (STABLE + FORCED ALTERNATION)
# -----------------------
def signal_generator(stop_event: threading.Event) -> None:
    astro_df = read_astro_csv()
    try:
        load_scrip_master(main_log_queue)
    except Exception:
        log("Initial Scrip Master load failed. Retrying in 5s...", "WARN")
        time.sleep(5)
        load_scrip_master(main_log_queue)
    log("Signal Generator Started", "SUCCESS")
    global admin_smart_obj, managed_users, current_trade, last_signal_side
    last_executed_signal = None

    while not stop_event.is_set():
        try:
            # ========== DAILY RESET ==========
            now = datetime.datetime.now()
            if now.hour >= 15 and now.minute >= 25:  # 3:25 PM IST
                with last_signal_lock:
                    if last_signal_side is not None:
                        log(f"🕐 Market close - Resetting last_signal_side: {last_signal_side} → None", "INFO")
                        last_signal_side = None
            # ============================================

            # ========== MORNING RESET (09:00–09:15) ==========
            now = datetime.datetime.now()
            if now.hour == 9 and now.minute < 16:  # From 9:00 to 9:15
                with last_signal_lock:
                    if last_signal_side is not None:
                        log(f"🌅 Morning Reset - Clearing last_signal_side ({last_signal_side})", "INFO")
                        last_signal_side = None
            # =================================================

            with trade_lock:
                if current_trade is not None:
                    time.sleep(3)
                    continue

            if admin_smart_obj is None:
                log("Admin API Not Logged In. Cannot fetch LTP.", "ERROR")
                time.sleep(3)
                continue

            rsi = get_live_rsi()
            if rsi is None:
                log("RSI fetch failed, skipping this iteration.", "WARN")
                time.sleep(5)
                continue

            astro_df = read_astro_csv()
            astro = get_astro(astro_df)
            signal = generate_signal(rsi, astro)

            try:
                log(f"RSI={rsi:.2f}  ASTRO={astro if astro else 'N/A'}  SIGNAL={signal if signal else 'NONE'}", "INFO")
            except Exception:
                log(f"RSI={rsi} ASTRO={astro} SIGNAL={signal}", "INFO")

            # ========== ALTERNATION BLOCK ==========
            if signal:
                current_side = "CE" if "CE" in signal else "PE"
                opposite_side = "PE" if current_side == "CE" else "CE"

                with last_signal_lock:
                    if last_signal_side is not None and last_signal_side == current_side:  
                        log(f"🚫 {current_side} REJECTED - Last: {last_signal_side} → Need {opposite_side} first", "INFO")
                        time.sleep(3)
                        continue
                
                if last_executed_signal == signal:
                    log(f"⏳ {current_side} PENDING - Recent failure, retry later...", "INFO")
                    time.sleep(5)
                    continue

                last_executed_signal = signal
            # =============================================================

                if not global_trade_enabled:
                    log("Global trading disabled — skipping execution of detected signal.", "WARN")
                    time.sleep(4)
                    continue

                ltp_index = get_ltp(admin_smart_obj, "NIFTY")
                if not ltp_index or ltp_index <= 0:
                    log(f"Invalid index LTP {ltp_index}. Skipping.", "WARN")
                    time.sleep(4)
                    continue

                option_type = signal.split()[-1]
                trading_symbol, symbol_token = get_option_details(ltp_index, option_type)
                if not symbol_token or not trading_symbol:
                    log(f"Could not resolve option for {option_type} at LTP {ltp_index}. Skipping.", "WARN")
                    time.sleep(4)
                    continue

                # ========================================================
                # 1) CHECK EXISTING POSITIONS & BUILD PARTICIPANTS
                # ========================================================
                
                existing_participants = []

                with managed_users_lock:
                        for cid, u in list(managed_users.items()):
                            if u.get("auto_trading", "OFF") != "ON":
                                 continue
                            if u.get("status") != "Running":
                                continue
                            if not u.get("smart_obj"):
                                    continue
                
                
                # with managed_users_lock:
                #     for cid, u in list(managed_users.items()):
                #         if u.get("auto_trading", "OFF") != "ON":
                #             continue
                #         if u.get("status") != "Running" or not u.get("smart_obj"):
                #             continue
                        
                        pos = find_existing_position(u["smart_obj"], trading_symbol)
                        if pos:
                            log(f"📌 EXISTING POSITION FOUND for {cid}: {pos}", "INFO")
                            
                            log_existing_position(cid, trading_symbol, pos["qty"], pos["side"], pos["avg_price"])
                
                            existing_participants.append({
                                "client_id": cid,
                                "smart_obj": u["smart_obj"],
                                "qty": pos["qty"],
                                "order_id": None,
                                "avg_price": pos["avg_price"],
                                "has_position": True,
                                "is_admin": False,  # ✅ Standard user
                                "side": pos["side"] # ✅ Store existing side
                            })
                
                participants = existing_participants.copy()
                executed_any = bool(participants)
                failed_users = set()

                with managed_users_lock:
                    for cid, u in list(managed_users.items()):
                        if u.get("auto_trading", "OFF") != "ON":
                             log(f"🚫 BLOCKED {cid}: Client automation OFF", "INFO")
                             continue
                        if u.get("status") != "Running":
                            log(f"🚫 BLOCKED {cid}: Admin has NOT started this user", "INFO")
                            continue
                        if not u.get("smart_obj"):
                            log(f"🚫 BLOCKED {cid}: No SmartAPI session", "INFO")
                            continue
                        




                        # if u.get("auto_trading", "OFF") != "ON":
                        #     log(f"Skipping user {cid}: automation OFF", "INFO")
                        #     continue

                        # # Skip users who already have existing positions
                        # if any(p["client_id"] == cid for p in existing_participants):
                        #     continue

                        # if u["status"] != "Running" or not u.get("smart_obj"):
                        #     log(f"Skipping user {cid}: not running or no smart_obj", "INFO")
                        #     continue
                        
                        lot_size = get_lot_size(main_log_queue, trading_symbol, NIFTY_LOT_SIZE)
                        qty = int(lot_size * u.get("lot_multiplier", 1))
                        
                        if qty <= 0:
                            log(f"User {cid} calculated invalid qty {qty}. Skipping.", "WARN")
                            continue
                        
                        try:
                            # ✅ PLACE ORDER FOR USER
                            order_id = place_order(u["smart_obj"], trading_symbol, symbol_token, qty)
                            if order_id:
                                executed_any = True
                                participants.append({
                                    "client_id": cid,
                                    "smart_obj": u["smart_obj"],
                                    "qty": qty,
                                    "order_id": order_id,
                                    "has_position": True,
                                    "is_admin": False, # ✅ Standard user
                                    "side": "BUY" # ✅ Store new entry side (always BUY for now)
                                })
                                log(f"ENTRY SUCCESS for {cid} | OrderID={order_id}", "TRADE")
                                log_success_users([cid], [order_id])
                            else:
                                failed_users.add(cid)
                                log_rejected_order(cid, trading_symbol, qty, "Entry order rejected")
                                continue
                        except Exception as e:
                            failed_users.add(cid)
                            log(f"Order exception for user {cid}: {e}", "ERROR")

                with last_signal_lock:
                    last_signal_side = current_side
                    opposite_side = "PE" if current_side == "CE" else "CE"
                    log(f"🔒 {current_side} LOCKED (win/lose) - Next MUST be {opposite_side}", "INFO")

                if not executed_any:
                    log("No entry orders succeeded and no existing positions found.", "WARN")
                    continue
                
                executed_ids = [p["client_id"] for p in participants]
                log(f"🔥 ACTIVE USERS (New+Existing): {executed_ids}", "TRADE")

                # ✅ FETCH ADMIN LTP FOR ENTRY PRICE
                try:
                    resp = admin_smart_obj.ltpData("NFO", trading_symbol, symbol_token)
                    entry_ltp = float(resp.get("data", {}).get("ltp", 0.0))
                except Exception as e:
                    log(f"Entry LTP fetch failed (Admin): {e}", "WARN")
                    entry_ltp = 0.0
                
                if entry_ltp <= 0:
                    log("❌ Invalid entry LTP - cannot start monitor", "ERROR")
                    continue
                
                # ✅ INJECT ADMIN INTO PARTICIPANTS FOR MASTER MONITOR
                participants.insert(0, {
                    "client_id": "ADMIN",
                    "smart_obj": admin_smart_obj,
                    "qty": 0, # Virtual qty for monitor
                    "order_id": "MASTER_MONITOR",
                    "has_position": False, # Admin is virtual, won't trigger exit API
                    "is_admin": True, # ✅ THIS IS THE MASTER
                    "side": "BUY"
                })

                trade_info = {
                    "trading_symbol": trading_symbol,
                    "symbol_token": symbol_token,
                    "side": option_type.upper(),
                    "entry_price": entry_ltp,
                    # Target/SL calculation moved to monitor_trade based on entry_price
                    "participants": participants,
                    "started_at": datetime.datetime.utcnow().isoformat(),
                }
                
                with trade_lock:
                    current_trade = trade_info
                
                t = threading.Thread(target=monitor_trade, args=(trade_info,))
                t.daemon = True
                t.start()
                log(f"Active trade initialized for {trading_symbol}. Monitor started via ADMIN LTP.", "SUCCESS")

            if not signal:
                last_executed_signal = None

            time.sleep(5)
        except Exception as e:
            log(f"Signal Generator critical error: {str(e)}\n{traceback.format_exc()}", "CRITICAL")
            time.sleep(5)
    log("Signal Generator Stopped", "WARN")


# -----------------------
# ADMIN AUTH ENDPOINTS
# -----------------------
@app.route("/admin_register", methods=["POST"])
def admin_register() -> Response:
    data = request.json or {}
    if not data.get("username") or not data.get("password"):
        return jsonify({"message": "Missing credentials"}), 400
    if admin_collection.find_one({"username": data["username"]}):
        return jsonify({"message": "Admin user already exists"}), 400
    hashed = bcrypt.hashpw(data["password"].encode('utf-8'), bcrypt.gensalt())
    admin_collection.insert_one({"username": data["username"], "password": hashed})
    log(f"Admin created: {data['username']}", "INFO")
    return jsonify({"message": "Admin Created"})


@app.route("/admin_login", methods=["POST"])
def admin_login() -> Response:
    data = request.json or {}
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return jsonify({"message": "Username and password required"}), 400
    user = admin_collection.find_one({"username": username})
    if not user:
        return jsonify({"message": "Invalid login"}), 401
    stored_hash = user["password"]
    if not bcrypt.checkpw(password.encode("utf-8"), stored_hash):
        return jsonify({"message": "Invalid login"}), 401

    token = create_jwt({"username": username, "role": "admin"})

    global admin_smart_obj
    admin_smart_obj = smart_login(
        os.environ.get("SMART_CLIENT_ID", ""),
        os.environ.get("SMART_API_KEY", ""),
        os.environ.get("SMART_PASSWORD", ""),
        os.environ.get("SMART_TOTP_SECRET", ""),
        "Admin"
    )

    if admin_smart_obj is None:
        log("Admin SmartAPI login failed. Bot cannot run trading until fixed.", "CRITICAL")

    log(f"Admin {username} logged in (token issued)", "INFO")
    return jsonify({"token": token})


# -----------------------
# CLIENT LOGIN ENDPOINT
# -----------------------
@app.route("/client_login", methods=["POST"])
def client_login():
    data = request.json or {}
    login_id = data.get("user_login_id")
    login_pass = data.get("user_login_password")
    if not login_id or not login_pass:
        return jsonify({"success": False, "message": "Missing credentials"}), 400
    user = users_collection.find_one({"user_login_id": login_id}) \
        or users_collection.find_one({"client_id": login_id}) \
        or users_collection.find_one({"user_login": login_id})
    if not user:
        return jsonify({"success": False, "message": "User not found"}), 404
    stored_pass = user.get("user_login_password") or user.get("password")
    try:
        if isinstance(stored_pass, (bytes, bytearray)):
            ok = bcrypt.checkpw(login_pass.encode("utf-8"), stored_pass)
        elif isinstance(stored_pass, str) and stored_pass.startswith("$2"):
            ok = bcrypt.checkpw(login_pass.encode("utf-8"), stored_pass.encode("utf-8"))
        else:
            ok = (str(stored_pass) == login_pass)
    except Exception:
        ok = (str(stored_pass) == login_pass)
    if not ok:
        return jsonify({"success": False, "message": "Incorrect password"}), 401
    token = create_jwt({"client_id": user.get("client_id", login_id), "role": "client"})
    return jsonify({
        "success": True,
        "message": "Login successful",
        "token": token,
        "client_id": user.get("client_id", login_id)
    })


# -----------------------
# USER MANAGEMENT ENDPOINTS
# -----------------------
@app.route("/add_user", methods=["POST"])
@token_required
def add_user() -> Response:
    data = request.json or {}
    required_keys = ["client_id", "api_key", "password", "totp_secret"]
    if not all(key in data for key in required_keys):
        return jsonify({"success": False, "message": "Missing user data fields"}), 400
    data['lot_multiplier'] = data.get('lot_multiplier', 1)
    users_collection.update_one({"client_id": data["client_id"]}, {"$set": data}, upsert=True)
    load_users_from_db()
    log(f"User {data['client_id']} added/updated", "INFO")
    return jsonify({"success": True, "message": f"User {data['client_id']} added/updated"})


@app.route("/remove_user", methods=["POST"])
@token_required
def remove_user() -> Response:
    cid = (request.json or {}).get("client_id")
    if not cid:
        return jsonify({"success": False, "message": "Missing client_id"}), 400
    users_collection.delete_one({"client_id": cid})
    load_users_from_db()
    log(f"User {cid} removed", "INFO")
    return jsonify({"success": True, "message": f"User {cid} removed"})


@app.route("/start_user", methods=["POST"])
@token_required
def start_user() -> Response:
    cid = (request.json or {}).get("client_id")
    if not cid:
        return jsonify({"success": False, "message": "Missing client_id"}), 400
    with managed_users_lock:
        u = managed_users.get(cid)
        if not u:
            return jsonify({"success": False, "message": "User not found in memory"}), 404
        u_obj = smart_login(cid, u["api_key"], u["password"], u["totp_secret"], who="Managed User")
        if u_obj:
            u["smart_obj"] = u_obj
            u["status"] = "Running"
            log(f"Managed user {cid} started and logged in", "INFO")
            return jsonify({"success": True, "message": f"User {cid} started and logged in."})
        else:
            u["status"] = "Stopped"
            return jsonify({"success": False, "message": f"User {cid} login failed. Check credentials."}), 500


@app.route("/stop_user", methods=["POST"])
@token_required
def stop_user() -> Response:
    cid = (request.json or {}).get("client_id")
    if not cid:
        return jsonify({"success": False, "message": "Missing client_id"}), 400
    with managed_users_lock:
        u = managed_users.get(cid)
        if not u:
            return jsonify({"success": False, "message": "User not found in memory"}), 404
        u["smart_obj"] = None
        u["status"] = "Stopped"
        log(f"Managed user {cid} stopped", "INFO")
    return jsonify({"success": True, "message": f"User {cid} stopped."})


@app.route("/users_status", methods=["GET"])
@token_required
def users_status():
    with managed_users_lock:
        if managed_users:
            status_list = [
                {"client_id": cid, "status": u.get("status", "Stopped"), "lot_multiplier": u.get("lot_multiplier", 1)}
                for cid, u in managed_users.items()
            ]
            return jsonify({"success": True, "data": status_list})
    docs = users_collection.find({}, {"_id": 0, "client_id": 1, "lot_multiplier": 1})
    fallback = [{"client_id": d.get("client_id"), "status": "Stopped", "lot_multiplier": d.get("lot_multiplier", 1)} for d in docs]
    return jsonify({"success": True, "data": fallback})


@app.route("/get_users", methods=["GET"])
@token_required
def get_users():
    users_map = {}
    for u in users_collection.find({}, {"_id": 0}):
        cid = u.get("client_id")
        if not cid:
            continue
        users_map[cid] = {
            "client_id": cid,
            "api_key": u.get("api_key"),
            "password": u.get("password"),
            "totp_secret": u.get("totp_secret"),
            "user_login_id": u.get("user_login_id"),
            "user_login_password": u.get("user_login_password"),
            "lot_multiplier": u.get("lot_multiplier", 1),
            "auto_trading": u.get("auto_trading", "OFF"),
            "status": "Stopped",
        }
    with managed_users_lock:
        for cid, mu in managed_users.items():
            if cid in users_map:
                users_map[cid]["status"] = mu.get("status", "Stopped")
                users_map[cid]["auto_trading"] = mu.get("auto_trading", "OFF")
    return jsonify({"success": True, "users": users_map})


@app.route("/save_lot", methods=["POST"])
def save_lot():
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    try:
        payload = decode_jwt(token)
        client_id = payload.get("client_id")
    except:
        return jsonify({"success": False, "message": "Invalid token"}), 401

    data = request.json or {}
    lot = int(data.get("lot_size", 1))

    users_collection.update_one(
        {"client_id": client_id},
        {"$set": {"lot_multiplier": lot}}
    )

    with managed_users_lock:
        if client_id in managed_users:
            managed_users[client_id]["lot_multiplier"] = lot

    return jsonify({"success": True, "message": "Lot updated"})


@app.route("/trade_toggle", methods=["POST"])
def trade_toggle():
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    try:
        payload = decode_jwt(token)
        client_id = payload.get("client_id")
    except:
        return jsonify({"success": False, "message": "Invalid token"}), 401

    data = request.json or {}
    state = data.get("trading", "OFF")

    users_collection.update_one(
        {"client_id": client_id},
        {"$set": {"auto_trading": state}}
    )

    with managed_users_lock:
        if client_id in managed_users:
            managed_users[client_id]["auto_trading"] = state

    return jsonify({"success": True, "message": "Trade toggle updated"})


# -----------------------
# FORCE EXIT ENDPOINTS
# -----------------------
@app.route("/exit_all_trades", methods=["POST"])
@token_required
def exit_all_trades():
    log("ADMIN REQUEST — EXIT ALL TRADES", "CRITICAL")
    exited_users = force_exit_all_positions()
    return jsonify({
        "success": True,
        "message": "Force exit executed for all users.",
        "exited_users": list(set(exited_users))
    })

@app.route("/reset_exit_state", methods=["POST"])
@token_required
def reset_exit_state():
    global global_trade_enabled, current_trade
    global_trade_enabled = True
    with trade_lock:
        current_trade = None
    log("Admin reset exit state — trading re-enabled.", "INFO")
    return jsonify({
        "success": True,
        "message": "Global exit state reset. Trading re-enabled.",
        "global_trade_enabled": global_trade_enabled
    })


@app.route('/test_symbol', methods=['POST'])
def test_symbol():
    data = request.json or {}
    symbol = data.get('symbol', 'NIFTY02DEC2526300CE')
    token = data.get('token', '46809')
    
    # Test with admin
    if admin_smart_obj:
        resp = admin_smart_obj.ltpData('NFO', symbol, token)
        return jsonify({'ltp_resp': resp})
    return jsonify({'error': 'No admin login'})


# -----------------------
# SSE LOG STREAMING
# -----------------------
def _sse_generator():
    while True:
        try:
            log_entry = main_log_queue.get(timeout=1.0)
            yield f"data: {log_entry}\n\n"
        except queue.Empty:
            yield ": keep-alive\n\n"
        except GeneratorExit:
            break
        except Exception:
            break

@app.route("/stream", methods=["GET"])
def stream_compat():
    token = request.args.get("token")
    if not token:
        return jsonify({"message": "Token missing in query param"}), 401
    try:
        payload = decode_jwt(token)
        if payload.get("role") != "admin":
            return jsonify({"message": "Unauthorized"}), 401
    except Exception:
        return jsonify({"message": "Invalid or expired token"}), 401

    return Response(stream_with_context(_sse_generator()), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no"
    })

@app.route("/stream_logs", methods=["GET"])
@token_required
def stream_logs() -> Response:
    return Response(stream_with_context(_sse_generator()), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no"
    })


# -----------------------
# BOT START / STOP ENDPOINTS & STARTUP
# -----------------------
def start_signal_thread():
    global signal_thread, signal_stop_event, bot_running, admin_smart_obj

    if signal_thread and signal_thread.is_alive():
        log("Signal Generator already running", "WARN")
        return True

    if admin_smart_obj is None:
        log("Cannot start: Admin API NOT logged in", "ERROR")
        return False

    signal_stop_event = threading.Event()
    signal_thread = threading.Thread(target=signal_generator, args=(signal_stop_event,))
    signal_thread.daemon = True
    signal_thread.start()
    bot_running = True
    log("Signal Generator started", "SUCCESS")
    return True

def stop_signal_thread():
    global signal_thread, signal_stop_event, bot_running
    if signal_stop_event:
        log("Sending stop signal to Signal Generator thread...", "INFO")
        signal_stop_event.set()
        if signal_thread and signal_thread.is_alive():
            signal_thread.join(timeout=5)
            if signal_thread.is_alive():
                log("Signal Generator thread did not stop gracefully.", "ERROR")
            else:
                log("Signal Generator thread stopped.", "SUCCESS")
        signal_thread = None
        signal_stop_event = None
    bot_running = False
    return True

@app.route("/start_bot", methods=["POST"])
@token_required
def start_bot_endpoint():
    ok = start_signal_thread()
    if ok:
        return jsonify({"success": True, "message": "Bot signal generator started."})
    return jsonify({"success": False, "message": "Failed to start bot (Check Admin Login)"}), 400

@app.route("/stop_bot", methods=["POST"])
@token_required
def stop_bot_endpoint():
    stop_signal_thread()
    return jsonify({"success": True, "message": "Bot signal generator stopped."})

@app.route("/start_signal_gen", methods=["POST"])
@token_required
def start_signal_gen_endpoint():
    ok = start_signal_thread()
    if ok:
        return jsonify({"success": True, "message": "Signal generator started"})
    return jsonify({"success": False, "message": "Failed to start signal generator"}), 400

@app.route("/stop_signal_gen", methods=["POST"])
@token_required
def stop_signal_gen_endpoint():
    ok = stop_signal_thread()
    if ok:
        return jsonify({"success": True, "message": "Signal generator stopped"})
    return jsonify({"success": False, "message": "Failed to stop signal generator"}), 400
# -----------------------
# FRONTEND ROUTES
# -----------------------

@app.route("/")
def main_home():
    return render_template("main.html")

@app.route("/home")
def home():
    return render_template("main.html")

@app.route("/login")
def login():
    return render_template("client-login.html")

@app.route("/dashboard")
def dashboard():
    return render_template("client-dashboard.html")
# -----------------------

@app.route("/about")
def about():
    return render_template("about-us.html")

@app.route("/strategies")
def strategies():
    return render_template("strategies.html")

@app.route("/contact")
def contact():
    return render_template("contact-us.html")


if __name__ == "__main__":
    load_users_from_db()

    admin_smart_obj = smart_login(
        os.environ.get("SMART_CLIENT_ID", ""),
        os.environ.get("SMART_API_KEY", ""),
        os.environ.get("SMART_PASSWORD", ""),
        os.environ.get("SMART_TOTP_SECRET", ""),
        "Admin"
    )

    start_signal_thread()

    port = int(os.environ.get("PORT", 9000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)