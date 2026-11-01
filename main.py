# ---- Gold CFD Bot (Bitget CFD data): 5M and 10M, SELL ONLY ----
# PAPER mode only: uses real Bitget CFD bid/ask prices and never sends an order.
#
# Candle 1 and candle 2 pass the rules, then the bot enters a sell right after candle 2 closes.
# Each timeframe (5M, 10M) runs two independent paper streams:
#   BASELINE = old rules, takes every signal (silent: no Telegram messages, summary only)
#   MAIN     = old rules + the step rule + the stop-loss block (sends the Telegram messages)
#     * step rule: among the 5 candles before candle 1 there must be two neighbouring
#       bullish candles where the second closes higher than the first
#     * stop-loss block: after a stop-loss, MAIN waits until the market gives 3 straight
#       falling candles; a "reset pair" (candle 1 bullish, candle 2 opens at candle 1's close
#       and closes at least 0.30 below it) puts the count back to 0
# One trade at a time per stream, so 5M and 10M (and both streams) can all be open together.
#
# Env vars (Railway -> Variables):
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
#   CFD_SYMBOL   (optional, default XAUUSD)
#   DB_PATH      (optional, default gold_5m10m.db; use /data/gold_5m10m.db on Railway)
#   MODE         (optional, default PAPER)
# requirements.txt: requests

import os
import sys
import time
import hmac
import hashlib
import base64
import sqlite3
import requests
from urllib.parse import urlencode
from datetime import datetime, timezone

BASE_URL = "https://api.bitget.com"

# ---- per timeframe settings (USD per ounce = "points") ----
SETTINGS = {
    5: {"min_move": 10.0, "scan_window": 120},
    10: {"min_move": 15.0, "scan_window": 180},
}
TIMEFRAMES = [5, 10]
STRAIGHT_CANDLES = 3          # straight falling candles needed to lift the block after a stop-loss

# ---- strategy settings (same for both timeframes) ----
RUNUP_WINDOW = 12             # look back 12 candles for the run-up
TOP_LOOKBACK = 5              # candle 1 must break the last 5 closes (also the step-rule window)
GAP_TOLERANCE = 0.10          # max |close of candle 1 - open of candle 2|
MIN_CLOSE_BEYOND = 0.30       # candle 2 must close at least this far below candle 1's close
SL_POINTS = 3.0
TP_POINTS = 3.0
MAX_SPREAD = 0.50             # skip a signal if bid/ask spread is wider than this

MONITOR_INTERVAL_SECONDS = 2
SUMMARY_INTERVAL_SECONDS = 86400

TOKEN = CHAT = DB_PATH = SYMBOL = None
API_KEY = API_SECRET = API_PASS = None
_last_logged = {}


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def log_once(key, msg):
    """Log a message only when it changes (keeps retries from flooding the log)."""
    if _last_logged.get(key) != msg:
        _last_logged[key] = msg
        log(msg)


def hm(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%m-%d %H:%M")


# ---------------- DATABASE ----------------
def db(sql, params=(), fetch=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()] if fetch else None
        conn.commit()
        return rows if fetch else cur.lastrowid
    finally:
        conn.close()


def init_db():
    cols = [r["name"] for r in db("PRAGMA table_info(trades)", fetch=True)]
    if cols and "variant" not in cols:                  # old-format table from a previous bot
        old = "trades_old_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        db(f"ALTER TABLE trades RENAME TO {old}")
        log(f"Old trades table kept as {old}; starting a fresh one")
    db("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variant TEXT, tf INTEGER, side TEXT, entry REAL, sl REAL, tp REAL, spread REAL,
            status TEXT DEFAULT 'open', outcome TEXT, exit_price REAL, pnl REAL,
            c2_ts INTEGER, opened_at TEXT, closed_at TEXT)""")
    db("CREATE TABLE IF NOT EXISTS signal_log (signal_key TEXT PRIMARY KEY)")
    db("CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value TEXT)")


def get_meta(key):
    rows = db("SELECT value FROM bot_meta WHERE key = ?", (key,), fetch=True)
    return rows[0]["value"] if rows else None


def set_meta(key, value):
    db("INSERT OR REPLACE INTO bot_meta (key, value) VALUES (?, ?)", (key, value))


def bump(key):
    set_meta(key, str(int(get_meta(key) or 0) + 1))


def signaled(key):
    return bool(db("SELECT 1 FROM signal_log WHERE signal_key = ?", (key,), fetch=True))


def log_signal(key):
    db("INSERT OR IGNORE INTO signal_log (signal_key) VALUES (?)", (key,))


def open_trades(tf=None, variant=None):
    if tf is None:
        return db("SELECT * FROM trades WHERE status='open'", fetch=True)
    return db("SELECT * FROM trades WHERE status='open' AND tf=? AND variant=?",
              (tf, variant), fetch=True)


def insert_trade(variant, tf, entry, sl, tp, spread, c2_ts):
    return db("""INSERT INTO trades (variant, tf, side, entry, sl, tp, spread, c2_ts, opened_at)
                 VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?)""",
              (variant, tf, entry, sl, tp, spread, c2_ts, datetime.now(timezone.utc).isoformat()))


def close_trade(trade_id, outcome, exit_price, pnl):
    db("""UPDATE trades SET status='closed', outcome=?, exit_price=?, pnl=?, closed_at=?
          WHERE id=?""",
       (outcome, exit_price, pnl, datetime.now(timezone.utc).isoformat(), trade_id))


# ---------------- TELEGRAM ----------------
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        log(f"[Telegram error] {e}")


# ---------------- BITGET CFD DATA ----------------
def api_get(path, params):
    query = urlencode(params)
    request_path = f"{path}?{query}"
    ts = str(int(time.time() * 1000))
    sign = base64.b64encode(
        hmac.new(API_SECRET.encode(), (ts + "GET" + request_path).encode(),
                 hashlib.sha256).digest()
    ).decode()
    headers = {
        "ACCESS-KEY": API_KEY, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS, "Content-Type": "application/json",
        "locale": "en-US",
    }
    r = requests.get(BASE_URL + request_path, headers=headers, timeout=15)
    try:
        j = r.json()
    except ValueError:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
    if j.get("code") != "00000":
        raise RuntimeError(f"Bitget {j.get('code')}: {j.get('msg')}")
    return j["data"]


def fetch_quote():
    """Returns (bid, ask)."""
    data = api_get("/api/v3/cfd/market/tickers", {"symbol": SYMBOL})
    d = data[0] if isinstance(data, list) else data
    return float(d["bid1"]), float(d["ask1"])


def parse_rows(data):
    return [[int(r[0])] + [float(x) for x in r[1:5]] for r in data]   # [ts,o,h,l,c]


def raw_1m(minutes):
    """1m candles (the ones used for sell signals) for the last `minutes` minutes. Bitget
    returns at most 100 per request (the newest 100 in the range), so this pages backwards."""
    now_ms = int(time.time() * 1000)
    start = now_ms - minutes * 60000
    rows = {}

    def grab(s, e):
        data = api_get("/api/v3/cfd/market/history-candlestick", {
            "symbol": SYMBOL, "interval": "1m", "side": "sell",
            "startTime": str(s), "endTime": str(e), "limit": "100"})
        got = parse_rows(data) if data else []
        for r in got:
            rows[r[0]] = r
        return got

    end = now_ms + 60000
    for _ in range(6):
        got = grab(start, end)
        if not got:
            break
        oldest = min(r[0] for r in got)
        if oldest <= start or len(got) < 100 or oldest >= end:
            break
        end = oldest
    for _ in range(3):                                  # safety: fill any gap up to now
        newest = max(rows) if rows else None
        if newest is None or newest >= now_ms - 120000:
            break
        got = grab(newest + 60000, now_ms + 60000)
        if not got or max(r[0] for r in got) <= newest:
            break
    return [rows[k] for k in sorted(rows)]


def build_from_1m(rows, tf, now_ms):
    """Group 1m candles into closed candles of tf minutes aligned to the clock.
    A candle is only built if every one of its 1m candles exists."""
    tf_ms = tf * 60000
    buckets = {}
    for r in rows:
        buckets.setdefault(r[0] - r[0] % tf_ms, []).append(r)
    out = []
    for start in sorted(buckets):
        grp = sorted(buckets[start])
        if start + tf_ms > now_ms:                      # still forming
            continue
        if [g[0] for g in grp] != [start + k * 60000 for k in range(tf)]:
            continue                                    # missing minute / market break
        out.append([start, grp[0][1], max(g[2] for g in grp),
                    min(g[3] for g in grp), grp[-1][4]])
    return out


def get_candles(tf, now_ms):
    return build_from_1m(raw_1m((RUNUP_WINDOW + 4) * tf), tf, now_ms)


# ---------------- RULES ----------------
def evaluate_sell(candles, tf):
    """The old sell rules (candle 1 / candle 2 and the run-up). candles = closed candles,
    oldest first, the last one is candle 2. Returns (fired, reason)."""
    tf_ms, min_move = tf * 60000, SETTINGS[tf]["min_move"]
    need = RUNUP_WINDOW + 2
    if len(candles) < need:
        return False, f"only {len(candles)} candles"
    candles = candles[-need:]
    for a, b in zip(candles, candles[1:]):
        if b[0] - a[0] != tf_ms:
            return False, "market break inside lookback"

    closes = [c[4] for c in candles]
    c1, c2 = len(candles) - 2, len(candles) - 1
    open1, close1 = candles[c1][1], candles[c1][4]
    open2, close2 = candles[c2][1], candles[c2][4]
    gap = abs(close1 - open2)

    if close1 <= open1:
        return False, f"c1 not bullish (open {open1:.2f} close {close1:.2f})"
    level = max(closes[c1 - TOP_LOOKBACK:c1])
    move = close1 - min(closes[c1 - RUNUP_WINDOW:c1])
    if close1 <= level:
        return False, f"c1 {close1:.2f} not above prior {TOP_LOOKBACK} high {level:.2f}"
    if move < min_move:
        return False, f"run-up {move:.2f} < {min_move}"
    if gap > GAP_TOLERANCE:
        return False, f"gap {gap:.2f} > {GAP_TOLERANCE}"
    low12 = min(closes[c2 - RUNUP_WINDOW:c2])
    if close2 < low12:
        return False, f"c2 close {close2:.2f} is a new {RUNUP_WINDOW}-candle low"
    back = close1 - close2
    if back < MIN_CLOSE_BEYOND:
        return False, f"c2 closed only {back:.2f} below c1, need {MIN_CLOSE_BEYOND}"
    return True, f"MATCH run-up {move:.2f} gap {gap:.2f} back {back:.2f}"


def step_ok(candles):
    """Step rule: among the 5 candles before candle 1 there are two NEIGHBOURING bullish
    candles where the second closes higher than the first."""
    c1 = len(candles) - 2
    pre = candles[c1 - TOP_LOOKBACK:c1]
    for a, b in zip(pre, pre[1:]):
        if a[4] > a[1] and b[4] > b[1] and b[4] > a[4]:
            return True
    return False


def reset_pair(candles, tf):
    """Block reset rule: the last two candles look like a sell pair (candle 1 bullish, candle 2
    opens at candle 1's close and closes at least MIN_CLOSE_BEYOND below it). Nothing else."""
    if len(candles) < 2:
        return False
    c1, c2 = candles[-2], candles[-1]
    if c2[0] - c1[0] != tf * 60000:
        return False
    return (c1[4] > c1[1] and abs(c1[4] - c2[1]) <= GAP_TOLERANCE
            and c1[4] - c2[4] >= MIN_CLOSE_BEYOND)


# ---------------- STOP-LOSS BLOCK STATE (kept in the database) ----------------
def blk_get(tf):
    """(blocked?, straight-candle count, SL time in ms, last counted candle ts)."""
    return (get_meta(f"sblk_on_{tf}") == "1", int(get_meta(f"sblk_count_{tf}") or 0),
            int(get_meta(f"sblk_since_{tf}") or 0), int(get_meta(f"sblk_last_{tf}") or 0))


def blk_set(tf, on, count, since, last):
    set_meta(f"sblk_on_{tf}", "1" if on else "0")
    set_meta(f"sblk_count_{tf}", str(count))
    set_meta(f"sblk_since_{tf}", str(since))
    set_meta(f"sblk_last_{tf}", str(last))


# ---------------- SCAN ----------------
def open_paper_trade(variant, tf, entry, spread, c2_ts):
    sl, tp = entry + SL_POINTS, entry - TP_POINTS
    tid = insert_trade(variant, tf, entry, sl, tp, spread, c2_ts)
    if variant == "MAIN":                               # BASELINE stays silent
        send_telegram(
            f"🔴 *GOLD {tf}M SELL* (#{tid}) PAPER\n"
            f"Entry: `{entry:.2f}`\nSL: `{sl:.2f}`\nTP: `{tp:.2f}`\nSpread: `{spread:.2f}`")
    return tid


def scan(tf, boundary_ms):
    """Candle 2 has just closed. True = finished. False = retry (data/quote problem)."""
    tf_ms = tf * 60000
    last_closed = boundary_ms - tf_ms                   # open time of candle 2
    bkey, mkey, skey = f"B-{tf}M-{last_closed}", f"M-{tf}M-{last_closed}", f"S-{tf}M-{last_closed}"
    base_todo = not open_trades(tf, "BASELINE") and not signaled(bkey)
    main_todo = not open_trades(tf, "MAIN") and not signaled(mkey)
    blocked, count, since, last = blk_get(tf)
    need_count = blocked and last != last_closed
    if not (base_todo or main_todo or need_count):
        return True

    try:
        candles = get_candles(tf, int(time.time() * 1000))
    except Exception as e:
        log_once(f"fetch-{tf}", f"{tf}M candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False
    if not candles or candles[-1][0] != last_closed:
        log_once(f"late-{tf}", f"{tf}M latest candle {last_closed} not available yet, retrying")
        return False

    ok, why = evaluate_sell(candles, tf)
    log(f"{tf}M [{hm(last_closed)}] sell: {why}")

    # ---- update the straight-candle count after a stop-loss ----
    if need_count and last_closed + tf_ms > since:
        candle = candles[-1]
        if last and last_closed - last > tf_ms:
            count = 0                                   # a candle was missed, not "straight"
        if reset_pair(candles, tf):
            count = 0                                   # a sell-like pair restarts the count
        elif candle[4] < candle[1]:                     # a falling candle
            count += 1
        else:
            count = 0
        if count >= STRAIGHT_CANDLES:
            log(f"{tf}M sell block lifted after {STRAIGHT_CANDLES} straight falling candles")
            blk_set(tf, False, 0, 0, 0)
            blocked = False
        else:
            blk_set(tf, True, count, since, last_closed)
            log(f"{tf}M sell block: {count}/{STRAIGHT_CANDLES} straight falling candles")

    if not ok:
        return True

    # ---- decide what each stream does ----
    plan = []
    if base_todo:
        plan.append(("BASELINE", bkey))
    if main_todo:
        if blocked:
            reason = "block"
        elif not step_ok(candles):
            reason = "step"
        else:
            reason = None
            plan.append(("MAIN", mkey))
        if reason and not signaled(skey):               # count each skipped signal once
            log_signal(skey)
            bump(f"skip_{reason}_{tf}")
            log(f"{tf}M MAIN signal skipped by the {reason} rule")
    if not plan:
        return True

    try:
        bid, ask = fetch_quote()
    except Exception as e:
        log(f"{tf}M signal matched but quote fetch failed: {e}")
        return False
    spread = ask - bid
    for variant, key in plan:
        log_signal(key)
        if spread > MAX_SPREAD:
            log(f"{tf}M {variant} skipped: spread {spread:.2f} > {MAX_SPREAD}")
            continue
        open_paper_trade(variant, tf, bid, spread, last_closed)     # a sell fills at the bid
        log(f"{tf}M [{hm(last_closed)}] {variant} SELL @ {bid:.2f} (spread {spread:.2f})")
    return True


# ---------------- MONITOR ----------------
def monitor():
    trades = open_trades()
    if not trades:
        return
    try:
        bid, ask = fetch_quote()
    except Exception as e:
        log_once("mon", f"monitor quote failed: {type(e).__name__}: {str(e)[:150]}")
        return

    for t in trades:
        px = ask                                        # a sell closes at the ask
        hit_sl, hit_tp = px >= t["sl"], px <= t["tp"]
        if not (hit_sl or hit_tp):
            continue
        pnl = t["entry"] - px
        outcome = "SL" if hit_sl else "TP"
        close_trade(t["id"], outcome, px, pnl)          # recorded at the price the check saw
        log(f"trade #{t['id']} {t['tf']}M {t['variant']} {outcome} entry {t['entry']:.2f} "
            f"exit {px:.2f} pnl {pnl:+.2f}")
        if t["variant"] != "MAIN":
            continue                                    # BASELINE stays silent
        extra = ""
        if outcome == "SL":                             # a stop-loss starts the block
            blk_set(t["tf"], True, 0, int(time.time() * 1000), 0)
            extra = f"\nSells paused until {STRAIGHT_CANDLES} falling candles"
        icon = "✅" if outcome == "TP" else "❌"
        send_telegram(
            f"{icon} *GOLD {t['tf']}M SELL #{t['id']} closed: {outcome}*\n"
            f"Entry `{t['entry']:.2f}` -> exit `{px:.2f}`\nResult: `{pnl:+.2f}` points{extra}")


# ---------------- SUMMARY ----------------
def summary():
    now = datetime.now(timezone.utc)
    last = get_meta("last_summary_sent")
    if last is None:
        set_meta("last_summary_sent", now.isoformat())
        return
    if (now - datetime.fromisoformat(last)).total_seconds() < SUMMARY_INTERVAL_SECONDS:
        return
    rows = db("""SELECT variant, tf, outcome, pnl FROM trades
                 WHERE status='closed' AND closed_at>=?""", (last,), fetch=True)

    def line(label, sub):
        tp = sum(1 for r in sub if r["outcome"] == "TP")
        net = sum(r["pnl"] or 0 for r in sub)
        return f"{label}: {len(sub)} closed | TP {tp} | SL {len(sub) - tp} | Net `{net:+.2f}`"

    parts = ["📊 *Daily Summary (PAPER, sell only)*"]
    for tf in TIMEFRAMES:
        base = [r for r in rows if r["tf"] == tf and r["variant"] == "BASELINE"]
        main = [r for r in rows if r["tf"] == tf and r["variant"] == "MAIN"]
        parts += ["", f"*{tf}M*", line("Baseline (old rules)", base), line("New rules", main),
                  f"Skipped by block: {get_meta(f'skip_block_{tf}') or 0} | "
                  f"Skipped by step rule: {get_meta(f'skip_step_{tf}') or 0}"]
        set_meta(f"skip_block_{tf}", "0")
        set_meta(f"skip_step_{tf}", "0")
    send_telegram("\n".join(parts))
    set_meta("last_summary_sent", now.isoformat())


# ---------------- MAIN ----------------
def pick_source():
    try:
        bid, ask = fetch_quote()
        c = get_candles(5, int(time.time() * 1000))
        if not c:
            log("[SOURCE] no candles could be built")
            return False
        log(f"[SOURCE] {SYMBOL}: bid {bid:.2f} ask {ask:.2f} spread {ask - bid:.2f}; "
            f"latest closed 5M candle {hm(c[-1][0])} UTC close {c[-1][4]:.2f}")
        return True
    except Exception as e:
        log(f"[SOURCE] failed: {type(e).__name__}: {str(e)[:200]}")
        return False


def main():
    global TOKEN, CHAT, DB_PATH, SYMBOL, API_KEY, API_SECRET, API_PASS
    TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT = os.environ["TELEGRAM_CHAT_ID"]
    API_KEY = os.environ["BITGET_API_KEY"]
    API_SECRET = os.environ["BITGET_API_SECRET"]
    API_PASS = os.environ["BITGET_API_PASSPHRASE"]
    SYMBOL = os.environ.get("CFD_SYMBOL", "XAUUSD")
    DB_PATH = os.environ.get("DB_PATH", "gold_5m10m.db")
    mode = os.environ.get("MODE", "PAPER").upper()
    if mode != "PAPER":
        log(f"MODE={mode} is not supported yet. Only PAPER is available. Exiting.")
        sys.exit(1)

    init_db()
    while not pick_source():
        log("No data reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ Gold 5M + 10M bot started (sell only) PAPER\nSymbol: {SYMBOL}\n"
                  f"5M run-up >= {SETTINGS[5]['min_move']:.0f} | 10M run-up >= {SETTINGS[10]['min_move']:.0f} "
                  f"| SL {SL_POINTS:.0f} / TP {TP_POINTS:.0f}")
    log("5M + 10M sell-only bot running in PAPER mode")

    last_scan = {tf: None for tf in TIMEFRAMES}
    last_summary = 0
    while True:
        try:
            now = time.time()
            for tf in TIMEFRAMES:
                into = now % (tf * 60)
                boundary_ms = int((now - into) * 1000)
                if boundary_ms != last_scan[tf] and into < SETTINGS[tf]["scan_window"]:
                    if scan(tf, boundary_ms):
                        last_scan[tf] = boundary_ms
            monitor()
            if time.time() - last_summary >= 3600:
                summary()
                last_summary = time.time()
        except Exception as e:
            log(f"Loop error: {type(e).__name__}: {e}")
        time.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
