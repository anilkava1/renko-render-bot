import os
import json
import time
import requests
import pandas as pd
import gradio as gr
import sys  # Stream output flush karne ke liye
from datetime import datetime, timedelta

# --- GLOBAL ARCHITECTURE PRESETS ---
BASE_URL = "https://api.india.delta.exchange" 
SYMBOL = "BTCUSD"           # Strategy Param: Strict 100% Accurate Linear Futures Ticker
BRICK_SIZE = 100.0         # Strategy Parameter: Fixed 100 Point Brick Size
LEVERAGE = 10.0            # Strategy Parameter: 10x Compound Power
MAX_LOT_LIMIT = 100.0      # Strategy Parameter: Strict 100 Lots Limit Cap
TAKER_FEE_RATE = 0.0005    # 0.05% Delta Exchange Futures Fee
SLIPPAGE_RATE = 0.0003     # 0.03% Slippage Padding

STATE_FILE = "data/live_renko_state.json"
LOG_FILE = "data/live_bot_log.txt"

os.makedirs("data", exist_ok=True)

# WARNINGS AND DEPRECATION CODES FILTER OUT BLOCK
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# =====================================================================
# SYSTEM CORE UTILITIES & LOGGERS
# =====================================================================
def get_ist_time():
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d %H:%M:%S')

def log_action(text):
    current_time = get_ist_time()
    formatted_text = f"[{current_time}] {text}"
    print(formatted_text)
    sys.stdout.flush()  # Instantly flushes output to Render Log Dashboard Panel
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(formatted_text + "\n")
    except:
        pass

def load_system_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        'balance': 10.00,
        'in_position': False,
        'position_type': None,
        'entry_price': 0.0,
        'sl_price': 0.0,
        'position_size': 0.0,
        'last_brick_price': None,
        'last_brick_type': None
    }

def save_system_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

# =====================================================================
# 🚀 ACCURATE DELTA PUBLIC HISTORY BOOTSTRAP (NO KEYS NEEDED)
# =====================================================================
def fetch_delta_futures_historical_closes(limit=24):
    url = f"{BASE_URL}/v2/history/candles"
    end_time = int(time.time())
    start_time = end_time - (limit * 3600 * 3)
    
    params = {"symbol": SYMBOL, "resolution": "1h", "start": start_time, "end": end_time}
    headers = {"User-Agent": "python-rest-client", "Accept": "application/json"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=8)
        if res.status_code == 200:
            raw_data = res.json()
            if 'result' in raw_data and raw_data['result']:
                closes = [float(c['close']) for c in raw_data['result'] if 'close' in c]
                if len(closes) > 1:
                    return closes[-limit-1:-1] if len(closes) > limit else closes[:-1]
    except Exception as e:
        log_action(f"⚠️ Futures History Node Connection Reset: {e}")
    return []

def bootstrap_renko_state_from_history():
    state = load_system_state()
    if state["last_brick_price"] is not None and state["last_brick_type"] is not None:
        return state

    log_action("🔄 INITIALIZATION: Delta Futures standard database se 100% accurate history load ho raha hai...")
    past_closes = fetch_delta_futures_historical_closes(limit=24)
    
    if not past_closes or len(past_closes) < 2:
        log_action("⚠️ Futures historical data frame missing. Falling back to dynamic live ticker anchor.")
        state["last_brick_price"] = 0.0  
        state["last_brick_type"] = "GREEN"
        save_system_state(state)
        return state

    anchor_price = past_closes[0]
    trend_type = "GREEN"
    
    for current_close in past_closes[1:]:
        gap = current_close - anchor_price
        if abs(gap) >= BRICK_SIZE:
            num_bricks = int(abs(gap) / BRICK_SIZE)
            direction = 1 if gap > 0 else -1
            trend_type = "GREEN" if direction == 1 else "RED"
            anchor_price = anchor_price + (direction * num_bricks * BRICK_SIZE)

    state["last_brick_price"] = anchor_price
    state["last_brick_type"] = trend_type
    save_system_state(state)
    log_action(f"🧱 FUTURES PRICE ACTION LOCKED -> Baseline Anchor Set At: ${anchor_price:,.2f} | Trend State: [{trend_type}]")
    return state

def fetch_delta_live_futures_close():
    url = f"{BASE_URL}/v2/history/candles"
    params = {"symbol": SYMBOL, "resolution": "1h", "limit": 2}
    headers = {"User-Agent": "python-rest-client", "Accept": "application/json"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=5)
        if res.status_code == 200:
            raw_data = res.json()
            if 'result' in raw_data and len(raw_data['result']) >= 2:
                return float(raw_data['result'][-2]['close'])
    except Exception as e:
        log_action(f"⚠️ Live Ticker Connection Shift: {e}")
    return None

# =====================================================================
# STRATEGY RISK EXECUTION ENGINE
# =====================================================================
def execute_live_trading_tick():
    state = load_system_state()
    live_price = fetch_delta_live_futures_close() 
    
    if live_price is None:
        return stream_terminal_output()

    if state["last_brick_price"] is None or state["last_brick_price"] == 0.0:
        state["last_brick_price"] = live_price
        state["last_brick_type"] = "GREEN"
        save_system_state(state)
        log_action(f"🧱 RENKO SEEDING COMPLETE: Level Set At ${live_price:,.2f}")
        return stream_terminal_output()

    current_brick_price = state["last_brick_price"]
    prev_brick_type = state["last_brick_type"]
    new_brick_formed = False
    current_brick_type = prev_brick_type

    if live_price >= current_brick_price + BRICK_SIZE:
        state["last_brick_price"] += BRICK_SIZE
        state["last_brick_type"] = "GREEN"
        current_brick_type = "GREEN"
        new_brick_formed = True
    elif live_price <= current_brick_price - BRICK_SIZE:
        state["last_brick_price"] -= BRICK_SIZE
        state["last_brick_type"] = "RED"
        current_brick_type = "RED"
        new_brick_formed = True

    if state["in_position"]:
        exit_order = False
        reason = ""
        
        if state["position_type"] == "BUY":
            if live_price <= state["sl_price"]:
                exit_order, reason = True, "SL HIT"
            elif new_brick_formed and current_brick_type == "RED":
                exit_order, reason = "REVERSAL_FLIP", "OPPOSITE_BRICK_EXIT"
                
        elif state["position_type"] == "SELL":
            if live_price >= state["sl_price"]:
                exit_order, reason = True, "SL HIT"
            elif new_brick_formed and current_brick_type == "GREEN":
                exit_order, reason = "REVERSAL_FLIP", "OPPOSITE_BRICK_EXIT"

        if exit_order:
            size = state["position_size"]
            entry_p = state["entry_price"]
            pnl_gross = (live_price - entry_p) * size if state["position_type"] == "BUY" else (entry_p - live_price) * size
            costs = (size * live_price * TAKER_FEE_RATE) + (size * live_price * SLIPPAGE_RATE)
            net_pnl = pnl_gross - costs
            
            state["balance"] += net_pnl
            state["in_position"] = False
            state["position_type"] = None
            log_action(f"❌ [DEMO CLOSED] via {reason} at ${live_price:,.2f} | Net PnL: ${net_pnl:+.2f} | Balance: ${state['balance']:.2f}")
            save_system_state(state)
            
            if exit_order == "REVERSAL_FLIP":
                flip_side = "SELL" if current_brick_type == "RED" else "BUY"
                state = trigger_demo_entry(state, live_price, flip_side)
                save_system_state(state)
            return stream_terminal_output()

    if not state["in_position"] and new_brick_formed:
        if prev_brick_type == "RED" and current_brick_type == "GREEN":
            state = trigger_demo_entry(state, live_price, "BUY")
            save_system_state(state)
        elif prev_brick_type == "GREEN" and current_brick_type == "RED":
            state = trigger_demo_entry(state, live_price, "SELL")
            save_system_state(state)

    pos_info = f"{state['position_type']} (Entry: ${state['entry_price']:.2f})" if state["in_position"] else "NO POSITION"
    log_action(f"📡 100% FUTURES SYNC -> Contract Price: ${live_price:,.2f} | Renko: ${state['last_brick_price']:.1f} [{state['last_brick_type']}] | Balance: ${state['balance']:.2f} | State: {pos_info}")
    return stream_terminal_output()

def trigger_demo_entry(state, price, side):
    calculated_qty = (state["balance"] * LEVERAGE) / price
    position_size = min(calculated_qty, MAX_LOT_LIMIT)
    
    if position_size <= 0:
        return state

    entry_costs = (position_size * price * TAKER_FEE_RATE) + (position_size * price * SLIPPAGE_RATE)
    state["balance"] -= entry_costs
    state["in_position"] = True
    state["position_type"] = side
    state["entry_price"] = price
    state["position_size"] = position_size
    state["sl_price"] = (price - BRICK_SIZE) if side == "BUY" else (price + BRICK_SIZE)
    
    log_action(f"🟢 [DEMO {side} OPENED] Price: ${price:,.2f} | Size: {position_size:.5f} Lots | SL: ${state['sl_price']:,.2f}")
    return state

# =====================================================================
# 🔄 2-MINUTE STABLE SELF-PING WAKE-LOCK MATRIX FOR RENDER
# =====================================================================
last_self_ping_time = time.time()
RENDER_APP_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:10000")

def run_self_ping_heartbeat():
    """Render Web Service external URL ping structure logic loop"""
    global last_self_ping_time
    current_time = time.time()
    
    # 120 Seconds = Strictly 2 Minutes
    if current_time - last_self_ping_time >= 120:
        log_action("💓 [SELF-PING HEARTBEAT] Sending keepalive ping request frame hook...")
        try:
            # Render instance public node itself checks inside incoming routing tables
            requests.get(RENDER_APP_URL, timeout=5)
            log_action("✅ Anti-sleep wake lock refreshed successfully.")
        except Exception as e:
            log_action(f"⚠️ Self-ping network bounce notice: {e}")
        last_self_ping_time = current_time

def stream_terminal_output():
    run_self_ping_heartbeat() # Injected directly inside execution tracking logs
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, 'r', encoding='utf-8') as f:
                return "".join(f.readlines()[-28:])
        except:
            pass
    return "Delta Exchange Futures Stream Connected..."

# Pre-bootstrap history fetch
bootstrap_renko_state_from_history()

with gr.Blocks(title="Delta Futures Accurate Station") as demo:
    gr.Markdown("# 📺 Real-Time Delta Exchange Pure Price Action Reversal Station (Strict 1-Hour Timeframe)")
    gr.Markdown("Architecture: Seeding & Live Stream via 100% Accurate Delta Futures Public API Nodes Contract Cluster")
    console_terminal = gr.TextArea(label="Telemetry Streams", lines=20, max_lines=24, interactive=False)
    
    anti_idle_clock = gr.Timer(value=10)
    anti_idle_clock.tick(execute_live_trading_tick, outputs=console_terminal)
    demo.load(stream_terminal_output, outputs=console_terminal)

if __name__ == "__main__":
    # Render maps standard external port to 10000 by default deployment configurations
    demo.queue().launch(server_name="0.0.0.0", server_port=10000, share=False)
