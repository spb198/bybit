import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from pybit.unified_trading import HTTP

# === Константы ===
CONFIG_FILE = os.path.join("configs", "xrp_grid.json")


LOG_FILE = "log.txt"

# === Переменные окружения ===
ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", "default")
BOT_NAME = os.environ.get("BOT_NAME", "grid_bot")
ACCOUNT_PATH = os.environ.get("ACCOUNT_PATH", os.path.join("accounts", ACCOUNT_NAME, BOT_NAME))
DATA_PATH = os.path.join(ACCOUNT_PATH, "data")
LOG_PATH = os.path.join(ACCOUNT_PATH, "logs")
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

EXECUTIONS_FILE = os.path.join(DATA_PATH, "executions.parquet")
LOG_FILE = os.path.join(LOG_PATH, "3_Запись_позиций.py.log")

# === Загрузка конфигурации ===
with open(CONFIG_FILE) as f:
    config = json.load(f)

SYMBOL = config["symbol"].upper()
CATEGORY = config.get("category", "linear")

# === Вспомогательные функции ===
def safe_float(value):
    try:
        return float(value)
    except:
        return 0.0

def log_message(message):
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def load_keys():
    return os.environ.get("BYBIT_API_KEY"), os.environ.get("BYBIT_API_SECRET")

def get_position(session):
    try:
        result = session.get_positions(category=CATEGORY, symbol=SYMBOL)
        positions = result.get("result", {}).get("list", [])
        pos_data = {"size": 0.0, "avg_price": 0.0, "side": "", "mark_price": 0.0}
        for pos in positions:
            pos_data["size"] = safe_float(pos.get("size"))
            pos_data["avg_price"] = safe_float(pos.get("avgPrice"))
            pos_data["side"] = pos.get("side", "")
            break  # берём только первую позицию

        # берём mark_price напрямую из get_tickers
        ticker = session.get_tickers(category=CATEGORY, symbol=SYMBOL)
        pos_data["mark_price"] = safe_float(ticker["result"]["list"][0]["markPrice"])

        return pos_data

    except Exception as e:
        log_message(f"❌ Ошибка при получении позиции: {e}")
        return {"size": 0.0, "avg_price": 0.0, "side": "", "mark_price": 0.0}

def get_open_orders(session):
    try:
        result = session.get_open_orders(category=CATEGORY, symbol=SYMBOL)
        orders = result.get("result", {}).get("list", [])
        prices = [safe_float(o["price"]) for o in orders]
        sizes = [safe_float(o["qty"]) for o in orders]
        return prices, sizes, len(orders)
    except Exception as e:
        log_message(f"❌ Ошибка при получении ордеров: {e}")
        return [], [], 0

def update_executions_table(ts: datetime, session):
    pos = get_position(session)
    prices, sizes, order_count = get_open_orders(session)

    if pos["size"] == 0.0 and order_count > 0:
        log_message("⚠️ Подозрение на сбой данных — пробуем переподключиться через 2 сек...")
        time.sleep(2)
        pos = get_position(session)
        prices, sizes, order_count = get_open_orders(session)

    record = {
        "ts": ts,
        "position_size": pos["size"],
        "avg_price": pos["avg_price"],
        "side": pos["side"],
        "mark_price": pos["mark_price"],
        "position_open": pos["size"] > 0,
        "order_count": order_count,
        "order_prices": json.dumps(prices),
        "order_sizes": json.dumps(sizes)
    }

    df_new = pd.DataFrame([record]).set_index("ts")

    if os.path.exists(EXECUTIONS_FILE):
        df_old = pd.read_parquet(EXECUTIONS_FILE)
        df = pd.concat([df_old, df_new])
        df = df[~df.index.duplicated(keep="last")]
    else:
        df = df_new

    df.sort_index(inplace=True)
    df.to_parquet(EXECUTIONS_FILE)

    log_message(f"[{ts}] ✅ Обновлено\n"
                f"  position_size = {pos['size']}\n"
                f"  avg_price     = {pos['avg_price']}\n"
                f"  side          = {pos['side']}\n"
                f"  mark_price    = {pos['mark_price']}\n"
                f"  position_open = {pos['size'] > 0}\n"
                f"  order_count   = {order_count}\n"
                f"  order_prices  = {prices}\n"
                f"  order_sizes   = {sizes}")

def wait_until_next_minute():
    now = datetime.utcnow()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    time.sleep((next_minute - now).total_seconds())

def main_loop():
    api_key, api_secret = load_keys()
    session = HTTP(testnet=False, api_key=api_key, api_secret=api_secret)

    log_message(f"=== Запуск записи позиций для {SYMBOL} в {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} ===")

    while True:
        now = datetime.utcnow().replace(second=0, microsecond=0)
        try:
            update_executions_table(now, session)
        except Exception as e:
            log_message(f"❌ Общая ошибка в цикле: {e}")
        wait_until_next_minute()

if __name__ == "__main__":
    main_loop()
