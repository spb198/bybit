import os
import time
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from datetime import datetime
from pybit.unified_trading import HTTP
from datetime import timezone




def load_config():
    BOT_NAME =  "sol_grid"   # жёстко заданное имя стратегии
    CONFIG_PATH = os.path.join("configs", f"{BOT_NAME}.json")
    with open(CONFIG_PATH) as f:
        return json.load(f)



parser = argparse.ArgumentParser()
parser.add_argument("--symbol", type=str, help="Override символ из config.json")
args = parser.parse_args()

config = load_config()
SYMBOL = args.symbol.upper() if args.symbol else config.get("symbol", "BTCUSDT").upper()
CATEGORY = config.get("category", "linear")
symbol_lower = SYMBOL.lower()

BOT_NAME = os.environ.get("BOT_NAME", config.get("bot_name", "grid_bot"))
DATA_PATH = os.path.join("strategy_data", BOT_NAME)
os.makedirs(DATA_PATH, exist_ok=True)

FILE_1MIN = os.path.join(DATA_PATH, f"{symbol_lower}_1min.parquet")
FILE_5MIN = os.path.join(DATA_PATH, f"{symbol_lower}_5min.parquet")
FILE_30MIN = os.path.join(DATA_PATH, f"{symbol_lower}_30min.parquet")
FILE_1H = os.path.join(DATA_PATH, f"{symbol_lower}_1h.parquet")

INTERVAL = "1"
LIMIT = 1000 if not os.path.exists(FILE_1MIN) or os.path.getsize(FILE_1MIN) < 100000 else 1

def fetch_new_candles(start_time=None, limit=1000):
    session = HTTP(testnet=False)
    try:
        params = {
            "category": CATEGORY,
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "limit": limit,
        }
        if start_time:
            params["start"] = int(start_time.timestamp() * 1000)

        response = session.get_kline(**params)
        if response.get("retMsg") != "OK":
            print(f"[{datetime.utcnow().isoformat()}] ❌ Ошибка от API:", response)
            return pd.DataFrame()

        data = response.get("result", {}).get("list", [])
        if not data:
            print(f"[{datetime.utcnow().isoformat()}] ⚠️ Пустой ответ от API.")
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume", "turnover"
        ])
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df["ts"] = pd.to_datetime(df["timestamp"].astype("int64"), unit='ms', utc=True)
        df.set_index("ts", inplace=True)
        df.drop(columns=["timestamp"], inplace=True)
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        return df
    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ❌ Ошибка при запросе:", e)
        return pd.DataFrame()

def safe_load_parquet(path):
    try:
        df = pd.read_parquet(path)
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
            df.set_index("ts", inplace=True)
        return df
    except Exception as e:
        print(f"⚠️ Ошибка при чтении {path}: {e}")
        if os.path.exists(path):
            os.remove(path)
        return pd.DataFrame()

def safe_save_parquet(df, path):
    tmp_file = path + ".tmp"
    df_to_save = df.copy().reset_index()
    df_to_save["ts"] = pd.to_datetime(df_to_save["ts"], utc=True)
    df_to_save.to_parquet(tmp_file, index=False)
    os.replace(tmp_file, path)

def aggregate_and_save(df, rule, output_file):
    agg_df = df.resample(rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    agg_df = agg_df.reset_index()
    agg_df["ts"] = pd.to_datetime(agg_df["ts"], utc=True)
    agg_df.to_parquet(output_file, index=False)
    print(f"📁 Сохранено: {output_file} ({len(agg_df)} строк)")

def update_parquet():
    print(f"\n🕒 [{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 Обновление данных для {SYMBOL}...")

    existing = safe_load_parquet(FILE_1MIN) if os.path.exists(FILE_1MIN) else pd.DataFrame()
    combined = existing.copy()
    now = datetime.utcnow().replace(tzinfo=timezone.utc, second=0, microsecond=0)

    if existing.empty:
        print("⚠️ База пуста — начинаем загрузку с нуля")
        last_time = now - timedelta(minutes=10000)
    else:
        last_time = existing.index.max()
        print(f"📌 Последний timestamp в базе: {last_time}")

    while last_time < now - timedelta(minutes=1):
        fetch_from = last_time + timedelta(minutes=1)
        # Определим лимит: если данные старые — берём пачку, если почти в реальном времени — берём по одной
        limit = 1000 if (now - fetch_from).total_seconds() > 180 else 1
        new_data = fetch_new_candles(fetch_from, limit=limit)

        if new_data.empty:
            print("❌ Нет новых данных от API — остановка загрузки.")
            break

        new_data.index = pd.to_datetime(new_data.index, utc=True)
        new_data = new_data[~new_data.index.duplicated(keep='last')]
        combined = pd.concat([combined, new_data])
        combined = combined[~combined.index.duplicated(keep='last')]
        combined.sort_index(inplace=True)

        last_time = combined.index.max()
        print(f"📈 Добавлено: {len(new_data)} свечей | Новый последний ts: {last_time}")

        time.sleep(0.25)

    safe_save_parquet(combined, FILE_1MIN)
    print(f"✅ Всего свечей после обновления: {len(combined)}")
    aggregate_and_save(combined, '5min', FILE_5MIN)
    aggregate_and_save(combined, '30min', FILE_30MIN)
    aggregate_and_save(combined, '1h', FILE_1H)

    print(f"📈 Последняя свеча:\n{combined.tail(1)}")


def sync_to_next_minute(start_time):
    elapsed = time.time() - start_time
    delay = 60 - (elapsed % 60)
    print(f"⏱️ Цикл завершён за {elapsed:.2f} сек. Ждём {delay:.2f} сек до следующего запуска.")
    if delay > 0:
        time.sleep(delay)

if __name__ == "__main__":
    try:
        while True:
            start_time = time.time()
            update_parquet()
            sync_to_next_minute(start_time)
    except KeyboardInterrupt:
        print("🛑 Остановка скрипта.")
