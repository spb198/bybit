import os
import time
import json
import argparse
import pandas as pd
from datetime import datetime
from pybit.unified_trading import HTTP

def load_config():
    BOT_NAME = "xrp_grid"  # жёстко заданное имя стратегии
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
LIMIT = 1000 if not os.path.exists(FILE_1MIN) or os.path.getsize(FILE_1MIN) < 10000 else 1

def fetch_new_candles(start_time=None):
    session = HTTP(testnet=False)
    try:
        params = {
            "category": CATEGORY,
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "limit": LIMIT,
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

    if existing.empty or len(existing) < 10000:
        print(f"⚠️ Недостаточно данных ({len(existing)} строк) — загружаем ~10 000 свечей...")
        combined = pd.DataFrame()
        session = HTTP(testnet=False)
        now = datetime.utcnow()
        start = now - pd.Timedelta(minutes=10500)
        start_ts = int(start.timestamp() * 1000)
        while len(combined) < 10000:
            df = fetch_new_candles(datetime.utcfromtimestamp(start_ts / 1000))
            if df.empty:
                print("❌ Прекращаем — API не вернул данных.")
                break
            df = df[~df.index.duplicated(keep='last')]
            df = df[~df.index.isin(combined.index)]
            combined = pd.concat([combined, df])
            combined.sort_index(inplace=True)
            if not df.empty:
                last_ts = df.index[-1]
                start_ts = int(last_ts.timestamp() * 1000) + 60_000
            time.sleep(0.2)
        safe_save_parquet(combined, FILE_1MIN)
        print(f"✅ Загружено {len(combined)} строк.")
    else:
        last_time = existing.index.max()
        fetch_from = last_time + pd.Timedelta(minutes=1)
        print(f"📌 Последний timestamp в базе: {last_time}")
        new_data = fetch_new_candles(fetch_from)
        if new_data.empty:
            print("⚠️ Нет новых данных.")
            return
        new_data.index = pd.to_datetime(new_data.index, utc=True)
        new_data = new_data[~new_data.index.duplicated(keep='last')]
        new_data.sort_index(inplace=True)
        existing_index = existing.index if not existing.empty else pd.DatetimeIndex([])
        new_data = new_data[~new_data.index.isin(existing_index)]
        if new_data.empty:
            print("⚠️ Нет новых уникальных свечей.")
            return
        combined = pd.concat([existing, new_data])
        combined = combined.sort_index()
        safe_save_parquet(combined, FILE_1MIN)
        print(f"✅ Добавлено {len(new_data)} свечей. Всего: {len(combined)}")

    print(f"📈 Последняя свеча:\n{combined.tail(1)}")
    aggregate_and_save(combined, '5min', FILE_5MIN)
    aggregate_and_save(combined, '30min', FILE_30MIN)
    aggregate_and_save(combined, '1h', FILE_1H)

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
