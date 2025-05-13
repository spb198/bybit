import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from ta.momentum import RSIIndicator
from ta.trend import CCIIndicator

# === Загрузка конфигурации ===
def load_config():
    BOT_NAME = "doge_grid"  # жёстко заданное имя стратегии
    CONFIG_PATH = os.path.join("configs", f"{BOT_NAME}.json")
    with open(CONFIG_PATH) as f:
        return json.load(f)



config = load_config()

# === Переменные окружения и пути ===
ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", config.get("account_name", "default"))
BOT_NAME = os.environ.get("BOT_NAME", config.get("bot_name", "grid_bot"))
DATA_PATH = os.path.join("strategy_data", BOT_NAME)
os.makedirs(DATA_PATH, exist_ok=True)


SYMBOL = config.get("symbol", "BTCUSDT").upper()
symbol_lower = SYMBOL.lower()

FEATURES_PATH = os.path.join(DATA_PATH, "features.parquet")
PARQUET_5M = os.path.join(DATA_PATH, f"{symbol_lower}_5min.parquet")
PARQUET_30M = os.path.join(DATA_PATH, f"{symbol_lower}_30min.parquet")
PARQUET_1H = os.path.join(DATA_PATH, f"{symbol_lower}_1h.parquet")

# === Параметры ===
params = json.loads(os.environ.get("BOT_PARAMS", "{}"))
RSI5_WINDOW = params.get("rsi5_window", 14)
RSI30_WINDOW = params.get("rsi30_window", 30)
CCI_WINDOW = params.get("cci_window", 20)
RSI5_THRESHOLD = params.get("rsi5_threshold", 44)
RSI30_THRESHOLD = params.get("rsi30_threshold", 55)
CCI_THRESHOLD = params.get("cci_threshold", 100)
FULL_REBUILD = params.get("full_rebuild", False)

# === Загрузка parquet с колонкой ts
def load_data_from_parquet(file_path: str, limit: int = None):
    if not os.path.exists(file_path):
        print(f"⚠️ Файл не найден: {file_path}")
        return pd.DataFrame()
    df = pd.read_parquet(file_path)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.sort_values("ts", inplace=True)
    return df.tail(limit) if limit else df

# === Расчёт индикаторов
def calculate_features(df_5m, df_30m, df_1h):
    df_5m["rsi_5"] = RSIIndicator(df_5m["close"], window=RSI5_WINDOW).rsi()
    df_30m["rsi_30"] = RSIIndicator(df_30m["close"], window=RSI30_WINDOW).rsi()
    df_1h["cci_1h"] = CCIIndicator(df_1h["high"], df_1h["low"], df_1h["close"], window=CCI_WINDOW).cci()

    df = df_5m.copy()
    df["rsi_30"] = df_30m.set_index("ts")["rsi_30"].reindex(df["ts"], method="ffill").values
    df["cci_1h"] = df_1h.set_index("ts")["cci_1h"].reindex(df["ts"], method="ffill").values

    df["signal"] = (
        (df["rsi_5"] < RSI5_THRESHOLD) &
        (df["rsi_30"] < RSI30_THRESHOLD) &
        (df["cci_1h"] < CCI_THRESHOLD)
    ).astype(int)

    df["entry_trigger"] = (df["signal"].shift(1) == 0) & (df["signal"] == 1)
    df["entry_trigger"] = df["entry_trigger"].fillna(False)

    df = df[df[["rsi_5", "rsi_30", "cci_1h"]].notna().all(axis=1)]
    return df

# === Формирование признаков
def prepare_features(full: bool = False):
    df_5m = load_data_from_parquet(PARQUET_5M, limit=None if full else 1000)
    df_30m = load_data_from_parquet(PARQUET_30M, limit=None if full else 500)
    df_1h = load_data_from_parquet(PARQUET_1H, limit=None if full else 500)

    if df_5m.empty or df_30m.empty or df_1h.empty:
        print("🚫 Недостаточно данных для расчёта.")
        return

    df = calculate_features(df_5m, df_30m, df_1h)
    if df.empty:
        print("⚠️ Нет признаков после расчёта индикаторов.")
        return

    if os.path.exists(FEATURES_PATH) and not full:
        df_old = pd.read_parquet(FEATURES_PATH)
        df_old["ts"] = pd.to_datetime(df_old["ts"], utc=True)
        df_combined = pd.concat([df_old, df])
        df_combined = df_combined.drop_duplicates(subset="ts", keep="last").sort_values("ts")
    else:
        df_combined = df

    df_combined["ts"] = pd.to_datetime(df_combined["ts"], utc=True)
    df_combined.to_parquet(FEATURES_PATH, index=False)
    print(f"✅ Признаки сохранены: {len(df_combined)} строк в {FEATURES_PATH}")

# === Цикл обновления
def wait_until_next_minute_with_buffer(buffer_sec=5):
    now = datetime.utcnow()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    time.sleep((next_minute + timedelta(seconds=buffer_sec) - now).total_seconds())

def auto_update_loop(buffer_sec=5):
    print(f"🚀 Старт автообновления признаков (с буфером {buffer_sec} сек)...\n")
    while True:
        try:
            print(f"\n⏱️ Обновление признаков: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
            prepare_features(full=FULL_REBUILD)
        except Exception as e:
            print(f"❌ Ошибка: {e}")
        wait_until_next_minute_with_buffer(buffer_sec=buffer_sec)

if __name__ == "__main__":
    auto_update_loop(buffer_sec=5)
