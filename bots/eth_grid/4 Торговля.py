import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from pybit.unified_trading import HTTP
import argparse
import sys

# Добавляем путь к корню проекта, чтобы видеть account_state.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from account_state import update_after_position_close, check_entry_allowed

# === Аргументы и конфиг ===
def load_config():
    BOT_NAME = "eth_grid"  # жёстко заданное имя стратегии
    CONFIG_PATH = os.path.join("configs", f"{BOT_NAME}.json")
    with open(CONFIG_PATH) as f:
        return json.load(f)


config = load_config()  # сначала загружаем конфиг

SYMBOL = config["symbol"].upper()
CATEGORY = config["category"]
symbol_lower = SYMBOL.lower()

# === Пути ===
ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", "default")
BOT_NAME = os.environ.get("BOT_NAME", config.get("bot_name", "grid_bot"))

ACCOUNT_PATH = os.path.join("accounts", ACCOUNT_NAME, BOT_NAME)
DATA_PATH = os.path.join(ACCOUNT_PATH, "data")
os.makedirs(DATA_PATH, exist_ok=True)

SHARED_DATA_PATH = os.path.join("strategy_data", BOT_NAME)
os.makedirs(SHARED_DATA_PATH, exist_ok=True)

SHARED_DATA_PATH = os.path.join("strategy_data", BOT_NAME)
FEATURES_PATH = os.path.join(SHARED_DATA_PATH, "features.parquet")

EXECUTIONS_PATH = os.path.join(DATA_PATH, "executions.parquet")


# === Параметры стратегии (общие и специфичные) ===
def get_param(key, default):
    return json.loads(os.environ.get("BOT_PARAMS", "{}")).get(key, default)

PROFIT_TARGET = get_param("profit_target", 0.006)
GRID_SIZE = get_param("grid_size", 10)
GRID_DISTANCE = get_param("grid_distance", 0.006)
OFFSET = get_param("offset", 0.0001)
CAPITAL_PERCENT = get_param("capital_percent", 1)
MARTINGALE_STEP = get_param("martingale_step", 1.1)
MARTINGALE_ORDER = get_param("martingale_order", 1.2)
MAX_RANGE = get_param("max_range", 0.10)
TP_UPDATE_COOLDOWN = get_param("tp_update_cooldown", 120)
REORDER_THRESHOLD = get_param("reorder_threshold", 0.002)
SIZE_MULTIPLIER = get_param("size_multiplier", 1.05)

# === Состояние ===
last_grid_time = None
last_tp_update_time = datetime.utcnow() - timedelta(seconds=9999)
first_grid_price = None

def load_keys():
    return os.environ.get("BYBIT_API_KEY"), os.environ.get("BYBIT_API_SECRET")

def get_wallet_balance_usdt(session):
    try:
        wallet = session.get_wallet_balance(accountType="UNIFIED")["result"]["list"][0]
        return float(wallet["totalEquity"])
    except Exception as e:
        print(f"{datetime.utcnow()} ⚠️ Ошибка получения баланса: {e}")
        return 0.0

def get_position_idx(position_mode: str, side: str) -> int:
    position_mode = position_mode.lower()
    side = side.lower()
    if position_mode == "hedge":
        return 1 if side == "buy" else 2
    return 0

def get_qty_precision_from_exchange(session):
    try:
        response = session.get_instruments_info(category=CATEGORY, symbol=SYMBOL)
        step_size_str = response["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
        if '.' in step_size_str:
            return len(step_size_str.rstrip('0').split('.')[1])
        else:
            return 0
    except Exception as e:
        print(f"❌ Не удалось получить шаг qty с биржи: {e}")
        return 3



def get_last_row_from_features():
    if not os.path.exists(FEATURES_PATH):
        print(f"{datetime.utcnow()} ❌ Файл не найден: {FEATURES_PATH}")
        return None
    df = pd.read_parquet(FEATURES_PATH)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df.sort_values("ts", inplace=True)
    if df.empty:
        return None
    return df.iloc[-1].copy()


def get_last_row_from_executions():
    if not os.path.exists(EXECUTIONS_PATH):
        print(f"{datetime.utcnow()} ❌ Файл не найден: {EXECUTIONS_PATH}")
        return None
    df = pd.read_parquet(EXECUTIONS_PATH)
    df.index = pd.to_datetime(df.index, utc=True)  # <- здесь
    df.sort_index(inplace=True)
    if df.empty:
        return None
    row = df.iloc[-1].copy()
    row["ts"] = df.index[-1]  # <- возвращаем ts как колонку
    return row



def cancel_all_orders(session):
    session.cancel_all_orders(category=CATEGORY, symbol=SYMBOL)
    print(f"{datetime.utcnow()} 🚫 Отменены все ордера")

def cancel_all_tp_orders(session):
    orders = session.get_open_orders(category=CATEGORY, symbol=SYMBOL)
    for o in orders.get("result", {}).get("list", []):
        if o.get("side") == "Sell":
            session.cancel_order(category=CATEGORY, symbol=SYMBOL, orderId=o["orderId"])
    print(f"{datetime.utcnow()} 🚫 Отменены все TP-ордера")

def place_grid_orders(session, mark_price, qty_precision):
    raw_balance = get_wallet_balance_usdt(session)
    capital_usd = raw_balance * CAPITAL_PERCENT
    n = GRID_SIZE

    # === Расчёт шагов по цене (мартингейл-шаги) ===
    first_step = mark_price * GRID_DISTANCE
    raw_steps = [MARTINGALE_STEP ** i for i in range(n - 1)]
    remaining_range = mark_price * MAX_RANGE - first_step
    scale_steps = remaining_range / sum(raw_steps)
    steps = [first_step] + [s * scale_steps for s in raw_steps]

    # === Цены ордеров ===
    prices = []
    base_price = mark_price * (1 - OFFSET)
    price = base_price
    for step in steps:
        price -= step
        prices.append(round(price, 4))

    # === Размеры ордеров (мартингейл) ===
    raw_sizes = [MARTINGALE_ORDER ** i for i in range(n)]
    notionals = [s * p for s, p in zip(raw_sizes, prices)]
    total_notional = sum(notionals)
    scale_sizes = capital_usd / total_notional
    final_sizes = [s * scale_sizes for s in raw_sizes]

    position_mode = config.get("position_mode", "oneway").lower()

    # === Выставляем ордера ===
    for i, (p, sz) in enumerate(zip(prices, final_sizes), start=1):
        qty = round(sz / p, qty_precision)

        if qty * p < 5:  # проверка min order value
            print(f"{datetime.utcnow()} ⚠️ Пропущен ордер {i}: {qty} @ {p} — сумма {qty * p:.2f} < 5 USDT")
            return  # полностью выходим из функции, не ставим сетку

        order_kwargs = dict(
            category=CATEGORY,
            symbol=SYMBOL,
            side="Buy",
            orderType="Limit",
            qty=qty,
            price=p,
            timeInForce="GTC"
        )
        if position_mode == "hedge":
            order_kwargs["positionIdx"] = 1

        session.place_order(**order_kwargs)
        print(f"{datetime.utcnow()} ⛓ Ордер {i}: {qty} @ {p}")




def update_take_profit(session, avg_price, size, qty_precision):
    tp_price = round(avg_price * (1 + PROFIT_TARGET), 4)
    qty = round(size, qty_precision)
    position_mode = config.get("position_mode", "oneway").lower()

    order_kwargs = dict(
        category=CATEGORY,
        symbol=SYMBOL,
        side="Sell",
        orderType="Limit",
        qty=qty,
        price=tp_price,
        timeInForce="GTC"
    )

    if position_mode == "hedge":
        order_kwargs["positionIdx"] = 1

    session.place_order(**order_kwargs)
    print(f"{datetime.utcnow()} 🎯 Обновлён TP: {qty} @ {tp_price}")

def close_position_and_exit(session, position_size, qty_precision):
    qty = round(abs(position_size), qty_precision)
    try:
        session.place_order(
            category=CATEGORY,
            symbol=SYMBOL,
            side="Buy",  # закрываем шорт
            orderType="Market",
            qty=qty,
            timeInForce="IOC"
        )
        print(f"{datetime.utcnow()} 🛑 Обнаружен шорт! Закрываем {qty} по рынку и выходим...")
    except Exception as e:
        print(f"{datetime.utcnow()} ❌ Ошибка при закрытии шорта: {e}")

    # ждём, пока позиция обнулится
    while True:
        pos = session.get_positions(category=CATEGORY, symbol=SYMBOL)
        p = pos["result"]["list"][0]
        size = float(p.get("size", 0))
        side = p.get("side", "")
        if size == 0 or side != "Sell":
            break
        time.sleep(1)

    print(f"{datetime.utcnow()} ✅ Позиция обнулена, скрипт остановлен")
    sys.exit(1)


def wait_until_next_minute():
    now = datetime.utcnow()
    next_minute = (now + timedelta(minutes=1)).replace(second=5, microsecond=0)
    time.sleep((next_minute - now).total_seconds())

# === Основной цикл торговли ===
def main():
    global last_grid_time, last_tp_update_time, first_grid_price

    api_key, api_secret = load_keys()
    session = HTTP(testnet=False, api_key=api_key, api_secret=api_secret)
    qty_precision = get_qty_precision_from_exchange(session)

    print(f"=== Запуск торговли в {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} ===")
    print(f"📁 SYMBOL = {SYMBOL} | EXECUTIONS_PATH = {EXECUTIONS_PATH} | FEATURES_PATH = {FEATURES_PATH}")
    print(f"{datetime.utcnow()} 🚀 Бот запущен. Цикл каждую минуту.")

    last_ts = None
    active = False
    last_position_size = 0
    last_avg_price = 0
    last_order_count = 0

    while True:
        row_feat = get_last_row_from_features()
        row_exec = get_last_row_from_executions()

        if row_feat is None or row_exec is None:
            print(f"{datetime.utcnow()} ❌ Не удалось прочитать данные из features или executions")
            wait_until_next_minute()
            continue

        # берём размер и сторону
        raw_size = float(row_exec["position_size"])
        side = row_exec.get("side", "")

        # если шорт → закрываем и выходим
        if side == "Sell" and raw_size > 0:
            close_position_and_exit(session, raw_size, qty_precision)

        # считаем подписанный размер
        position_size = raw_size if side == "Buy" else -raw_size
        avg_price = row_exec["avg_price"]
        order_count = row_exec["order_count"]
        position_open = (position_size != 0)

        ts = pd.to_datetime(row_feat["ts"], utc=True)
        signal = int(row_feat["signal"])
        mark_price = float(row_exec["mark_price"])

        print(f"{datetime.utcnow()} 🕒 ts = {ts} | signal = {signal} | open = {position_open} | "
              f"size = {position_size} | side = {side} | orders = {order_count} | "
              f"price = {mark_price} | last_size = {last_position_size}")



        if not active and position_size == 0 and order_count > 0:
            print(f"{datetime.utcnow()} 🔁 Первый запуск: нет позиции, но есть ордера — отменяем всё")
            cancel_all_orders(session)
            wait_until_next_minute()
            continue

        if not position_open and signal == 1 and position_size == 0 and order_count == 0:
            if not check_entry_allowed(ACCOUNT_PATH):
                print("🚫 user_balance < 0 — новые входы запрещены")
                wait_until_next_minute()
                continue

            if last_grid_time and (datetime.utcnow() - last_grid_time) < timedelta(minutes=2):
                print(f"{datetime.utcnow()} ⏳ Прошло меньше 2 минут после выставления предыдущей сетки — ждём...")
                wait_until_next_minute()
                continue

            print(f"{datetime.utcnow()} 🚀 Вход по сигналу — выставляем сетку")
            cancel_all_orders(session)
            place_grid_orders(session, mark_price, qty_precision)
            first_grid_price = round(mark_price * (1 - OFFSET), 4)
            last_grid_time = datetime.utcnow()
            active = True
            wait_until_next_minute()
            continue

        if position_size > 0 and last_position_size == 0:
            cancel_all_tp_orders(session)
            print(f"{datetime.utcnow()} ✅ Позиция открыта — устанавливаем TP")
            update_take_profit(session, avg_price, position_size, qty_precision)
            last_position_size = position_size
            last_tp_update_time = datetime.utcnow()
            wait_until_next_minute()
            continue

        if position_size > 0 and position_size != last_position_size:
            seconds_since_tp = (datetime.utcnow() - last_tp_update_time).total_seconds()
            if seconds_since_tp < TP_UPDATE_COOLDOWN:
                print(f"{datetime.utcnow()} ⏳ TP недавно обновлялся ({int(seconds_since_tp)} сек назад) — ждём...")
                wait_until_next_minute()
                continue

            print(f"{datetime.utcnow()} 🔄 Размер позиции изменился ({last_position_size} → {position_size}) — обновляем TP")
            cancel_all_tp_orders(session)
            update_take_profit(session, avg_price, position_size, qty_precision)
            last_position_size = position_size
            last_tp_update_time = datetime.utcnow()
            wait_until_next_minute()
            continue

        if position_size == 0 and last_position_size > 0:
            print(f"{datetime.utcnow()} 📤 Позиция закрыта — обновляем виртуальный счёт и отменяем ордера")
            balance = get_wallet_balance_usdt(session)
            update_after_position_close(ACCOUNT_PATH, balance)
            cancel_all_orders(session)
            last_position_size = 0
            active = False
            wait_until_next_minute()
            continue

        print(f"{datetime.utcnow()} 🟡 Ничего не изменилось — ждём следующую минуту...")

        if active and last_grid_time and (datetime.utcnow() - last_grid_time).total_seconds() > 180:
            if position_size == 0 and order_count > 0:
                threshold_price = first_grid_price * (1 + REORDER_THRESHOLD)
                print(f"{datetime.utcnow()} 🔍 Проверка перестановки сетки: mark_price = {mark_price}, first_grid_price = {first_grid_price}, threshold = {threshold_price}")
                if mark_price > threshold_price:
                    elapsed_time = (datetime.utcnow() - last_grid_time).total_seconds()
                    price_growth_percent = ((mark_price / first_grid_price) - 1) * 100
                    print(f"{datetime.utcnow()} 📈 Цена ушла вверх на {price_growth_percent:.2f}% за {elapsed_time:.0f} сек — отменяем сетку")
                    cancel_all_orders(session)
                    active = False
                    last_grid_time = None
                    first_grid_price = None
                    wait_until_next_minute()
                    continue

        last_ts = ts
        last_position_size = position_size
        last_avg_price = avg_price
        last_order_count = order_count

        wait_until_next_minute()

if __name__ == "__main__":
    main()

