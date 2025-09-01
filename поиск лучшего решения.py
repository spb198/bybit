import pandas as pd
import numpy as np
import random
from itertools import product
from tqdm import tqdm

# === Пути ===
FEATURES_PARQUET = "strategy_data/bnb_grid/features.parquet"

# === Загрузка готовых признаков ===
df_combined = pd.read_parquet(FEATURES_PARQUET)
df_combined.index = pd.to_datetime(df_combined.index, utc=True)
df_combined.sort_index(inplace=True)
df_combined.dropna(inplace=True)

# === Бэктест функции ===
def backtest(df_combined, rsi5_thr, rsi30_thr, cci_thr, grid_size, grid_dist, profit_target, offset):
    initial_cash = 10000
    max_grid_capital = 10000

    cash = initial_cash
    position = 0
    entry_prices = []
    orders = []
    entry_triggered = False
    entry_time = None
    last_entry_price = None
    profit_history = []

    for current_time, row in df_combined.iterrows():
        price = row['close']
        rsi5 = row['rsi_5']
        rsi30 = row['rsi_30']
        cci = row['cci_1h']

        if not entry_triggered and rsi5 < rsi5_thr and rsi30 < rsi30_thr and cci < cci_thr:
            base_price = price * (1 - offset)
            orders = []
            total_value = 0
            size = (max_grid_capital / grid_size) / base_price

            for i in range(grid_size):
                order_price = base_price * (1 - i * grid_dist)
                order_value = order_price * size
                if total_value + order_value > max_grid_capital:
                    break
                orders.append({'price': order_price, 'size': size})
                total_value += order_value
                size *= 1.05

            if orders:
                entry_triggered = True
                entry_time = current_time
                last_entry_price = price
            continue

        if entry_triggered:
            for order in orders[:]:
                if price <= order['price']:
                    cost = order['price'] * order['size']
                    if cash >= cost:
                        position += order['size']
                        cash -= cost
                        entry_prices.append((order['price'], order['size']))
                        orders.remove(order)

            if price >= last_entry_price * (1 + 0.004):
                base_price = price * (1 - offset)
                orders = []
                total_value = 0
                size = (max_grid_capital / grid_size) / base_price

                for i in range(grid_size):
                    order_price = base_price * (1 - i * grid_dist)
                    order_value = order_price * size
                    if total_value + order_value > max_grid_capital:
                        break
                    orders.append({'price': order_price, 'size': size})
                    total_value += order_value
                    size *= 1.05

                last_entry_price = price

            if position > 0:
                total_size = sum(s for _, s in entry_prices)
                avg_price = sum(p * s for p, s in entry_prices) / total_size
                target_price = avg_price * (1 + profit_target)
                if price >= target_price:
                    cash += price * position
                    profit = price * position - sum(p * s for p, s in entry_prices)
                    profit_history.append(profit)

                    position = 0
                    entry_prices = []
                    orders = []
                    entry_triggered = False
                    entry_time = None

    if position > 0:
        total_size = sum(s for _, s in entry_prices)
        avg_price = sum(p * s for p, s in entry_prices) / total_size
        final_price = df_combined['close'].iloc[-1]
        cash += final_price * position
        profit = final_price * position - sum(p * s for p, s in entry_prices)
        profit_history.append(profit)

    return {
        "RSI5": rsi5_thr,
        "RSI30": rsi30_thr,
        "CCI": cci_thr,
        "GRID_SIZE": grid_size,
        "GRID_DISTANCE": grid_dist,
        "PROFIT_TARGET": profit_target,
        "OFFSET": offset,
        "Trades": len(profit_history),
        "ProfitNet": round(sum(profit_history), 4),
        "ActualCash": round(cash, 2)
    }

# === Параметры перебора ===
rsi5_range = range(40, 55, 5)     # 40, 42, 44, 46, 48, 50, 52
rsi30_range = range(40, 65, 5)    # 40, 45, 50, 55, 60
cci_range = range(-200, 150, 50)  # -200, -150, -100, -50, 0, 50, 100
grid_size_range = range(10, 31, 5)    # 5 до 16 уровней
grid_dist_range = [ 0.08]  # 0.1%, 0.2%, 0.4%, 0.6%
profit_target_range = [0.002, 0.004, 0.006, 0.008]
offset_range = [0.0001, 0.0005, 0.001]


# === Формирование всех возможных комбинаций
all_combinations = list(product(
    rsi5_range,
    rsi30_range,
    cci_range,
    grid_size_range,
    grid_dist_range,
    profit_target_range,
    offset_range
))

# === Random Search: берём случайные 3000 сетов
random.seed(42)
sample_size = min(3000, len(all_combinations))
sampled_params = random.sample(all_combinations, sample_size)

# === Поиск лучших параметров по прибыли
best_profit_result = None
results = []

for params in tqdm(sampled_params):
    result = backtest(df_combined, *params)
    results.append(result)

    if best_profit_result is None or result["ProfitNet"] > best_profit_result["ProfitNet"]:
        best_profit_result = result
        print(f"\n🔥 Новый лучший результат: ProfitNet = {best_profit_result['ProfitNet']} | Параметры: {best_profit_result}")

# === Сохранение всех результатов
results_df = pd.DataFrame(results)
results_df.to_csv("xrp_optimization_results_random.csv", index=False)

# === Вывод лучшего результата
print("\n🏁 Лучший результат по прибыли:")
print(best_profit_result)
