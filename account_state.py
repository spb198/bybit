import os
import json
from datetime import datetime

def load_account_state(account_path):
    path = os.path.join(account_path, "account_state.json")
    if not os.path.exists(path):
        return {
            "initial_balance": None,
            "last_balance": None,
            "user_balance": 0.0,
            "profit": 0.0,
            "commission_rate": 0.1,
            "next_allowed_entry": True,
            "last_updated": None
        }
    with open(path, "r") as f:
        return json.load(f)

def save_account_state(account_path, state):
    path = os.path.join(account_path, "account_state.json")
    with open(path, "w") as f:
        json.dump(state, f, indent=2)

def check_entry_allowed(account_path):
    state = load_account_state(account_path)
    return state.get("user_balance", 0.0) >= 0

def update_after_position_close(account_path, current_balance):
    state = load_account_state(account_path)
    now = datetime.utcnow().isoformat()

    if state["initial_balance"] is None:
        state["initial_balance"] = current_balance
        state["last_balance"] = current_balance
        state["user_balance"] = round(current_balance * 0.1, 2)  # стартовый виртуальный счёт = 10%
        state["last_updated"] = now
        save_account_state(account_path, state)
        return state

    last_balance = state.get("last_balance", current_balance)
    profit = current_balance - last_balance
    profit = round(profit, 2)

    if profit > 0:
        commission = round(profit * state.get("commission_rate", 0.1), 2)
        state["user_balance"] = round(state.get("user_balance", 0.0) - commission, 2)
        state["profit"] = round(state.get("profit", 0.0) + profit, 2)
    else:
        # Убыток не влияет на user_balance, только на накопленную прибыль
        state["profit"] = round(state.get("profit", 0.0) + profit, 2)

    state["last_balance"] = current_balance
    state["last_updated"] = now
    state["next_allowed_entry"] = state["user_balance"] >= 0

    save_account_state(account_path, state)
    return state
