import subprocess
import sys
import os
import time
import json
import signal
import psutil

ACCOUNTS_FILE = "accounts.json"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

processes = {}  # {key: process}
own_pids = set()
restart_timestamps = {}
RESTART_DELAY = 60  # сек

def shutdown():
    print("\n🛑 Завершаем все процессы...")
    for pid in own_pids:
        try:
            psutil.Process(pid).terminate()
        except Exception:
            pass
    sys.exit(0)

signal.signal(signal.SIGINT, lambda sig, frame: shutdown())
signal.signal(signal.SIGTERM, lambda sig, frame: shutdown())

# === Загрузка аккаунтов ===
with open(ACCOUNTS_FILE) as f:
    accounts = json.load(f)

# === Запуск мониторинга памяти ===
try:
    subprocess.Popen([sys.executable, "memory_watcher.py"])
    print("🧠 memory_watcher.py запущен для контроля памяти процессов")
except Exception as e:
    print(f"⚠️ Не удалось запустить memory_watcher.py: {e}")

# === Старт глобальных скриптов по каждому bot_name ===
launched_global = set()

for acc in accounts:
    for bot_name in acc.get("bots", {}):
        for global_script in ["1 Формирование базы.py", "2 Подготовка данных к тесту.py"]:
            key = f"global:{bot_name}:{global_script}"
            script_path = os.path.join("bots", bot_name, global_script)
            log_path = os.path.join(LOG_DIR, f"{bot_name}_{global_script.replace(' ', '_')}.log")
            if key not in processes or processes[key].poll() is not None:
                now = time.time()
                if now - restart_timestamps.get(key, 0) < RESTART_DELAY:
                    print(f"⏳ Пропущен перезапуск {key} (ждём {RESTART_DELAY} сек)")
                    continue
                restart_timestamps[key] = now
                print(f"🚀 Запуск глобального скрипта: {global_script} для {bot_name}")
                log_file = open(log_path, "a", encoding="utf-8")
                log_file.write(f"\n=== Запуск {global_script} ({bot_name}) в {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                log_file.flush()
                proc = subprocess.Popen(
                    [sys.executable, script_path],
                    stdout=log_file,
                    stderr=log_file
                )
                processes[key] = proc
                own_pids.add(proc.pid)

i = 0

while True:
    now = time.time()

    for acc in accounts:
        name = acc["name"]
        api_key = acc["api_key"]
        api_secret = acc["api_secret"]
        symbol = acc.get("symbol", "XRPUSDT").upper()
        bots = acc.get("bots", {})

        for bot_name, params in bots.items():
            account_path = os.path.join("accounts", name, bot_name)
            data_path = os.path.join(account_path, "data")
            logs_path = os.path.join(account_path, "logs")
            os.makedirs(data_path, exist_ok=True)
            os.makedirs(logs_path, exist_ok=True)

            for script_name in ["3 Запись позиций.py", "4 Торговля.py"]:
                key = f"{name}:{bot_name}:{script_name}"
                should_start = False

                if key not in processes:
                    should_start = True
                else:
                    proc = processes[key]
                    exit_code = proc.poll()
                    if exit_code is not None:
                        print(f"⚠️ Процесс {key} завершился с кодом {exit_code}")
                        if now - restart_timestamps.get(key, 0) > RESTART_DELAY:
                            should_start = True
                        else:
                            print(f"⏳ Пропущен перезапуск {key} (ждём {RESTART_DELAY} сек)")

                if should_start:
                    restart_timestamps[key] = now
                    print(f"🚀 Запуск: {script_name} для {name} [{bot_name}]")
                    log_file_path = os.path.join(logs_path, script_name.replace(" ", "_") + ".log")
                    log_file = open(log_file_path, "a", encoding="utf-8")
                    log_file.write(f"\n=== Запуск {script_name} в {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                    log_file.flush()

                    env = os.environ.copy()
                    env["BYBIT_API_KEY"] = api_key
                    env["BYBIT_API_SECRET"] = api_secret
                    env["ACCOUNT_NAME"] = name
                    env["ACCOUNT_PATH"] = account_path
                    env["BOT_NAME"] = bot_name
                    env["BOT_PARAMS"] = json.dumps(params)

                    script_path = os.path.join("bots", bot_name, script_name)

                    proc = subprocess.Popen(
                        [sys.executable, script_path, "--symbol", symbol],
                        stdout=log_file,
                        stderr=log_file,
                        env=env
                    )
                    processes[key] = proc
                    own_pids.add(proc.pid)

    # 🔁 Проверка глобальных снова (на случай падений)
    for key, proc in list(processes.items()):
        if key.startswith("global:") and proc.poll() is not None:
            bot_name, script_name = key.split(":")[1:]
            print(f"⚠️ Глобальный скрипт {script_name} для {bot_name} завершился")
            del processes[key]  # чтобы на следующей итерации он запустился снова

    if i % 3 == 0:
        alive = [k for k, p in processes.items() if p.poll() is None]
        print(f"🟢 Активные: {', '.join(alive)}")

    i += 1
    time.sleep(10)
