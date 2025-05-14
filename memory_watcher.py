import psutil
import time
import os

PROCESS_LIMIT_MB = 250       # Порог для одного процесса (опционально)
TOTAL_LIMIT_MB = 5000        # Общий лимит для всех Python-процессов
LOG_FILE = "logs/memory_watch.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def log(msg):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")
        f.flush()

def monitor_memory():
    print(f"🧠 Watchdog: следим за Python-процессами — общая граница {TOTAL_LIMIT_MB} MB")
    while True:
        processes = []
        total_mem = 0

        for proc in psutil.process_iter(['pid', 'name', 'memory_info', 'cmdline']):
            try:
                if 'python' in proc.info['name'].lower():
                    mem_mb = proc.info['memory_info'].rss / 1024 / 1024
                    cmd = ' '.join(proc.info['cmdline'])
                    processes.append((proc, mem_mb, cmd))
                    total_mem += mem_mb
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        log(f"📊 Всего Python-памяти: {total_mem:.2f} MB")

        # Логирование всех процессов
        for proc, mem_mb, cmd in processes:
            log(f"PID {proc.pid} | {mem_mb:.2f} MB | {cmd}")

        # Если суммарная память превысила лимит — убиваем самых тяжёлых
        if total_mem > TOTAL_LIMIT_MB:
            log(f"🚨 Память превышена ({total_mem:.2f} MB > {TOTAL_LIMIT_MB} MB) — начинаем сброс...")
            # Сортировка по убыванию
            for proc, mem_mb, cmd in sorted(processes, key=lambda x: -x[1]):
                try:
                    proc.kill()
                    log(f"💀 Убит PID {proc.pid} | {mem_mb:.2f} MB | {cmd}")
                    total_mem -= mem_mb
                    if total_mem <= TOTAL_LIMIT_MB:
                        log(f"✅ Память снижена до {total_mem:.2f} MB — остановка убийства")
                        break
                except Exception as e:
                    log(f"❌ Не удалось убить PID {proc.pid}: {e}")

        time.sleep(60)

if __name__ == "__main__":
    monitor_memory()
