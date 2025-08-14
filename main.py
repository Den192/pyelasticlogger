import asyncio
import aiohttp
import aiofiles
from pathlib import Path
import time
import json
import os
import socket
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import time as dtime
from dotenv import load_dotenv

# ---------------- Настройки ----------------
load_dotenv()

ELASTIC_URL_BASE = os.getenv("ELASTIC_URL_BASE")
LOG_FILES = [Path(p.strip()) for p in os.getenv("LOG_FILES", "").split(";") if p.strip()]
OFFSET_FILE = os.getenv("OFFSET_FILE", "offsets.json")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL"))
AGENT_LOG_FILE = Path(os.getenv("LOG_FILE", "agent.log"))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "7"))

HOSTNAME = socket.gethostname().lower()
INDEX_NAME = f"softcake-{HOSTNAME}"  # Тут индекс по хосту, но можно и по другому

# Логгируем
logger = logging.getLogger("LogAgent")
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(
    filename=AGENT_LOG_FILE,
    when="midnight",
    interval=1,
    backupCount=LOG_BACKUP_COUNT,
    encoding="utf-8",
    atTime=dtime(hour=6, minute=0)
)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# офсеты для грамотного чтения файла
def load_offsets():
    try:
        with open(OFFSET_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {str(p): {"pos": 0, "size": 0} for p in LOG_FILES}

def save_offsets(offsets):
    with open(OFFSET_FILE, "w") as f:
        json.dump(offsets, f)

async def ensure_index(session):
    index_url = f"{ELASTIC_URL_BASE}/{INDEX_NAME}"
    async with session.get(index_url) as resp:
        if resp.status == 200:
            logger.info(f"Index '{INDEX_NAME}' already exists.")
            return
    # создаём индекс с правильным mapping, а иначе вместо времени получаем херню полную
    mapping = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date", "format": "epoch_second"},
                "hostname": {"type": "keyword"},
                "logfile": {"type": "keyword"},
                "logpath": {"type": "keyword"},
                "message": {"type": "text"}
            }
        }
    }
    async with session.put(index_url, json=mapping) as resp:
        if resp.status in (200, 201):
            logger.info(f"Index '{INDEX_NAME}' created with mapping.")
        else:
            text = await resp.text()
            logger.error(f"Failed to create index: {resp.status} {text}")

async def send_to_elastic(session, log_data: dict):
    try:
        url = f"{ELASTIC_URL_BASE}/{INDEX_NAME}/_doc"
        async with session.post(url, json=log_data, timeout=5) as resp:
            if resp.status in (200, 201):
                return True
            else:
                text = await resp.text()
                logger.error(f"Elasticsearch returned {resp.status}: {text}")
                return False
    except Exception as e:
        logger.error(f"Send failed: {e}")
        return False

# Читаем логи тут, если ок - вызываем send_to_elastic
async def process_log_file(session, file_path: Path, offsets: dict):
    meta = offsets.get(str(file_path), {"pos": 0, "size": 0})
    last_pos = meta["pos"]
    last_size = meta["size"]

    if not file_path.exists():
        return

    current_size = os.path.getsize(file_path)
    if current_size < last_size:
        logger.info(f"Rotation detected for {file_path.name}, restarting from 0")
        last_pos = 0

    async with aiofiles.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        await f.seek(last_pos)
        lines = await f.readlines()
        if not lines:
            offsets[str(file_path)] = {"pos": last_pos, "size": current_size}
            return

        for line in lines:
            log_data = {
                "timestamp": int(time.time()),  # целые секунды
                "hostname": HOSTNAME,
                "logfile": file_path.name,
                "logpath": str(file_path.resolve()),
                "message": line.strip()
            }
            success = await send_to_elastic(session, log_data)
            if not success:
                logger.warning(f"Failed to send log from {file_path.name}, will retry later.")
                return

        offsets[str(file_path)] = {"pos": await f.tell(), "size": current_size}

# Цикл просмотра
async def watch_logs():
    offsets = load_offsets()
    logger.info(f"Agent started on {HOSTNAME} with index {INDEX_NAME}")

    async with aiohttp.ClientSession() as session:
        await ensure_index(session)  # проверяем и создаём индекс при старте
        while True:
            for log_file in LOG_FILES:
                await process_log_file(session, log_file, offsets)
            save_offsets(offsets)
            await asyncio.sleep(SEND_INTERVAL)

# Запуск
if __name__ == "__main__":
    try:
        asyncio.run(watch_logs())
    except Exception as e:
        logger.critical(f"Agent crashed: {e}")
