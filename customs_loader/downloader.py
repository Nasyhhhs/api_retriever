import asyncio
import aiohttp
import csv
import os
import time
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm_asyncio
from loguru import logger

BASE_URL = "http://5.159.103.79:4000"
OUTPUT_FILE = "customs_data.csv"
PER_PAGE = 200

MAX_REQUESTS_PER_SECOND = 40

# Очередь для потоковой записи на диск
data_queue = asyncio.Queue(maxsize=500)
rate_limiter = AsyncLimiter(MAX_REQUESTS_PER_SECOND, 1.0)


async def download_all_pages_async():
    """Изолированный воркер для записи на диск"""
    is_empty = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0
    with open(OUTPUT_FILE, mode='a', newline='', encoding='utf-8') as file:
        writer = None
        while True:
            batch = await data_queue.get()
            if batch is None:
                data_queue.task_done()
                break
            if batch:
                if writer is None:
                    # Берем заголовки из первого словаря первого элемента
                    headers = list(batch[0].keys())
                    writer = csv.DictWriter(file, fieldnames=headers, delimiter='\t')
                    if is_empty:
                        writer.writeheader()
                        is_empty = False
                writer.writerows(batch)
            data_queue.task_done()


async def fetch_page(session, page, pbar, max_retries=10):
    url = f"{BASE_URL}/api/v1/logs"
    params = {'page': page, 'per_page': PER_PAGE}

    for attempt in range(max_retries):
        async with rate_limiter:
            try:
                async with session.get(url, params=params, timeout=60) as response:
                    if response.status == 200:
                        res_json = await response.json()
                        items = res_json.get('items', [])
                        if items:
                            await data_queue.put(items)
                        pbar.update(1)
                        return len(items)

                    elif response.status == 429:
                        logger.warning(f"Rate limit на странице {page}. Ждем 180с...")
                        # Снижаем планку RPS на 20% от текущей
                        new_rate = max(1, rate_limiter.max_rate * 0.8)
                        rate_limiter.max_rate = new_rate

                        logger.warning(
                            f"Сервер словил перезруз на странице {page}. Снижаем скорость до {new_rate:.1f} запр/сек. Спим 180с...")
                        await asyncio.sleep(180)
                        continue
                    else:
                        logger.error(f"Ошибка {response.status} на странице {page}. Повтор через 5 сек...")
                        await asyncio.sleep(5)

            except Exception as e:
                logger.error(f" Ошибка сети на странице {page}: {repr(e)}")
                await asyncio.sleep(5)

    logger.error(f"Страница {page} упала")
    pbar.update(1)
    return 0


async def main():
    if os.path.exists(OUTPUT_FILE):
        try:
            os.remove(OUTPUT_FILE)
            logger.info("Старый файл успешно удален.")
        except PermissionError:
            logger.warning(f"Файл {OUTPUT_FILE} занят системой! Данные будут дозаписаны.")

    # Получаем кол-во записей в базе
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BASE_URL}/api/v1/logs", params={'page': 1, 'per_page': 1}, timeout=15) as resp:
                res = await resp.json()
                total_entries = res.get('totalEntries', 0)
        except Exception as e:
            logger.error(f"Не удалось связаться с API: {e}")
            return

    total_pages = (total_entries // PER_PAGE) + (1 if total_entries % PER_PAGE > 0 else 0)
    logger.info(f"Всего строк: {total_entries}. Страниц: {total_pages}")

    start_time = time.time()

    # Запускаем фоновую запись
    writer_task = asyncio.create_task(download_all_pages_async())

    # Настраиваем сессию, скорость регулирует AsyncLimiter
    connector = aiohttp.TCPConnector(limit=None)
    async with aiohttp.ClientSession(connector=connector) as session:
        pbar = tqdm_asyncio(total=total_pages, desc="Скачивание")

        # Создаем пул задач для всех страниц
        tasks = [fetch_page(session, page, pbar) for page in range(1, total_pages + 1)]

        results = await asyncio.gather(*tasks)
        total_rows = sum(results)
        pbar.close()

    # Закрываем файл
    await data_queue.put(None)
    await writer_task

    logger.info(f"Успех! Сохранено строк: {total_rows}")
    logger.info(f"Общее время: {(time.time() - start_time) / 60:.2f} мин.")

