import logging
import datetime as dt
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from selenium.webdriver.common.by import By
from seleniumbase import SB
from fake_useragent import UserAgent

# --- КОНФИГУРАЦИЯ ---
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "2028",
    "host": "localhost",
    "port": "5432"
}

# Настройка логирования в файл
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='avito_scraper.log'
)

class DatabaseManager:
    def __init__(self, config):
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(1, 10, **config)
            self._init_migrations_table() # Создаем таблицу для учета миграций
            self.apply_migrations()       # Запускаем сами миграции
        except Exception as e:
            logging.error(f"Could not initialize DB: {e}")
            raise

    @contextmanager
    def get_cursor(self):
        conn = self.pool.getconn()
        conn.autocommit = True
        try:
            yield conn.cursor()
        finally:
            self.pool.putconn(conn)

    def _init_migrations_table(self):
        """Создает служебную таблицу для хранения примененных миграций."""
        query = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.get_cursor() as cur:
            cur.execute(query)

    def apply_migrations(self):
        """Список всех изменений в базе данных."""
        # Ключ — номер версии, Значение — список SQL-команд
        migrations = {
            1: [ # Начальное создание таблиц
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    Date TIMESTAMP,
                    Users INTEGER,
                    Messages TEXT
                )""",
                """
                CREATE TABLE IF NOT EXISTS history (
                    id SERIAL PRIMARY KEY,
                    Date TIMESTAMP,
                    Users INTEGER
                )""",
                """
                CREATE TABLE IF NOT EXISTS apartmentsAvito (
                    id SERIAL PRIMARY KEY,
                    DateScan TIMESTAMP,
                    name TEXT,
                    url TEXT,
                    price TEXT,
                    requirement TEXT,
                    active BOOLEAN DEFAULT TRUE,
                    apartmentsID VARCHAR(50) UNIQUE
                )"""
            ],
            2: [ # Пример будущей миграции: добавим индекс для ускорения поиска
                "CREATE INDEX IF NOT EXISTS idx_apartments_id ON apartmentsAvito(apartmentsID)"
            ],
            # 3: ["ALTER TABLE apartmentsAvito ADD COLUMN city TEXT"] # Так можно добавлять колонки позже
        }

        with self.get_cursor() as cur:
            # Узнаем текущую версию БД
            cur.execute("SELECT MAX(version) FROM schema_migrations")
            result = cur.fetchone()
            current_version = result[0] if result[0] is not None else 0

            # Применяем миграции, которые еще не были запущены
            for version in sorted(migrations.keys()):
                if version > current_version:
                    logging.info(f"Applying migration version {version}...")
                    try:
                        for sql in migrations[version]:
                            cur.execute(sql)
                        cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
                        logging.info(f"Migration {version} applied successfully.")
                    except Exception as e:
                        logging.error(f"Error applying migration {version}: {e}")
                        raise

    def log_to_db(self, message, user_id=1):
        query = "INSERT INTO logs (Date, Users, Messages) VALUES (%s, %s, %s)"
        try:
            with self.get_cursor() as cur:
                cur.execute(query, (dt.datetime.now(), user_id, message))
        except Exception as e:
            logging.error(f"Failed to write log to DB: {e}")


db = DatabaseManager(DB_CONFIG)


def retry(retries=2, delay=1):
    """Декоратор для повторных попыток."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} failed in {func.__name__}: {e}")
                    if attempt == retries - 1:
                        db.log_to_db(f"Error in {func.__name__}: {str(e)}")
                        return None
                    time.sleep(current_delay)
                    current_delay *= 2
            return None

        return wrapper

    return decorator


def extract_case(page_source, marker):
    """Безопасный поиск маркеров в исходном коде."""
    try:
        start = page_source.find(marker)
        if start == -1:
            return None
        end = page_source.find('"', start)
        return page_source[start:end]
    except Exception as e:
        logging.error(f"findcase error: {e}")
        return None


@retry()
def save_items_to_db(items):
    """Сохранение найденных объектов в БД с проверкой на дубликаты."""
    if not items:
        return

    check_query = "SELECT apartmentsID FROM apartmentsAvito WHERE apartmentsID = %s"
    insert_query = """
        INSERT INTO apartmentsAvito (DateScan, name, url, price, requirement, active, apartmentsID)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    with db.get_cursor() as cur:
        for item in items:
            cur.execute(check_query, (item['apartmentsID'],))
            if not cur.fetchone():
                cur.execute(insert_query, (
                    dt.datetime.now(),
                    item['name'],
                    item['url'],
                    item['price'],
                    item['requirement'],
                    True,
                    item['apartmentsID']
                ))
    db.log_to_db(f"Saved {len(items)} items to database.")


def parse_elements(sb):
    """Парсинг текущей страницы."""
    items = []
    sb.sleep(10)  # Важно давать паузу
    # Используем более надежные селекторы Avito
    listings = sb.find_elements('div[data-marker="item"]')

    for listing in listings:
        try:
            # Используем вложенный поиск внутри элемента
            name_el = listing.find_element(By.CSS_SELECTOR, 'h2[itemprop="name"]')
            price_el = listing.find_element(By.CSS_SELECTOR, '[itemprop="price"]')
            req_el = listing.find_element(By.CSS_SELECTOR, '[data-marker="item-specific-params"]')

            items.append({
                'name': name_el.text,
                'url': name_el.get_attribute("href"),
                'price': price_el.get_attribute("content"),
                'requirement': req_el.text,
                'apartmentsID': listing.get_attribute("data-item-id")
            })
        except Exception:
            continue  # Пропускаем битые элементы
    return items


def collect_data_apartments():
    """Основная логика скрапинга."""
    try:
        # uc=True включает Anti-bot обход
        with SB(uc=True, headless=False) as sb:
            url = "https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok?metro=171-181&s=104"
            sb.open(url)

            # Обход "Main Filter" (если нужно)
            main_filter_class = extract_case(sb.get_page_source(), "form-mainFilters")
            if main_filter_class:
                selector = f"div.{main_filter_class} label"
                if sb.is_element_visible(selector):
                    sb.click(selector)

            # Нажимаем кнопку поиска
            if sb.is_element_visible('button[data-marker="search-filters/submit-button"]'):
                sb.click('button[data-marker="search-filters/submit-button"]')
                sb.sleep(5)

            # Цикл по страницам пагинации
            page = 1
            while page <= 5:  # Ограничим для примера 5 страницами
                logging.info(f"Parsing page {page}")
                items = parse_elements(sb)
                if items:
                    save_items_to_db(items)

                # Переход на следующую страницу
                next_button = 'a[data-marker="pagination-button/nextPage"]'
                if sb.is_element_visible(next_button):
                    sb.click(next_button)
                    sb.sleep(6)  # Важно давать паузу
                    page += 1
                else:
                    break

    except Exception as e:
        logging.critical(f"Scraper crashed: {e}")
        db.log_to_db(f"CRITICAL ERROR: {str(e)}")


if __name__ == '__main__':
    # Запись начала работы
    db = DatabaseManager(DB_CONFIG)
    with db.get_cursor() as cursor:
        cursor.execute("INSERT INTO history (Date, Users) VALUES (%s, %s)", (dt.datetime.now(), 1))

    collect_data_apartments()
