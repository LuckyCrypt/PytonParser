import logging
import time
import json
import datetime as dt
from idlelib.autocomplete import TRY_A
import psycopg2
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumbase import SB
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='avito_scraper.log')

def retry(func, retries=3, delay=1):
    """Декоратор для повторных попыток выполнения функции."""
    def wrapper(*args, **kwargs):
        nonlocal delay  # Указываем, что delay - не локальная переменная
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Attempt {attempt+1} failed in {func.__name__}: {e}")
                time.sleep(delay)
                delay *= 2  # Экспоненциальная задержка
        logging.error(f"Function {func.__name__} failed after {retries} attempts.")
        return None  # Или raise исключение, если нужно
    return wrapper

def findcase(page_source, marker):
    """Находит подстроку, содержащую указанный маркер."""
    try:
        start = page_source.find(marker)
        if start == -1:
            return None  # Маркер не найден
        end = page_source.find("\"", start)  # Ищем ближайший пробел после маркера
        if end == -1:
            return None  # Если конец тега не найден, возвращаем ошибку
        return page_source[start:end]
    except Exception as e:
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + "', 1,\'Error in findcase: {"+marker+"} " + e + "\')"
        send_query_database(database_query)
        logging.error(f"Error in findcase: {e}")
        return None

@retry
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        logging.error("Connection to PostgreSQL DB successful")
    except Exception as e:
        logging.error(f"The error '{e}' occurred")
    return connection

@retry
def send_query_database(query):
    #Убрать константы !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    connection = create_connection(
        "postgres", "postgres", "2028", "localhost", "5432"
    )
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.close()
        logging.error("Query executed successfully" + query)
    except Exception as e:
        logging.error(f"The error '{e}' occurred")


@retry
def click(self, selector, timeout=10):
    try:
        element = WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
        )
        element.click()
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + "', 1,\'Clicked element with selector: {" + selector + "}\')"
        send_query_database(database_query)
        logging.info(f"Clicked element with selector: {selector}")
    except Exception as e:
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + "', 1,\'Error clicking element with selector: {"+selector+"} " + e + "\')"
        send_query_database(database_query)
        logging.error(f"Error clicking element with selector {selector}: {e}")
        raise

@retry
def sends_keys(sb, selector, keys, timeout):  # Поддержка нескольких аргументов для keys
    try:
        # element.clear()  # Очищаем поле перед вводом
        for key in keys:  # Вводим каждый аргумент по очереди
            sb.send_keys(selector,str(key),timeout=timeout)  # Преобразуем в строку
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + "', 1,\'Sent keys {"+keys+"} to element with selector {"+selector+"}:\')"
        send_query_database(database_query)
        logging.info(f"Sent keys '{keys}' to element with selector: {selector}")

    except Exception as e:
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + "', 1,\'Error sending keys {"+keys+"} to element with selector {"+selector+"}: " + e + "\')"
        send_query_database(database_query)
        logging.error(f"Error sending keys '{keys}' to element with selector {selector}: {e}")
        raise


@retry
def saveitems(titles,connection):
    items = []
    for title in titles:
        try:
            name = title.find_element(By.CSS_SELECTOR, 'a[data-marker="item-title"]').get_attribute("Title")
            url = title.find_element(By.CSS_SELECTOR, 'a[data-marker="item-title"]').get_attribute("href")
            price = title.find_element(By.CSS_SELECTOR, 'meta[itemprop="price"]').get_attribute("content")
            requirement = title.find_element(By.CSS_SELECTOR, 'p[data-marker="item-specific-params"]').text
            test1 = requirement.find("·")
            apartmentsID = title.get_attribute("data-item-id")

            item = {
                'name': name,
                'url': url,
                'price': price,
                'requirement':requirement,
                'apartmentsID':apartmentsID,
            }

            if item not in items:
                items.append(item)

        except Exception as e:
            database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S') + "', 1,\'Error extracting data from title: " + e + "\')"
            send_query_database(database_query)
            logging.error(f"Error extracting data from title: {e}")

    try:
        connection.autocommit = True
        cursor = connection.cursor()
        for item in items:
            cursor.execute("Select apartmentsID from apartmentsAvito where apartmentsID = "+item['apartmentsID']+"")
            result = cursor.fetchall()
            if len(result) == 0:
                query = "INSERT INTO apartmentsAvito (DateScan,name,url,price,requirement,active,apartmentsID) VALUES ('" + dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "', \'"+item['name']+"\',\'"+item['url']+"\',\'"+item['price']+"\',\'"+item['requirement']+"\',True,\'"+item['apartmentsID']+"\')"
                cursor.execute(query)

        #with open('result.json', 'a+', encoding="utf-8") as file:
        #    json.dump(items, file, indent=4, ensure_ascii=False)
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + "', 1,\'Data saved\')"
        send_query_database(database_query)
        #logging.info("Data saved to result.json")
    except Exception as e:
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "', 1,\'Error saving data: " + e + "\')"
        send_query_database(database_query)
        logging.error(f"Error saving data to JSON file: {e}")

def read_json_file(filepath):
    """Читает JSON-файл и возвращает его содержимое в виде Python-объекта (словаря или списка)."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:  # Явно указываем кодировку
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Ошибка: Файл не найден: {filepath}")
        return None
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный JSON в файле: {filepath}")
        return None
    except Exception as e:
        print(f"Произошла ошибка при чтении файла: {e}")
        return None


@retry
def collect_data_apartments():
    try:
        ua = UserAgent(platforms='pc')
        with SB(uc=True, headless=False) as sb:  #  headless=False для видимости
            connection = create_connection(
                "postgres", "postgres", "2028", "localhost", "5432"
            )
            sb.open("https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok?metro=171-181&s=104")

            nameMainFilter = findcase(sb.get_page_source(), "form-mainFilters")
            if nameMainFilter:
                sb.click(
                    "div." + nameMainFilter + " > form > div:nth-of-type(3) > div:nth-of-type(2) > div > div:nth-of-type(2) > div > label")  # form-mainFilters-y0xZT
            else:
                database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + "', 1,\' Could not find main filter element.\')"
                send_query_database(database_query)
                #logging.warning("Could not find main filter element.")

            element = sb.wait_for_element('input[data-marker="price-to/input"]', timeout=10)

            #Фильтры
            if element:
                #sends_keys(sb,'input[data-marker="price-to/input"]',"60000",timeout=10)  # Отправляем все цифры сразу
                sb.click('button[data-marker="search-filters/submit-button"]', timeout=10)
            else:
                database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + "', 1,\' Could not find price input element.\')"
                send_query_database(database_query)
                #logging.warning("Could not find price input element.")

            spisok = sb.find_elements(By.CSS_SELECTOR,'div[data-marker="item"]')
            if len(spisok)>0:
                titles = sb.find_elements(By.CSS_SELECTOR,'div[data-marker="item"]')
                saveitems(titles,connection)
            else:
                return "Not find elements"

            try:
                #Переход на след.страницу
                while len(sb.find_elements('a[data-marker="pagination-button/nextPage"]') )> 0:
                    sb.click('a[data-marker="pagination-button/nextPage"]')
                    spisoksssecond = sb.find_elements(By.CSS_SELECTOR, 'div[data-marker="item"]')
                    if len(spisoksssecond) > 0:
                        newtitles = sb.find_elements(By.CSS_SELECTOR,'div[data-marker="item"]')
                        saveitems(newtitles,connection)

            except Exception as e:
                database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S') + "', 1,\'Error saving data:" + e + "\')"
                send_query_database(database_query)

                logging.error(f"Error saving data to JSON file: {e}")
            connection.close()

    except Exception as e:
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + "', 1,\'Critical error in collect_data: "+ e +"\')"
        send_query_database(database_query)

        logging.critical(f"Critical error in collect_data: {e}")
        return [] # Возвращаем пустой список, чтобы не сломать логику

    return 0

# Пример использования
if __name__ == '__main__':
    database_query = "INSERT INTO history (Date,Users) VALUES ('" + dt.datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S') + "', 1)"
    send_query_database(database_query)
    collected_items = collect_data_apartments()
    if collected_items == "Not find elements":
        database_query = "INSERT INTO logs (Date,Users,Messages) VALUES ('" + dt.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S') + "', 1,\'not find element\')"
        send_query_database(database_query)
