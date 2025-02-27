import json
import os.path
import logging
import time

from seleniumbase import SB
from fake_useragent import UserAgent
from selenium.webdriver.common.keys import Keys
from seleniumbase.undetected import ChromeOptions
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from fake_useragent import UserAgent


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='automation.log')

# def retry(func, retries=3, delay=1):
#     """Декоратор для повторных попыток выполнения функции."""
#     def wrapper(*args, **kwargs,*delay):
#         for attempt in range(retries):
#             try:
#                 return func(*args, **kwargs)
#             except Exception as e:
#                 logging.error(f"Attempt {attempt+1} failed: {e}")
#                 time.sleep(delay)
#                 delay *= 2  # Экспоненциальная задержка
#         logging.error(f"Function {func.__name__} failed after {retries} attempts.")
#         return None  # Или raise исключение, если нужно
#     return wrapper



def collect_data():
    try:
        # from selenium import webdriver
        #
        # driver = webdriver.Firefox()
        # driver.get("https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok?metro=171-181&s=104")
        # login_form = driver.find_element_by_xpath(".//*[@id='pt1:content']")
        # login_form.send_keys('test')
        # driver.find_element_by_xpath(".//*[@id='pt1:cb12']").click()
        # driver.quit()
        # return 0
        # driver = webdriver.Firefox()
        ua = UserAgent(platforms='pc')
        # with SB(uc=True, agent=ua.random, headless=True) as sb:
        with SB(uc=True, agent=ua.random) as sb:
            sb.open("https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok?metro=171-181&s=104")
            # sb.open("https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok?metro=171-181&s=104")
            nameMainFilter = findcase(sb.get_page_source(),"form-mainFilters")
            sb.click(
                "div."+nameMainFilter+" > form > div:nth-of-type(3) > div:nth-of-type(2) > div > div:nth-of-type(2) > div > label")#form-mainFilters-y0xZT
            nameIndexContext = findcase(sb.get_page_source(),"index-content")
            # sb.check_if_unchecked('input[value="5703"]')
            # # sb.click(th-of-type(6) > div:nth-of-type(2) > div > div:nth-of-type(2) > div > div > div > div > div > label > div > div")
            # sb.check_if_unchecked('input[name=
            #             # #     "div.form-mainFilters-y0xZT > form > div:n"params[110486]"]')
            element = sb.wait_for_element('input[data-marker="price-to/input"]', timeout=100000)

            sb.send_keys('input[data-marker="search-form/suggest"]',"26000", timeout=100000)
            # sb.send_keys('input[data-marker="price-to/input"]', 6, timeout=100000)
            # sb.send_keys('input[data-marker="price-to/input"]', 0, timeout=100000)
            # sb.send_keys('input[data-marker="price-to/input"]', 0, timeout=100000)
            # sb.send_keys('input[data-marker="price-to/input"]', 0, timeout=100000)
            # sb.click('button[data-marker="search-filters/submit-button"]',timeout=100)



            titles = sb.find_elements('div[data-marker="item"]')
            # if titles.
            # if len(titles) == 50:
            #     while True:
            #         sb.click('a[data-marker="pagination-button/nextPage"]')
            #         newtitles = sb.find_elements('div[data-marker="item"]')
            #         titles += newtitles
            #         if len(newtitles) < 50:
            #             break




            # titles = sb.find_elements('div[data-marker="item"]')

            items = []

            if os.path.isfile('result.json'):
                with open('result.json', 'r', encoding="utf-8") as file:
                    items = json.load(file)

            for title in titles:
                name = title.find_element("css selector", 'h3[itemprop="name"]').text
                url = title.find_element("css selector", 'a[data-marker="item-title"]').get_attribute("href")
                price = title.find_element("css selector", 'meta[itemprop="price"]').get_attribute("content")

                if int(price) < 35000:
                    item = {
                        'name' : name,
                        'url' : url,
                        'price' : price
                    }

                    if item not in items:
                        items.append(item)

            with open('result.json', 'w', encoding="utf-8") as file:
                json.dump(items, file, indent=4, ensure_ascii=False)
    except ValueError:
        print("Error")

# def enternumber(parsString, findstring):
#
#     return 1
def findcase(parsString, findstring):
    findPositionStartString = parsString.find(findstring)
    findPositionEndString = parsString.find("\"", findPositionStartString)
    return parsString[findPositionStartString:findPositionEndString]


def main():
    collect_data()

if __name__ == '__main__':
    main()

