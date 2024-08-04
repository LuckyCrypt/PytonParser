import json
import os.path
from seleniumbase import SB
from fake_useragent import UserAgent




def collect_data():
    ua = UserAgent(platforms='pc')
    with SB(uc=True, agent=ua.random, headless=True) as sb:
        sb.open("https://www.avito.ru/sankt-peterburg/kvartiry/sdam/na_dlitelnyy_srok?metro=171-181&s=104")
        sb.click(
            "div.form-mainFilters-okGYr > form > div:nth-of-type(3) > div:nth-of-type(2) > div > div > div:nth-of-type(2) > label > div > div")
        sb.check_if_unchecked('input[value="5703"]')
        sb.click(
            "div.form-mainFilters-okGYr > form > div:nth-of-type(6) > div:nth-of-type(2) > div > div:nth-of-type(2) > div > div > div > div > div > label > div > div")
        sb.check_if_unchecked('input[name="params[110486]"]')
        sb.click('button[data-marker="search-filters/submit-button"]')

        titles = sb.find_elements('div[data-marker="item"]')

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

def main():
    collect_data()

if __name__ == '__main__':
    main()

