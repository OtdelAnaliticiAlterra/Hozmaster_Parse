import aiohttp
import asyncio
from selectolax.parser import HTMLParser
import time
import pandas as pd
import os
import openpyxl
from dotenv import load_dotenv, find_dotenv
from telegram_bot_logger import TgLogger
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

load_dotenv(find_dotenv())

CHATS_IDS = '\\\\TG-Storage01\\Аналитический отдел\\Проекты\\Python\\chats_ids.csv'

logger = TgLogger(
    name='Парсинг_Хозмастер',
    token=os.environ.get('LOGGER_BOT_TOKEN'),
    chats_ids_filename=CHATS_IDS,
)
async def get_response(session, url, retries=3):
    """Получение ответа от сервера с обработкой ошибок"""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=50) as response:
                response.raise_for_status()
                return await response.text()
        except (aiohttp.ClientTimeout, aiohttp.ClientError) as e:
            print(f"Network error occurred: {e}. Attempt {attempt + 1} of {retries}. Retrying...")
            await asyncio.sleep(2)
        except asyncio.TimeoutError:
            print(f"Timeout error occurred for URL: {url}. Attempt {attempt + 1} of {retries}. Retrying...")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"An unexpected error occurred while requesting {url}: {e}")
            break
    return None


async def parse_categories(session):
    """Парсинг категорий товаров"""
    response_text = await get_response(session, 'https://www.hozmaster.ru/products/')
    if response_text is not None:
        parser = HTMLParser(response_text)
        cat_links = [f'https://www.hozmaster.ru{categories.attributes.get("href")}' for categories in
                 parser.css('div a.cat2level')]
        for elems in parser.css("div.cat1level a"):
            cat_links.append(f'https://www.hozmaster.ru{elems.attributes.get("href")}')

        print(len(cat_links))
        return cat_links
    return []


async def parse_goods(session):
    """Парсинг ссылок на товары"""
    cat_links = await parse_categories(session)
    ref_list = []

    for el in cat_links:
        response_text = await get_response(session, el)
        if response_text is not None:
            parser = HTMLParser(response_text)

            if "cat2level" in parser.html:
                for categ in parser.css("div a.cat2level"):
                    cat_links.append(f'https://www.hozmaster.ru{categ.attributes.get("href")}')
                if "listnu" in parser.html:
                    for categ in parser.css("td a.listnu"):
                        ref_list.append("https://www.hozmaster.ru" + categ.attributes.get("href"))

            else:
                for categ in parser.css("td a.listnu"):
                    ref_list.append("https://www.hozmaster.ru" + categ.attributes.get("href"))

    return ref_list




async def parse_products(session):
    """Парсинг информации о товарах"""

    supply_path = await parse_goods(session)
    supply_path = pd.Series(supply_path).drop_duplicates().tolist()
    article_list = []
    naming = []
    price_list = []
    for elem in supply_path:
        print(f"elem = {elem}")
        response_text = await get_response(session, elem)
        if response_text is not None:
            parser = HTMLParser(response_text)

            for eldata in parser.css("div.productprice"):
                price_list.append(eldata.text().split(" ")[0].split("\t")[-1].replace(".",","))
                break

            for element in parser.css("div.productcode"):
                article_list.append(element.text().split(" ")[1].split("\t")[0])
                break

            for categ in parser.css("div.production td h2"):
                naming.append(categ.text())
    return supply_path, article_list,naming, price_list


async def main():
    start = time.time()
    async with aiohttp.ClientSession() as session:
        product_links, article_list, name_list, price_list = await parse_products(session)

        new_slovar = {
            "Код конкурента": "01-01046949",
            "Конкурент": "Хозмастер",
            "Артикул": article_list,
            "Наименование": name_list,
            "Вид цены": "Цена Хозмастер Барнаул",
            "Цена": price_list,
            "Ссылка": product_links
        }

        df = pd.DataFrame(new_slovar)
        file_path = "\\tg-storage01\\Аналитический отдел\\Проекты\\Python\\Парсинг ОПТ\\Выгрузки\\Хозмастер\\Выгрузка цен.xlsx"


        df.to_excel(file_path, sheet_name="Лист 1", index=False)

        print("Парсинг выполнен")
    end = time.time()
    print("Время", (end - start))


if __name__ == "__main__":
    asyncio.run(main())
