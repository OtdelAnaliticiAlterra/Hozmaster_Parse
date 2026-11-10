import logging
import ssl

import aiohttp
import asyncio
from selectolax.parser import HTMLParser
import time
import pandas as pd
import os
from dotenv import load_dotenv, find_dotenv
from max_logger import MaxLogger
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(find_dotenv())


loger_token = os.environ.get('MAX_LOGGER_TOKEN')
recipient_ids = tuple(int(x.strip()) for x in os.environ["MAX_LOGGER_RECIPIENT_IDS"].split(","))

logger = MaxLogger(
    name='Парсинг_Хозмастер',
    project_file=__file__,
    token=loger_token,
    recipient_ids=recipient_ids,
    level=logging.DEBUG,
)


async def get_response(session, url, retries=3):
    """Получение ответа от сервера с обработкой ошибок"""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=50) as response:
                response.raise_for_status()
                return await response.text()
        # except (aiohttp.ClientTimeout, aiohttp.ClientError) as e: ClientTimeout - класс не является исключением!
        except aiohttp.ClientError as e:
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


def parse_product_page(html, url):
    """Разбор страницы товара в одну строку выгрузки; отсутствующие поля = None"""
    parser = HTMLParser(html)

    price = None
    price_node = parser.css_first("div.productprice")
    if price_node is not None:
        price = price_node.text().split(" ")[0].split("\t")[-1].replace(".", ",")
        if price in ("", "0,00"):
            price = None

    article = None
    code_node = parser.css_first("div.productcode")
    if code_node is not None:
        parts = code_node.text().split(" ")
        if len(parts) > 1:
            article = parts[1].split("\t")[0] or None

    name_node = parser.css_first("div.production td h2")
    name = name_node.text() if name_node is not None else ""
    name = name if name.strip() else None

    return {"Артикул": article, "Наименование": name, "Цена": price, "Ссылка": url}


async def parse_products(session):
    """Парсинг информации о товарах"""

    supply_path = await parse_goods(session)
    supply_path = pd.Series(supply_path).drop_duplicates().tolist()
    rows = []
    for elem in supply_path:
        print(f"elem = {elem}")
        response_text = await get_response(session, elem)
        if response_text is None:
            rows.append({"Артикул": None, "Наименование": None, "Цена": None, "Ссылка": elem})
        else:
            rows.append(parse_product_page(response_text, elem))
    return rows


async def main():
    start = time.time()
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        rows = await parse_products(session)

        df = pd.DataFrame(rows, columns=["Артикул", "Наименование", "Цена", "Ссылка"])
        incomplete = df[df[["Артикул", "Наименование", "Цена"]].isna().any(axis=1)]
        if not incomplete.empty:
            print(f"Пропущено товаров без цены/артикула/наименования: {len(incomplete)}")
            print(incomplete.to_string())
        df = df.drop(incomplete.index)

        if df.empty:
            raise ValueError("Не собрано ни одного товара, файл выгрузки не перезаписан")

        df.insert(0, "Код конкурента", "01-01046949")
        df.insert(1, "Конкурент", "Хозмастер")
        df.insert(4, "Вид цены", "Цена Хозмастер Барнаул")

        file_path = "\\\\tg-storage01\\Аналитический отдел\\Проекты\\Python\\Парсинг ОПТ\\Выгрузки\\Хозмастер\\Выгрузка цен.xlsx"

        df.to_excel(file_path, sheet_name="Данные", index=False)

        print("Парсинг выполнен")
    end = time.time()
    print("Время", (end - start))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(e)
        raise e
