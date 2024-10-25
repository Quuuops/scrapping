import asyncio
import aiohttp
from bs4 import BeautifulSoup

import csv
from typing import List, Dict, Optional, Tuple

OTODOM_URL = 'https://www.otodom.pl'
OLX_URL = 'https://www.olx.pl'


ALL_LINKS = []
DEFAULT_HEADERS = [
    'Url',
    'Title',
    'Price',
    'Description',
    'Added',
    'Location'
]

OTODOM_ADDITIONAL_PARAMS = [
    'Powierzchnia',
    'Czynsz',
    'Liczba pokoi',
    'Kaucja',
    'Piętro',
    'Rodzaj zabudowy',
    'Dostępne od'
]


async def fetch(session: aiohttp.ClientSession, url: str) -> Tuple[str, Optional[str]]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                print(f"Failed to retrieve the page: {url}, status code: {response.status}")
                return (url, None)
            html_content = await response.text()
            return (url, html_content)
    except aiohttp.ClientConnectorError as e:
        print(f"Connection error for URL: {url} - {e}")
        return (url, None)


async def process_apartments(session: aiohttp.ClientSession, links: List[str]) -> List[Optional[str]]:
    tasks = [fetch(session, link) for link in links]
    responses = await asyncio.gather(*tasks)
    return list(responses)


async def get_apartment_links(session: aiohttp.ClientSession, url: str) -> List[str]:
    _, page_content = await fetch(session, url)
    if not page_content:
        return []
    soup = BeautifulSoup(page_content, 'html.parser')
    apartment_blocks = soup.find_all('div', class_='css-qfzx1y')

    links = []
    for apartment in apartment_blocks:
        link_tag = apartment.find('a')
        if link_tag and link_tag.get('href'):
            link = link_tag['href']
            if not link.startswith(OTODOM_URL):
                link = f'{OLX_URL}{link}'
            links.append(link)

    return links


def extract_olx_apartment_data(url: str, html_content: str) -> Dict[str, str]:
    soup = BeautifulSoup(html_content, 'html.parser')

    price = soup.find('h3', class_='css-90xrc0').get_text(strip=True) if soup.find('h3',  class_='css-90xrc0') else 'No price'
    added = soup.find('span', attrs={'data-cy': 'ad-posted-at'}).get_text(strip=True) if soup.find('span', attrs={'data-cy': 'ad-posted-at'}) else 'No Date'
    title = soup.find('h4', class_='css-1kc83jo').get_text(strip=True) if soup.find('h4', class_='css-1kc83jo') else 'No title'
    description = soup.find('div', class_='css-1t507yq').get_text(strip=True) if soup.find('div', class_='css-1t507yq') else 'No Description'

    return {
        'Url': url,
        'Title': title,
        'Price': price,
        'Description': description,
        'Added': added
    }


def extract_otodom_apartment_data(url: str, html_content: str) -> Dict[str, str]:
    description_text = ''

    soup = BeautifulSoup(html_content, 'html.parser')
    price = soup.find('strong', attrs={'data-cy': 'adPageHeaderPrice'}).get_text(strip=True) if soup.find('strong', attrs={'data-cy': 'adPageHeaderPrice'}) else 'No price'
    added = soup.find('div', class_='css-1soi3e7 e4mhl2h4').get_text(strip=True) if soup.find('div', class_='css-1soi3e7 e4mhl2h4') else 'No Date'
    title = soup.find('h1', {'data-cy': 'adPageAdTitle'}).get_text(strip=True) if soup.find('h1', {'data-cy': 'adPageAdTitle'}) else 'No title'
    location = soup.find('a', {'aria-label': 'Adres'}).get_text(strip=True) if soup.find('a', {'aria-label': 'Adres'}) else 'No Location'
    description_div = soup.find('div', {'data-cy': 'adPageAdDescription'})
    if description_div:
        paragraphs = description_div.find_all('p')
        description_text = '\n'.join(p.get_text(strip=True) for p in paragraphs)

    data = {
        'Url': url,
        'Title': title,
        'Price': price,
        'Description': description_text,
        'Added': added,
        'Location': location,
    }
    additional_params = {param: extract_otodom_additional_info(soup, param) for param in OTODOM_ADDITIONAL_PARAMS}
    data.update(additional_params)

    return data


def extract_otodom_additional_info(soup, label):
    section = soup.find('div', {'aria-label': label})
    if section:
        value = section.find('div', {'class': 'css-1wi2w6s'})
        if value:
            return value.get_text(strip=True)
    return ''


def is_olx_page(url: str) -> bool:
    return url.startswith(OLX_URL)


def write_to_csv(data_list: List[Dict[str, str]], filename: str, mode='a') -> None:
    if not data_list and mode != 'w':
        return

    ALL_HEADERS = DEFAULT_HEADERS + OTODOM_ADDITIONAL_PARAMS

    with open(filename, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=ALL_HEADERS)
        if mode == 'w':
            writer.writeheader()
        writer.writerows(data_list)


async def main():
    base_url = f'{OLX_URL}/nieruchomosci/mieszkania/wynajem/wroclaw/'
    page = 1
    batch_size = 10
    apartments_data = []

    write_to_csv([], 'apartments.csv', mode='w')

    async with aiohttp.ClientSession() as session:
        while True:
            url = f'{base_url}?page={page}'
            async with session.get(url) as response:
                if response.status != 200:
                    print(f'Error: Received response status {response.status}. Ending process.')
                    break

            apartment_links = await get_apartment_links(session, url)

            if not apartment_links:
                print('No more apartments found. Ending process.')
                break

            apartments_html = await process_apartments(session, apartment_links)
            for link, html in apartments_html:
                if html:
                    if link in ALL_LINKS:
                        continue
                    if is_olx_page(link):
                        data = extract_olx_apartment_data(link, html)
                    else:
                        data = extract_otodom_apartment_data(link, html)

                    ALL_LINKS.append(link)
                    apartments_data.append(data)

            print(f'Processed page {page}')

            if page % batch_size == 0:
                print(f'Writing data to file after processing {page} pages...')
                write_to_csv(apartments_data, 'apartments.csv', mode='a')
                apartments_data = []

            page += 1

        if apartments_data:
            print(f'Writing remaining data to file...')
            write_to_csv(apartments_data, 'apartments.csv', mode='a')


asyncio.run(main())