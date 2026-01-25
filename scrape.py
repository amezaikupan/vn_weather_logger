from datetime import datetime
from time import sleep
import httpx
import pandas as pd
from bs4 import BeautifulSoup

def parse_weather(html):
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    data['update_time'] = soup.find('div', class_='time-update').text.split('Cập nhật:')[1].strip()
    for li in soup.find('ul', class_='list-info-wt').find_all('li'):
        divs = li.find_all('div', class_='uk-width-3-4')
        if divs: 
            key = li.find('div', class_='uk-width-1-4').text.strip()
            val = divs[0].text.strip()[2:].strip()
            data[key] = val
    return data

url_base = "https://nchmf.gov.vn/kttvsiteE/vi-VN/1/sai-gon-tp-ho-chi-minh-w{}.html"
results = []
for i in range(2, 74):
    try:
        resp = httpx.get(url_base.format(i), timeout=10)
        print(f"Location {i}: {resp.status_code}")
        if resp.status_code == 200:
            data = parse_weather(resp.content)
            data['location_id'] = i
            soup = BeautifulSoup(resp.content, 'lxml')
            title = soup.find('h1', class_='tt-news')
            data['location'] = title.text.strip() if title else None
            results.append(data)
            print(f"  -> {data['location']}")
        sleep(2)
    except Exception as e: print(f"  Error: {e}")

df = pd.DataFrame(results)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'vn_weather_{timestamp}.csv'
df.to_csv(filename, index=False)
print(f"Saved {len(results)} locations to {filename}")
