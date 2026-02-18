from datetime import datetime
from time import sleep
import httpx
import pandas as pd
from bs4 import BeautifulSoup

def parse_weather(html, location_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {'location_id': location_id}
    
    # Get location title
    title = soup.find('h1', class_='tt-news')
    data['location'] = title.text.strip() if title else None
    
    # Get update time and weather data
    update_div = soup.find('div', class_='time-update')
    data['update_time'] = update_div.text.split('Cập nhật:')[1].strip() if update_div else None
    
    for li in soup.find('ul', class_='list-info-wt').find_all('li'):
        divs = li.find_all('div', class_='uk-width-3-4')
        if divs: 
            key = li.find('div', class_='uk-width-1-4').text.strip()
            val = divs[0].text.strip()[2:].strip()
            data[key] = val
    return data

url_base = "https://nchmf.gov.vn/kttvsiteE/vi-VN/1/sai-gon-tp-ho-chi-minh-w{}.html"
results = []

for i in range(2, 65):
    try:
        resp = httpx.get(url_base.format(i), timeout=10)
        print(f"Location {i}: {resp.status_code}")
        
        if resp.status_code == 200:
            data = parse_weather(resp.content, i)
            results.append(data)
            print(f"  -> {data['location']}")
            sleep(2)
    except Exception as e: 
        print(f"  Error: {e}")

df = pd.DataFrame(results)
filename = f'{datetime.now().year}_vn_weather.parquet'

try:
    existing = pd.read_parquet(filename)
    combined = pd.concat([existing, df], ignore_index=True)
    combined = combined.drop_duplicates()
    combined.to_parquet(filename, index=False)
    print(f"Added {len(df)} records, {len(combined)} total after removing duplicates")
except FileNotFoundError:
    df.to_parquet(filename, index=False)
    print(f"Created new file with {len(df)} records")
