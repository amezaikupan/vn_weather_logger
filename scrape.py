from datetime import datetime
from time import sleep
import httpx
import pandas as pd
from bs4 import BeautifulSoup
import re

def parse_num(str_w_num: str): 
    temp = re.findall(r'\d+', str_w_num)
    return int(temp[0]) if temp else None

def parse_wind(wind_info: str):
    speed = parse_num(wind_info)  
    drct = wind_info.split("-")[0].strip()
    return drct, speed

def extract_province(location_str): return re.search(r'\(([^)]+)\)', location_str).group(1)

def clean_weather_data(df):
    df = df.dropna(subset=['update_time', 'location'])
    df['update_time'] = pd.to_datetime(df['update_time'].str.replace('\xa0', ' '), format='%Hh  %d/%m/%Y')
    df['temp'] = df['Nhiệt độ'].apply(parse_num)
    df[['wind_direction', 'wind_speed']] = df['Hướng gió'].apply(
        lambda x: pd.Series(parse_wind(x))
    )
    df['Province'] = df['location'].apply(extract_province)
    df = df.drop(columns=['Nhiệt độ', 'Hướng gió'])
    return df

def parse_weather(html, location_id):
    soup = BeautifulSoup(html, 'lxml')
    data = {'location_id': location_id}
    
    title = soup.find('h1', class_='tt-news')
    data['location'] = title.text.strip() if title else None
    
    update_div = soup.find('div', class_='time-update')
    data['update_time'] = update_div.text.split('Cập nhật:')[1].strip() if update_div else None
    
    for li in soup.find('ul', class_='list-info-wt').find_all('li'):
        divs = li.find_all('div', class_='uk-width-3-4')
        if divs: 
            key = li.find('div', class_='uk-width-1-4').text.strip()
            val = divs[0].text.strip()[2:].strip()
            data[key] = val
    return data

def scrape_weather(): 
    url_base = "https://nchmf.gov.vn/kttvsiteE/vi-VN/1/sai-gon-tp-ho-chi-minh-w{}.html"
    results = []

    for i in range(2, 65):
        if i == 40: continue
        try:
            resp = httpx.get(url_base.format(i), timeout=10)        
            if resp.status_code == 200: results.append(parse_weather(resp.content, i))
            sleep(2)
        except Exception as e: 
            print(f"  Error: {e}")
    
    return pd.DataFrame(results)

df = clean_weather_data(scrape_weather())  # Clean new data
filename = f'{datetime.now().year}_vn_weather.parquet'

try:
    existing = pd.read_parquet(filename)
    combined = pd.concat([existing, df], ignore_index=True).drop_duplicates()
    combined.to_parquet(filename, index=False)
    print(f"Added {len(df)} records, {len(combined)} total after removing duplicates")
except FileNotFoundError:
    df.to_parquet(filename, index=False)
    print(f"Created new file with {len(df)} records")