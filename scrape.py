from datetime import datetime

url_base = "https://nchmf.gov.vn/kttvsiteE/vi-VN/1/sai-gon-tp-ho-chi-minh-w{}.html"
results = []
for i in range(2, 74):
    try:
        resp = httpx.get(url_base.format(i), timeout=10)
        print(resp)
        if resp.status_code == 200:
            data = parse_weather(resp.content)
            data['location_id'] = i
            soup = BeautifulSoup(resp.content, 'lxml')
            title = soup.find('h1', class_='tt-news')
            data['location'] = title.text.strip() if title else None
            print(data)
            results.append(data)
        sleep(2)
    except: pass

import pandas as pd
df = pd.DataFrame(results)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'vn_weather_{timestamp}.csv'
df.to_csv(filename, index=False)
