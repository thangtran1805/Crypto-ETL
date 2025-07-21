import requests
import json
from datetime import datetime
def crawl_cryto():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency' : 'usd',
        'order' : 'market_cap_desc',
        'per_page' : 100,
        'page' : 1
    }
    r = requests.get(url,params=params)

    data = r.json()
    if data:
        print('Crawling succesfully')
    else:
        print('Failed to crawl data')
        return None
    
    json_object = json.dumps(data,indent=4)

    # get date
    today = datetime.today().strftime('%Y_%m_%d')

    path = r'/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/raw/crawl_crypto_' + f'{today}.json'

    with open(path,'w') as outfile:
        outfile.write(json_object)

    print(f'Saving to {path}')
crawl_cryto()