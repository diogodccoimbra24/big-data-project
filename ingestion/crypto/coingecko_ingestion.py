
import requests
import time
import datetime
import json


#using datetime to mention the desired dates to retrieve data
#start of the sample
date_from = datetime.date(2025, 2, 1)
#end of the sample
date_to = datetime.date(2025, 12, 25)

#Converting those dates to unix time
unixtime_from = time.mktime(date_from.timetuple())
unixtime_to = time.mktime(date_to.timetuple())

api_key = 'CG-G9pf6Zs4ovWsZkvfoxDjP9mn'
url = 'https://api.coingecko.com/api/v3/'

#id so we can select the id of the desired coin in the endpoint
id = 'bitcoin'

#Endpoint desired to retrieve data (API documentation)
endpoint = f'coins/{id}/market_chart/range'

headers = {'x-cg-demo-api-key': api_key}

params = {
    'vs_currency': 'usd',
    #API requires unix time date stamps
    'from': unixtime_from,
    'to': unixtime_to
}

response = requests.get(url + endpoint, headers = headers, params = params)

if response.status_code == 200:
    data = response.json()
    bitcoin_price = data

    #If response is good a json file is created in data/raw/crypto
    created_file_path = f'data/raw/crypto/{id}_price_from{date_from}_to_{date_to}.json'

    with open (created_file_path, 'w') as f:
        json.dump(bitcoin_price, f, indent = 2)

else:
    print(response.status_code)
    print('Impossible to retrieve any data')

