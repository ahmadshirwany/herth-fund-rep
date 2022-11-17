import concurrent.futures
import datetime
import time
import requests
import json
import sqlite3
import pandas as pd

from threading import Lock

data_list = []
lock = Lock()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    chunks = []
    for i in range(0, len(lst), n):
        chunks.append(lst[i:i + n])
    return chunks


def post_api(Z):
    # print('start of post api')
    # print(Z)
    headers = {
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
        'Access-Control-Max-Age': '600',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'DNT': '1',
        'Origin': 'https://www.remax.com',
        'Referer': 'https://www.remax.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
    }

    json_data = {
        'count': 10000,
        'offset': 0,
        'sorts': {
            '0': {
                'listingContractDate': 'desc',
            },
        },
        'terms': {
            'place': {
                'placename': Z,
                'placeType': 'zip',
                'placeId': Z,
            },
            'zipCodeId': [
                Z,
            ],
        },
        'listingLoadLevel': 'WebGeo',
    }
    # print('start of request api')
    response = requests.post('https://public-api-gateway-prod.kube.remax.booj.io/listings/search/run/', headers=headers,
                             json=json_data)
    res = json.loads(response.text)
    # print(res)
    # API REQ + DB INSERT
    # sleep(0.00001)
    # return data + 1000
    if 'message' in list(res.keys()):
        if res['message'] == 'Server Error':
            print('skip ' + Z)
    try:
        asas = res['data']['results']
    except:
        return False
    for b in res['data']['results']:
        try:
            result_list = []
            result_list.append(b['geo'][0]['displayName'])
            result_list.append(b['geo'][0]['addressComponents']['state'])
            result_list.append(b['geo'][0]['addressComponents']['postalCode'])
            result_list.append(b['oUID'])
            result_list.append(str(b['listingId']))
            try:
                result_list.append(b['city'])
            except Exception as e:
                result_list.append('')
                print(e)
            try:
                result_list.append(b['listPrice'])
            except Exception as e:
                result_list.append('')
                print(e)
            result_list.append(b['location']['lon'])
            result_list.append(b['location']['lat'])
            result_list.append(b['uPI'])
            result_list.append(b['calculatedCity'])
            result_list.append((b['_id']))
            result_list.append(b['_searchAfter'][0])
            result_list.append(b['_searchAfter'][1])
            result_list.append('')
            result_list.append('')
            result_list.append('')
            result_list.append('')
            result_list.append('')
            result_list.append(datetime.datetime.now().strftime('%d-%m-%Y'))
            data_list.append(tuple(result_list))
            # print(result_list)
        except Exception as e:
            print(e)
            print(b)
            continue
    return True


if __name__ == "__main__":
    zipcodes_df = pd.read_excel('zip_code_database.xls')
    df = zipcodes_df.query("type != 'PO BOX'")
    zipcodes1 = zipcodes_df.query("type != 'PO BOX'")['zip'].tolist()
    zipcodes = []
    for zip in zipcodes1:
        Z = str(zip)
        if len(Z) < 5:
            if len(Z) == 3:
                Z = '00' + Z
            elif len(Z) == 4:
                Z = '0' + Z
        zipcodes.append(Z)
    connect = sqlite3.connect("demo5.db")
    cursor2 = connect.cursor()
    chunk_list = chunks(zipcodes, 500)
    print('total chunks ' + str(len(chunk_list)))
    for index, chunk in enumerate(chunk_list):
        data_list = []
        threaded_start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=500) as executor:
            futures = []
            for zip in chunk:
                # print(zip)
                futures.append(executor.submit(post_api, str(zip)))

            results = [future.result() for future in futures]
        for data in data_list:
            try:
                cursor2.execute(
                    "insert into Zipcodesdata values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)", data)
            except Exception as e:
                print(e)
                if e.args[0] == 'UNIQUE constraint failed: Zipcodesdata.listingid':
                    print('listingid already exist')
        connect.commit()
        print("Threaded time:", time.time() - threaded_start)
        print(
            '*********************************' + str(
                index + 1) + ' chunk completed*************************************')
