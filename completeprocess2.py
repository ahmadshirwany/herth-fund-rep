import concurrent.futures
import sqlite3
import geopy.distance
import time
from threading import Lock

new_data = []
lock = Lock()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    chunks = []
    for i in range(0, len(lst), n):
        chunks.append(lst[i:i + n])
    return chunks


def closest_ad(Z):
    closest = ''
    closest_data = ''
    data = Z[0]
    e = list(data)
    e.append(e[18])
    e.append(e[19])
    clients_db = [a for a in Z[1] if data[2] == str(a[6])]
    clients_db2 = [a for a in Z[3] if data[2] == str(a[60])]
    clients_db3 = [a for a in Z[4] if data[5] == str(a[1])]
    longitude = data[7]
    latitude = data[8]
    if latitude > 90 or latitude < -90:
        print('latitude ' + str(latitude))
        print('longitude ' + str(longitude))
        return True
    # print('latitude1 '+str(latitude))
    # print('longitude1 ' + str(longitude))
    for index, check in enumerate(clients_db):

        longitude2 = check[8]
        latitude2 = check[7]
        if latitude2 > 90 or latitude2 < -90:
            print('latitude2 ' + str(latitude2))
            print('longitude2 ' + str(longitude2))
            continue
        coords_1 = (latitude, longitude)
        if (longitude2 == None or latitude2 == None) or (longitude2 == 0 and latitude2 == 0):
            continue
        coords_2 = (latitude2, longitude2)
        try:
            distance = geopy.distance.geodesic(coords_1, coords_2).km
        except Exception as e:
            print(e)
            print(coords_1)
            print(coords_2)
            print('index is ' + str(index))
            raise Exception
        if closest == '':
            closest = distance
            closest_data = check
        else:
            if distance < closest:
                closest = distance
                closest_data = check

    if closest_data == "":
        e[14] = 'No Matichng Zipcode'
        e[15] = float('inf')
    else:
        if closest_data[3] == '':
            e[14] = closest_data[0]
            e[15] = float(closest)

        else:
            e[14] = closest_data[3]
            e[15] = float(closest)
    closest = ''
    closest_data = ''
    for index, check in enumerate(clients_db2):

        longitude2 = check[52]
        latitude2 = check[51]
        if latitude2 > 90 or latitude2 < -90:
            print('latitude2 ' + str(latitude2))
            print('longitude2 ' + str(longitude2))
            continue
        coords_1 = (latitude, longitude)
        if (longitude2 == None or latitude2 == None) or (longitude2 == 0 and latitude2 == 0):
            continue
        coords_2 = (latitude2, longitude2)
        try:
            distance = geopy.distance.geodesic(coords_1, coords_2).km
        except Exception as e:
            print(e)
            print(coords_1)
            print(coords_2)
            print('index is ' + str(index))
            raise Exception
        if closest == '':
            closest = distance
            closest_data = check
        else:
            if distance < closest:
                closest = distance
                closest_data = check
    if closest_data == "":
        e[16] = 'No Matichng Zipcode'
        e[17] = float('inf')
    else:
        if closest_data[50] == '':
            e[16] = closest_data[0]
            e[17] = float(closest)
            print(e)
        else:
            e[16] = closest_data[50]
            e[17] = float(closest)
    closest = ''
    closest_data = ''
    for index, check in enumerate(clients_db3):
        longitude2 = float(check[3])
        latitude2 = float(check[2])
        if latitude2 > 90 or latitude2 < -90:
            print('latitude2 ' + str(latitude2))
            print('longitude2 ' + str(longitude2))
            continue
        coords_1 = (latitude, longitude)
        if (longitude2 == None or latitude2 == None) or (longitude2 == 0 and latitude2 == 0):
            continue
        coords_2 = (latitude2, longitude2)
        try:
            distance = geopy.distance.geodesic(coords_1, coords_2).km
        except Exception as e:
            print(e)
            print(coords_1)
            print(coords_2)
            print('index is ' + str(index))
            raise Exception
        if closest == '':
            closest = distance
            closest_data = check
        else:
            if distance < closest:
                closest = distance
                closest_data = check
    if closest_data == "":
        e[18] = 'No Matichng Zipcode'
        e[19] = float('inf')
    else:
        if closest_data[0] == '':
            e[18] = closest_data[0]
            e[19] = float(closest)
            print(e)
        else:
            e[18] = closest_data[0]
            e[19] = float(closest)
    e[20] = min(e[15], e[17], e[19])
    lock.acquire(True)
    new_data.append(tuple(e))
    lock.release()
    return True

if __name__ == "__main__":
    connect = sqlite3.connect("scrapedLinks.db")
    cursor2 = connect.cursor()
    cursor2.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor2.fetchall())
    cursor2.execute("select * from facilities;")
    clients_db = cursor2.fetchall()
    clients_db = [a for a in clients_db if a[7] and a[8] != None]
    connect.close()
    connect = sqlite3.connect("reonomy_ss.db")
    cursor2 = connect.cursor()
    cursor2.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor2.fetchall())
    cursor2.execute("select * from reonomy;")
    clients_db2 = cursor2.fetchall()
    connect.close()
    connect = sqlite3.connect("ahmad_google_facilities.db")
    cursor2 = connect.cursor()
    cursor2.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor2.fetchall())
    cursor2.execute("select * from all_google_facilities;")
    clients_db3 = cursor2.fetchall()
    connect = sqlite3.connect("demo5.db")
    cursor2 = connect.cursor()
    cursor2.execute("select * from Zipcodesdata")
    my_db = cursor2.fetchall()
    my_db.sort(key=lambda x: int(x[2]))
    connect.close()
    connect = sqlite3.connect("output2.db")
    cursor2 = connect.cursor()
    cursor2.execute(
        ' CREATE TABLE IF NOT EXISTS Zipcodesdata ( displayName varchar(255),state varchar(255),postalCode varchar(255), oUID varchar(255), '
        'listingid varchar(255) PRIMARY KEY, city varchar(255),listPrice INT,longitude FLOAT, latitiude FLOAT, uPI varchar(255), calculatedCity varchar(255),'
        'ID varchar(255), searchafter1 FLOAT , searchafter2 FLOAT,AD1 Text, closestdistance FLOAT ,AD2 Text, closestdistance2 FLOAT,AD3 Text, closestdistance3 FLOAT,closest_from_all_databases Text,FLOAT Text );')
    cursor2.execute("select * from Zipcodesdata")
    print(cursor2.description)
    cursor2.execute("select * from Zipcodesdata")
    updated_db = cursor2.fetchall()
    lengh_db = len(updated_db)
    lengh_db
    connect.close()
    chunk_list = chunks(my_db, 1000)
    print('total chunks :' + str(len(chunk_list)))
    for index, chunk in enumerate(chunk_list):
        threaded_start = time.time()
        new_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
            futures = []
            for data in chunk:
                # closest_ad([data, clients_db, my_db, clients_db2, clients_db3])
                # closest_ad([data,clients_db])
                # print(zip)
                # closest_ad([data,clients_db])
                futures.append(executor.submit(closest_ad, [data, clients_db, my_db, clients_db2, clients_db3]))
            results = [future.result() for future in futures]
        connect = sqlite3.connect("output2.db")
        cursor2 = connect.cursor()
        for value in new_data:
            # print(value)
            try:
                cursor2.execute(
                    "insert into Zipcodesdata values (?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    value)
            except Exception as e:
                print(e)
        connect.commit()
        connect.close()
        print("Threaded time:", time.time() - threaded_start)
        print("**************************chunk no " + str(index) + " complete*****************************")
