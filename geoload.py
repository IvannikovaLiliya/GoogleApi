# create sqllite file

import urllib.error, urllib.request, urllib.parse
import ssl
import http
import sqlite3
import json
import time
import sys

serviceurl = "https://maps.googleapis.com/maps/api/geocode/json?"
api_key = input('Enter api_key:)

# connect to geodata.sqlite
connSql = sqlite3.connect('geodata.sqlite')
cursor = connSql.cursor()
userChoise = input('Enter yes if Delete old DB?')
if userChoise == 'yes':
    cursor.executescript('''DROP TABLE IF EXISTS Locations;
    CREATE TABLE Locations (address TEXT, geodata TEXT)''')
else:
    cursor.execute('''CREATE TABLE IF NOT EXISTS Locations
    (address TEXT, geodata TEXT)''')

# ignore ssl sert errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

geoData = open('where.data')
count = 0
for line in geoData:
    if count == 200 :
        print('\nRetrieved 200 locations, restart to retrieve more')
        break

    print()
    adressLine = line.rstrip()
    # check in sql it
    cursor.execute("SELECT geodata FROM Locations WHERE address= ?",
        (memoryview(adressLine.encode()), ))

    try:
        data = cursor.fetchone()[0]
        print("Found in database ",adressLine)
        continue
    except:
        pass

    #url connect
    parms = dict()
    parms['key'] = api_key
    parms['address'] = adressLine
    print('Resolving', adressLine)
    url = serviceurl + urllib.parse.urlencode(parms)
    print('Retrieving', url)
    uh = urllib.request.urlopen(url, context=ctx)

    data = uh.read().decode()
    print('Retrieved', len(data), 'characters', data[:20].replace('\n', ' '))
    count = count + 1

    #response data
    try:
        js = json.loads(data)
    except:
        print(data)  # We print in case unicode causes an error
        continue

    if 'status' not in js or (js['status'] != 'OK' and js['status'] != 'ZERO_RESULTS') :
        print('==== Failure To Retrieve ====')
        print(data)
        break

    # update db
    cursor.execute('''INSERT INTO Locations (address, geodata)
            VALUES ( ?, ? )''', (memoryview(adressLine.encode()), memoryview(data.encode()) ) )
    connSql.commit()

    if count % 10 == 0 :
        print('Pausing for a bit...')
        time.sleep(5)

print("\nRun geodump.py to read the data from the database so you can vizualize it on a map.")
