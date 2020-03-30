#!/usr/bin/env python
# coding: utf-8



import redis
import mysql.connector
import numpy as np
import time
import re
from datetime import datetime


def connection_db(nameDB):
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        database = nameDB
        )
    curr = mydb.cursor()
    return curr, mydb



def fill(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = "INSERT INTO paketdata (dates, sequence, emiten_code, open_price, high_price, low_price, close_price, volume, value, frequency)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #OK
    # kolom times di ilangin, dates setting ke datetime
    curr.execute(sql,data)
    mydb.commit() 

def reg(data) :
    data = re.sub('\[', '', str(data))
    data = re.sub('\]','', str(data))
    data = re.sub('\s','', str(data))
    data = re.sub('\'','', str(data))
    return data

def get_datetime(date, time):
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])
    hour = int(time [:2])
    minute = int(time[2:4])
    second = int(time[4:])

    event = str(datetime(year, month, day, hour, minute, second))
    return event

r = redis.Redis()
p = r.pubsub()


nameDB = 'Test14Maret'
p.subscribe('testing') #sesuain nama channel di tes.py

print("OK")

start = True
while (start) :
    
    message = p.get_message() 
    if message: 
        command = message['data']
        if (type(command) == bytes) :
            data = command.decode('utf-8', 'ignore')
            data = reg(data)
            rowData = re.split('\,',str(data))
            # time.sleep(3) #gausah pake delay

            #buat datetimenya
            datetimes = get_datetime(rowData[0], rowData[1])
            # semisalnya error pke rowdata[0], rowdata[1], pake ini :
            # date = row[0]
            # time = row[1]
            # datetimes = get_datetime(date, time)

            dataPaket = rowData[2:]
            dataPaket.insert(0, datetimes) # insert si datetime kedalam list
            # ['2020-03-10 09:00:03', '00005276', 'BHIT', '00000000087.00', '00000000000.00', '00000000000.00', '00000000000.00', '000000000000', '0000000000000000', '0000000']
            
            fill(dataPaket[0:], namaDB)
            print(dataPaket[0:])
            

            #//////fill(rowData[0:12], nameDB)
            #//////print(rowData[0:12])
            
# contoh data :
    # ['20200310', '090003', '00005276', 'BHIT', '00000000087.00', '00000000000.00', '00000000000.00', '00000000000.00', '000000000000', '0000000000000000', '0000000']
    # 


