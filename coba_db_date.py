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
    sql = "INSERT INTO paketdata (datetimes, sequence, emiten_code, open_price, high_price, low_price, close_price, volume, value, frequency)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #OK
    curr.execute(sql,data)
    mydb.commit() 

def reg(data) :
    data = re.sub('\[', '', str(data))
    data = re.sub('\]','', str(data))
    data = re.sub('\s','', str(data))
    data = re.sub('\'','', str(data))
    return data



nameDB = 'db_idx'

print("OK")
data = ['20200310','090003', '00005276', 'BHIT', '00000000087.00', '00000000000.00', '00000000000.00', '00000000000.00', '000000000000', '0000000000000000', '0000000']
date = data[0]
time = data[1]

year = int(date[:4])
month = int(date[4:6])
day = int(date[6:])
hour = int(time [:2])
minute = int(time[2:4])
second = int(time[4:])

event = str(datetime(year, month, day, hour, minute, second))
alldata = data[2:]
alldata.insert(0,event)
fill(alldata, nameDB)

# insert(data)





# contoh data :
    # ['20200310', '090003', '00005276', 'BHIT', '00000000087.00', '00000000000.00', '00000000000.00', '00000000000.00', '000000000000', '0000000000000000', '0000000']


# ['2020-03-10 09:00:03', '00005276', 'BHIT', '00000000087.00', '00000000000.00', '00000000000.00', '00000000000.00', '000000000000', '0000000000000000', '0000000']