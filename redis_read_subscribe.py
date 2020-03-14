#!/usr/bin/env python
# coding: utf-8



import redis
import mysql.connector
import numpy as np
import time
import re


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
    sql = "INSERT INTO paketdata (dates, timee, sequence, emiten_code, open_price, high_price, low_price, close_price, volume, value, frequency)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #OK
    curr.execute(sql,data)
    mydb.commit() 

def reg(data) :
    data = re.sub('\[', '', str(data))
    data = re.sub('\]','', str(data))
    data = re.sub('\s','', str(data))
    data = re.sub('\'','', str(data))
    return data


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
            fill(rowData[0:12], nameDB)
            print(rowData[0:12])
            # print(rowData[1])


# contoh data :
    # ['20200310', '090003', '00005276', 'BHIT', '00000000087.00', '00000000000.00', '00000000000.00', '00000000000.00', '000000000000', '0000000000000000', '0000000']
    # 


