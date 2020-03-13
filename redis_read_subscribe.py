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
    sql = "INSERT INTO paketdata (dates, timee, sequence, sec_code, high_price, low_price, close_price, volume, value, frequency)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" #OK
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
# p.subscribe('Paket_Satu') #Tinggal di ganti mau paket berapanya


nameDB = '10March20'
p.subscribe('testing')



start = True
while (start) :
    message = p.get_message() 
    if message: 
        command = message['data']
        if (type(command) == bytes) :
            data = command.decode('utf-8', 'ignore')
            data = reg(data)
            rowData = re.split('\,',str(data))
            time.sleep(3)
            fill(rowData[0:], nameDB)
            # print(rowData[0:])


