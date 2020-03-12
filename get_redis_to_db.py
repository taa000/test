#!/usr/bin/env python
# coding: utf-8

# In[396]:


import redis
import mysql.connector
import array
import re
import time


# In[397]:
# KONEKSI DATA BASE

def connection_db(nameDB):
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        database = nameDB
        )
    curr = mydb.cursor()
    return curr, mydb


# In[398]:

# QUERY UNTUK FILTER BERDASARKAN RECORDTYPE

def filter_rt0(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype_0 VALUES (%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
# In[399]:
def filter_rt1(data, nameDB):
    curr, mydb = connection_db(nameDB) 
    sql = """INSERT INTO recordtype_1 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
# In[400]:
def filter_rt2(data, nameDB):
    curr, mydb = connection_db(nameDB) 
    sql = """INSERT INTO recordtype_2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
# In[401]:
def filter_rt3(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype_3 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
# In[402]:
def filter_rt4(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype_4 VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    curr.execute(sql,data)
    mydb.commit() 
# In[403]:
def filter_rt5(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype_5 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    curr.execute(sql,data)
    mydb.commit() 
# In[404]:
def filter_rt6(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype_6 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    curr.execute(sql,data)
    mydb.commit() 
def alldata(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype VALUES (%s,%s,%s,%s)"""
    curr.execute(sql,data)
    mydb.commit()

# In[405]:


def filter_redis_rt(r, nameDB, reType):
    start = True
    j = 0
    while (start) :
        if (r.get(j) == b'OK 172.16.3.158\\r\\n') or (r.get(j) == b"b'Welcome to Datafeed Server 172.16.3.158\\nOK 172.16.3.158\\r\\n") :
            i = j+1
            while(start) :
                go = r.get(i)
                if (go == None):
                    break
                elif (go != b"'") and (go != b"b''") and (go != b"\\n'") and (go != b""):
                    data=go.decode('utf-8','ignore')
                    data = re.sub('\s','', str(data))
                    rowData = re.split('\|',str(data))
                    index = rowData[3:4]
                    if (index ==['0']) :
                        filter_rt0(rowData[0:6], nameDB)
                        data = rowData
                    elif (index ==['1']) :
                        filter_rt1(rowData[0:19], nameDB)
                        data = rowData
                    elif (index ==['2']) :
                        filter_rt2(rowData[0:21], nameDB)
                        data = rowData
                    elif (index ==['3']) :
                        filter_rt3(rowData[0:17], nameDB)
                        data = rowData
                    elif (index ==['4']) :
                        filter_rt4(rowData[0:7], nameDB)
                        data = rowData
                    elif (index ==['5']) :
                        filter_rt5(rowData[0:23], nameDB)
                        data = rowData
                    elif (index ==['6']) :
                        filter_rt6(rowData[0:12], nameDB)
                        data = rowData
                    reType.append(data)
                    alldata(rowData[0:4],nameDB) #MASUK KE DB DENGAN TABEL RECORDTYPE
                    inc = rowData[2]
                # print(data)
                i+=1
            start = False
        j+=1
        if (j == 100) :
            start = False
    return reType


# In[406]:


def main() :
    reType=[]

    r = redis.Redis()
    nameDB = "10march20"
    #FILTER DATA RECORDTYPE TO DB
    print("START")
    reType = filter_redis_rt(r, nameDB, reType)

    #RETYPE BISA DI GUNAKAN UNTUK SET KE REDIS
    

# inc = 0
# looping = True
# while (looping) :
#     inc = main()

main()
