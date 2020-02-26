#!/usr/bin/env python
# coding: utf-8

# In[396]:


import redis
import mysql.connector
import array
import re
import time


# In[397]:


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


# In[405]:


def filter_redis_rt(r, reType0, reType1, reType2, reType3, reType4, reType5, reType6, nameDB):
    reType=[]
    for i in range (9999) :
        go = r.get(i+2)
        if (go == None):
            break
        elif (go !=b'"') and (go != b"'") :
            data=go.decode('utf-8','ignore')
            data = re.sub('\s','', str(data))
            rowData = re.split('\|',str(data))
            if (rowData[4]=='0') :
                filter_rt0(rowData[1:7], nameDB)
                reType0.append(rowData[0:7])
            elif (rowData[4]=='1') :
                filter_rt1(rowData[1:20], nameDB)
                reType1.append(rowData[0:20])
            elif (rowData[4]=='2') :
                filter_rt2(rowData[1:22], nameDB)
                reType2.append(rowData[0:22])
            elif (rowData[4]=='3') :
                filter_rt3(rowData[1:18], nameDB)
                reType3.append(rowData[0:18])
            elif (rowData[4]=='4') :
                filter_rt4(rowData[1:8], nameDB)
                reType4.append(rowData[0:8])
            elif (rowData[4]=='5') :
                filter_rt5(rowData[1:24], nameDB)
                reType5.append(rowData[0:24])
            elif (rowData[4]=='6') :
                filter_rt6(rowData[1:13], nameDB)
                reType6.append(rowData[0:13])
    reType = reType0, reType1, reType2, reType3, reType4, reType5, reType6
    return reType


# In[406]:


def main() :
    reType0=[]
    reType1=[]
    reType2=[]
    reType3=[]
    reType4=[]
    reType5=[]
    reType6=[]
    reType=[]

    r = redis.Redis()
    nameDB = "get"
    #FILTER DATA RECORDTYPE TO DB
    reType = filter_redis_rt(r, reType0, reType1, reType2, reType3, reType4, reType5, reType6, nameDB)
    
    #FILTER DATA PAKET 1
    


# In[407]:


main()

