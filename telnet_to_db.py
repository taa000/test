#!/usr/bin/env python
# coding: utf-8

# In[11]:


import telnetlib 
import time 
import redis
import re
import mysql.connector
import array


# In[12]:


def telnet_conn(HOST,PORT) :
    tn = telnetlib.Telnet(HOST,PORT) 
    tn.write(b'tcsre122|jakarta123|2 r\n') 
    tn.set_debuglevel(1) 
    return tn

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
    print('sukses')
    


# In[399]:


def filter_rt1(data, nameDB):
    curr, mydb = connection_db(nameDB) 
    sql = """INSERT INTO recordtype_1 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
    print('sukses')


# In[400]:


def filter_rt2(data, nameDB):
    curr, mydb = connection_db(nameDB) 
    sql = """INSERT INTO recordtype_2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
    print('sukses')

# In[401]:


def filter_rt3(data, nameDB):
    curr, mydb = connection_db(nameDB)
    sql = """INSERT INTO recordtype_3 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" #OK
    curr.execute(sql,data)
    mydb.commit() 
    print('sukses')


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

def classdata(data, nameDB) :
    data = re.sub('\s','', str(data))
    rowData = re.split('\|',str(data))
    index = rowData[3:4]
    print(index)
    if (index =='0') :
        filter_rt0(rowData[0:6], nameDB)
        data = rowData
    elif (index =='1') :
        filter_rt1(rowData[0:19], nameDB)
        data = rowData
    elif (index =='2') :
        filter_rt2(rowData[0:21], nameDB)
        data = rowData
    elif (index =='3') :
        filter_rt3(rowData[0:17], nameDB)
        data = rowData
    elif (index =='4') :
        filter_rt4(rowData[0:7], nameDB)
        data = rowData
    elif (index =='5') :
        filter_rt5(rowData[0:23], nameDB)
        data = rowData
    elif (index =='6') :
        filter_rt6(rowData[0:1], nameDB)
        data = rowData
    return data    
    
# In[13]:

def read_data_to_redis(tn, r, nameDB):
        getData = []
        while True: 
            tn_read = tn.read_very_eager()
            output = repr(tn_read)
            output = str(output)
            output = output.split('IDX|')
            for x in range (len(output)):
                time.sleep(0.1)
                data = classdata(output[x], nameDB)
                getData.append(data)



         
                


# In[14]:


def main() :
    r = redis.Redis()
    nameDB = "4March20"
    HOST = '172.18.2.213'
    PORT = 9010
    tn = telnet_conn(HOST,PORT)
    read_data_to_redis(tn,r, nameDB)



main()



