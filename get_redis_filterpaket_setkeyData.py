#!/usr/bin/env python
# coding: utf-8

# In[31]:


import redis
import re
import numpy as np
import pandas as pd


# In[47]:


def filter_redis_paket(r):
    rt1=[]
    rt2=[]
    rt5=[]
    dataa = []

    i = 2
    golive = True
    while (golive) :
        go = r.get(i)
        gi = r.get(i+1)
        i = i+1
        if (gi == None):
            golive = False
        data = go.decode('utf-8','ignore')
        data = re.sub('\s','', str(data))
        rowdata = re.split('\|',str(data))
        recordtype = rowdata[4:5]
        if (recordtype==['1']) :
            data = [rowdata[1], rowdata[2], rowdata[3], rowdata[7], rowdata[10]]
            if (rowdata[2]>= '085500') and (rowdata[2] <='085559') :
                d = [rowdata[3], rowdata[7], rowdata[10]]
                rt1.append(d)
        elif(recordtype==['2']) :
            if (rowdata[2]>'085959') and (rowdata[6]=='0') :
                e = [rowdata[3],rowdata[7], rowdata[10]]
                rt2.append(e)
        elif (recordtype==['5']) :
            g = [rowdata[1], rowdata[2], rowdata[3], rowdata[5],rowdata[8], rowdata[9], rowdata[10], rowdata[12], rowdata[13], rowdata[14]]
            dataa.append(g)
            if ((int(rowdata[2]) <= 85959) and (rowdata[6]=='RG')) :
                f =rowdata[5]
                rt5.append(f)
    return rt1, rt2, rt5, dataa


# In[48]:


def get_list_emiten(rt5) :
    e = ""
    data = []
    rt5.sort()
    for i in (rt5) :
        if (i != e) :
            data.append(i)
            e = i
    ListEm = pd.DataFrame({'Emiten' :data})
    return data, ListEm


# In[49]:


def fill_open(rt, list_emiten) :
    emiten = []
    open_price = []
    for i in range (len(list_emiten)) :
        em = re.sub('\s', '', str(list_emiten[i]))
        for j in range (len(rt)) :
            row_rt = rt[j]
            if (em == row_rt[1]):
                emiten.append(row_rt[1])
                open_price.append(row_rt[2])
                break
    data = pd.DataFrame({'Emiten':emiten, 'OpenPrice':open_price})
    return data


# In[50]:


def f_one(ListEm) :
    data = []
    for i in (ListEm) :
        i = re.sub('\[','', str(i))
        i = re.sub('\]','', str(i))
        i = re.sub('\'','', str(i))
        data.append(i)
    return data


# In[51]:


def get_openPrice(List_Em_Open, emiten) :
    for x in range (len(List_Em_Open.Emiten)) :
        if (List_Em_Open.Emiten[x] == emiten) :
#             Open_Price = List_Em_Open.OpenPrice[x]
            return List_Em_Open.OpenPrice[x]


# In[52]:

def datafet(roll_data, List_Em_Open):
    datafeed = pd.DataFrame(
                columns= ['Tanggal', 'Waktu',
                        'Sequence', 'Emiten', 
                        'OpenPrice',
                        'HighPrice', 'LowPrice', 
                        'ClosePrice', 'Volume', 
                        'Value', 'Frequency'])

    for j in range (len(roll_data)) :
        i = roll_data[j]
        
        #logic if untuk nyamain dengan list Emiten+Open Price
        Open_Price = get_openPrice(List_Em_Open, i[3])
        #High
        if (i[4] == '00000000000.00') :
            High_Price = Open_Price 
        else :
            High_Price = i[4]
        #Low    
        if (i[5] == '00000000000.00') :
            Low_Price = Open_Price 
        else :
            Low_Price = i[5]
        #Close    
        if (i[6] == '00000000000.00') :
            Close_Price = Open_Price 
        else :
            Close_Price = i[4]

        data_dict = {'Tanggal' : i[0], 
                    'Waktu' : i[1],
                    'Sequence' : i[2],
                    'Emiten' : i[3],
                    'OpenPrice' : Open_Price,
                    'HighPrice' : High_Price, 
                    'LowPrice' : Low_Price, 
                    'ClosePrice' : Close_Price, 
                    'Volume' : i[7], 
                    'Value' : i[8], 
                    'Frequency' : i[9]}
        datafeed = datafeed.append(data_dict, ignore_index=True)

    # datafeed #Pengisian OpenPrice

    return datafeed

r = redis.Redis()
reType1 = []
reType2 = []
reType5 = []
roll_data = []
reType1, reType2, reType5, roll_data = filter_redis_paket(r)

# print(len(reType1))
# for i in reType1 :

#     print(i)
list_emiten, ListEm = get_list_emiten(reType5) #588
# print(len(list_emiten))
# for i in range (len(list_emiten)) :
#     print(list_emiten)

data = fill_open(reType1, list_emiten)

#List Emiten Open price, untuk RT 1
data

for i in (data.Emiten) :
    indexNames = ListEm[ListEm.Emiten == i].index
    ListEm.drop(indexNames, inplace=True)

list_emiten_now = ListEm.values.tolist()

list_emiten_now = f_one(list_emiten_now)


# In[53]:


#List Emiten Open price, untuk RT 2
now_data = fill_open(reType2, list_emiten_now)

# In[54]:


#List Emiten sisa

for i in (now_data.Emiten) :
    indexNames = ListEm[ListEm.Emiten == i].index
    ListEm.drop(indexNames, inplace=True)


# In[55]:


#List Emiten + Open yang ada

List_Em_Open = data.append(now_data, ignore_index = True)



# In[56]:


# df = pd.DataFrame(
#     columns= ['Tanggal', 'Waktu',
#              'Sequence', 'Emiten', 
#              'OpenPrice',
#              'HighPrice', 'LowPrice', 
#              'ClosePrice', 'Volume', 
#              'Value', 'Frequency'])


# In[57]:


# for j in range (len(roll_data)) :
#     i = roll_data[j]
    
#     #logic if untuk nyamain dengan list Emiten+Open Price
#     Open_Price = get_openPrice(List_Em_Open, i[3])
    
#     data_dict = {'Tanggal' : i[0], 
#                  'Waktu' : i[1],
#                  'Sequence' : i[2],
#                  'Emiten' : i[3],
#                  'OpenPrice' : Open_Price,
#                  'HighPrice' : i[4], 
#                  'LowPrice' : i[5], 
#                  'ClosePrice' : i[6], 
#                  'Volume' : i[7], 
#                  'Value' : i[8], 
#                  'Frequency' : i[9]}
#     df = df.append(data_dict, ignore_index=True)

# df #Pengisian OpenPrice


# In[58]:
datafet(roll_data, List_Em_Open)

# for i in df.HighPrice :
#     print(i)


# In[59]:




# In[101]:


# bring to key redis
# values= datafeed.values.tolist()
# for i in range (9999999) :
#     name_key = 'Data'+str(i)
#     r.mset({name_key:str(values[i])})


# In[ ]:


# delete key diatas
# for i in range (9999999) :
#     name_key = 'Data'+str(i)
#     r.delete(name_key)


# In[67]:


### kalau mau pake langsung tanpa di simpan ke key dalam redis
###
# import redis
# import time
# r = redis.Redis()
# data = datafeed.values.tolist()
# for i in range (999999999) :
#     row = data[i]
#     time.sleep(0.5)
#     r.publish('Paket_Satu',str(row[0:9]))


