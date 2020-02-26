#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import re
import array


# In[2]:


def aksesDB(sql) :    
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        database = 'test ')
    act = mydb.cursor()
    act.execute(sql)
    result = act.fetchall()
    return result


# In[3]:


def daftar_emiten() :
    sql = "SELECT sec_code FROM recordtype_5 WHERE board_code = 'RG' AND timee < 090000 GROUP BY sec_code"
    result = aksesDB(sql)
    i_symbol=[]
    for i in result :
        i = re.sub('\,','',str(i)) #filter koma
        i = re.sub('\s','',str(i)) #filter spacing
        i = re.sub('\(','',str(i)) #filter (
        i = re.sub('\)','',str(i)) #filter )
        i = re.sub('\'','',str(i)) #filter '
        i_symbol.append(i)
    return i_symbol


# In[4]:


def open_rt1() :
    sql = "SELECT dates,timee, ord_comm,sec_code, prices FROM `recordtype_1` WHERE timee < 090000"
    result = aksesDB(sql)
    return result


# In[5]:


def open_rt2() :
    sql = "SELECT dates,timee, board_code, sec_code, prices FROM `recordtype_2` WHERE trade_comm = 0 AND timee > 085500"
    result = aksesDB(sql)
    return result


# In[6]:


def filter_open(result) :
    j_symbol = []
    j_open = []
    j_data = []
    for j in result :
        row = re.split('\,',str(j))
        row[3] = re.sub('\s','', str(row[3]))
        row[3] = re.sub('\'','',str(row[3]))
        row[4] = re.sub('\s','', str(row[4]))
        row[4] = re.sub('\)','', str(row[4]))
        data = (row[3],row[4])
        j_data.append(data)
    return j_data


# In[7]:


def emiten_kosong(j_data,i_symbol) :    
    sisa = []
    i_data = []
    for x in range(len(i_symbol)):
#         print(i_symbol[x])
        for y in range(len(j_data)) :
            symbol, open = j_data[y]
#             print(symbol +" AND "+ i_symbol[x])
            if (i_symbol[x]==symbol) :
                symbol = i_symbol[x]
                sisa.append(symbol)
                data = (symbol, open)
                i_data.append(data)
                break

#     print("banyak emiten : ", (len(i_symbol)))
#     print("emiten terisi : ", (len(sisa)))
#     print("emiten kosong : ", (len(i_symbol)-len(sisa)))

#     for i in range(len(sisa)) :
#         i_symbol.remove(sisa[i])

    i_symbol = remove_list(i_symbol,sisa)
#     print(len(i_symbol))
#     print(i_symbol)
    return i_symbol, i_data


# In[8]:


def remove_list(a,b) :
    for i in range(len(b)) :
        a.remove(b[i])
    return a


# In[9]:


def append_list(a,b) :
    for i in range(len(b)) :
        a.append(b[i])
    return a


# In[10]:


def bagiData(data) :
    La = []
    Lb = []
    for i in range (len(data)) :
        a,b = data[i]
        La.append(a)
        Lb.append(b)
    return La,Lb


# In[11]:


def daftar_open_price(nEmiten):
    rt1 = open_rt1()
    filter_rt1= filter_open(rt1)
    open_null,open = emiten_kosong(filter_rt1, nEmiten)
  
    rt2 = open_rt2()
    filter_rt2= filter_open(rt2)
    a,b = emiten_kosong(filter_rt2, open_null)
    
    open = append_list(open,b)
    return open,a


# In[12]:


def open_rt5():
    sql = "SELECT dates, timee, sec_code, high_price, low_price, close_price, trade_vol, trade_val, trade_freq FROM `recordtype_5` WHERE board_code = 'RG' AND timee > 085959"
    result = aksesDB(sql)
    return result


# In[13]:


def paket_satu(dataOpen):
    rt5 = open_rt5()
    data_pk1 = filter_pk1(rt5, dataOpen)
    return data_pk1


# In[14]:


def filter_pk1(result, dataOpen):
    all_data = []
    a,b = bagiData(dataOpen)
#     print(len(result))
    for j in result :
        row = re.split('\,',str(j))
        row[2] = re.sub('\s','', str(row[2]))
        row[2] = re.sub('\'','', str(row[2]))
        row[3] = re.sub('\s','', str(row[3]))
        row[4] = re.sub('\s','', str(row[4]))
        row[5] = re.sub('\s','', str(row[5]))
        row[6] = re.sub('\s','', str(row[6]))
        row[7] = re.sub('\s','', str(row[7]))
        row[8] = re.sub('\s','', str(row[8]))
        for i in range (len(a)) :
            if (row[2]==a[i]) :
                if (row[3]=='0') : 
                    row[3]=b[i]
                if (row[4]=='0') :
                    row[4]=b[i]
                if (row[5]=='0') :
                    row[5]=b[i]
                data = row[0:9]
        data = row[0:9]
        all_data.append(data)
    
#     for i in range(len(all_data)):
#         print(all_data[i])
        
    return all_data


# In[16]:


def main() :
    ## daftar emiten
    nEmiten = daftar_emiten()
    
    # daftar open prices
    openPrice, nullOpen = daftar_open_price(nEmiten)
#     print(openPrice)  
#     for i in range(len(nullOpen)):
#         print(nullOpen[i])

    # deal paket 1 : date, code, open, high, low, close, volume
    paket_data1 = paket_satu(openPrice)
    for i in range (len(paket_data1)) :
        print(paket_data1[i])
    
main()


# In[ ]:




