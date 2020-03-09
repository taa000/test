#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import re
import array
import csv


# In[2]:

# Koneksi DB ke nameDB

def aksesDB(sql, nameDB) :    
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        database = nameDB)
    act = mydb.cursor()
    act.execute(sql)
    result = act.fetchall()
    return result


# In[3]:

# Daftar emiten :
#     Hasil dari filter recordtype_5 : 
#         - data kurang dari jam 9 pagi
#         - data dengan boardcode RG
#     Hasilnya disimpan di <i_symbol>

def daftar_emiten(nameDB) :
    sql = "SELECT sec_code FROM recordtype_5 WHERE board_code = 'RG' AND timee < 090000 GROUP BY sec_code"
    result = aksesDB(sql,nameDB)
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

# DATABASE QUERY UNTUK OPEN DATA ( RECORD TYPE 1 DAN RECORD TYPE 2)
def open_rt1(nameDB) :
    sql = "SELECT dates,timee, ord_comm,sec_code, prices FROM `recordtype_1` WHERE timee < 090000"
    result = aksesDB(sql,nameDB)
    return result
# In[5]:
def open_rt2(nameDB) :
    sql = "SELECT dates,timee, board_code, sec_code, prices FROM `recordtype_2` WHERE trade_comm = 0 AND timee > 085500"
    result = aksesDB(sql, nameDB)
    return result
# In[6]:

# FILTER DATA OPEN DI RT1 OR RT2
#     RESULT = HASIL QUERY DARI DB
#     ROW[3] = EMITEN
#     ROW[4] = OPEN PRICE
#     J_DATA = SEMUA [EMITEN + OPEN PRICE] DI RT1 OR RT2


def filter_open(result) :
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

# SELEKSI EMITEN YANG KOSONG
#     J_DATA = DAFTAR EMITEN + PRICE
#     I_SYMBOL = DAFTAR EMITEN 
#     SISA = DAFTAR EMITEN DI I_SYMBOL YANG SAMA DENGAN EMITEN DI J_DATA
#     I_DATA = DAFTAR OPEN PRICE + EMITEN YANG SAMA DENGAN DAFTAR DI I_SYMBOL
# HASILNYA : I_DATA= DAFTAR OPENPRICE+EMITEN YANG BARU ; I_SYMBOL = DAFTAR EMITEN SISA SUDAH DI REMODE DENGAN SISA


def emiten_kosong(j_data,i_symbol) :    
    sisa = []
    i_data = []
    for x in range(len(i_symbol)):
        for y in range(len(j_data)) :
            symbol, open = j_data[y]
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

# gabungan semua proses untuk dapet open price
# open price : 
#     - recordtype_1
#     - recordtype_2
# Hasilnya : open = daftar openprice+emiten ; a = daftar emiten sisa yang openpricenya adalah 0

def daftar_open_price(nEmiten, nameDB):
    rt1 = open_rt1(nameDB) 
    filter_rt1= filter_open(rt1) 
    open_null,open = emiten_kosong(filter_rt1, nEmiten)
  
    rt2 = open_rt2(nameDB)
    filter_rt2= filter_open(rt2)
    a,b = emiten_kosong(filter_rt2, open_null)
    
    open = append_list(open,b)
    return open, a



# In[12]:
def open_rt5(nameDB):
    sql = "SELECT dates, timee, sec_code, high_price, low_price, close_price, trade_vol, trade_val, trade_freq FROM `recordtype_5` WHERE board_code = 'RG' AND timee > 085959"
    result = aksesDB(sql, nameDB)
    return result


# In[13]:


def paket(dataOpen, nameDB):
    rt5 = open_rt5(nameDB)
    data_pk1, data_pk2, data_pk3 = filter_paket(rt5, dataOpen)
    return data_pk1, data_pk2, data_pk3


# In[14]:
# SEBELUM INSERT OPEN PRICE
# ROW[2] = EMITEN
# ROW[3] = HIGH PRICE
# ROW[4] = LOW PRICE
# ROW[5] = CLOSE
# ROW[6] = VOLUME
# ROW[7] = VALUE
# ROW[8] = FREQUENCY



def filter_paket(result, dataOpen):
    pk1 = []
    pk2 = []
    pk3 = []

    # membagi data dari daftar openprice+emiten
    #     a = data emiten
    #     b = data open price

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
        row.insert(3,'0') #Insert Open Price
        for i in range (len(a)) :
            if (row[2]==a[i]) :
                row[3] = b[i]
                if (row[4]=='0') : 
                    row[4]=b[i]
                if (row[5]=='0') :
                    row[5]=b[i]
                if (row[6]=='0') :
                    row[6]=b[i] 


        # Paket 1            
        data_paket1 = row[0:8]
        pk1.append(data_paket1)
        # Paket 2            
        data_paket2 = row[0:9]
        pk2.append(data_paket2)
        # Paket 3            
        data_paket3 = row[0:10]
        pk3.append(data_paket3)

    return pk1, pk2, pk3


def save_to_txt(nameFile, paket_data1 ):
    with open(nameFile,'a') as files :
        for i in range (len(paket_data1)) :
            print(paket_data1[i])
         #save to file txt
            write = csv.writer(files)
            write.writerow(paket_data1[i])

# In[16]:


def main() :
    # name DB : nama db yang akan di eksekusi
    nameDB = '5March20'

    ## daftar emiten
    nEmiten = daftar_emiten(nameDB)
    
    # daftar open prices
    openPrice, nullOpen = daftar_open_price(nEmiten, nameDB)
#     print(openPrice)  
#     for i in range(len(nullOpen)):monitor
#         print(nullOpen[i])

    paket_data1, paket_data2, datapaket3 = paket(openPrice, nameDB)

#   DIPILIH YANG DI SAVE PAKET DATA BERAPA
    nameFile = 'data_belum_divalidasi/5March20-fromDB.txt'
    save_to_txt(nameFile,paket_data1)
    
main()



