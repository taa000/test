#!/usr/bin/env python
# coding: utf-8

# In[115]:


import re 
import redis
import time
import pandas as pd
import numpy as np

r = redis.Redis() 


# In[116]:


# kumpulam rowdata[4] = emiten di RT 5
def appendEmit(emiten, EM) :
    return EM.append(emiten)


# In[117]:


#seleksi emitennya cuyy
def selecUnique(EM) :
    e = ''
    data = []
    EM.sort()
    for i in (EM) :
        if (i != e) :
            data.append(i)
            e = i
    return data


# In[118]:


# kumpul semua harga open dan emitennya di RT1 dan RT2
def appendOpen(emit, selec, openp, OpenRT) :
    data = emit, selec, openp
    return OpenRT.append(data)


# In[119]:


#kumpul data di rt5
def appendData(date, time, sequence, emiten, highp, lowp, closep, volume, value, freq, dataSeries) : 
    data = date, time, sequence, emiten, highp, lowp, closep, volume, value, freq
    return dataSeries.append(data)


# In[120]:


# kumpul net value
def appendNet(date, time, sequence, emiten, netValue, buyType, sellType, Val):
    data = date, time, sequence, emiten, netValue, buyType, sellType
    return Val.append(data)


# In[121]:


# Data Emiten
def DataEmiten(EM) :
    df_EM = pd.DataFrame({'Emiten' : EM})
    # print(df_EM) #print disini untuk cek all emiten


# In[122]:


###Proses Pengumpulan Dataset untuk OpenPrice####
def OpenOrdSatu(OpenRT1, DataOpenPrice) :
    df_OpenRT1 = pd.DataFrame(OpenRT1,columns = ['SelecEm1', 'Emiten', 'OpenPrice'])
    Emiten1 = df_OpenRT1.SelecEm1.unique() #Mencari emiten yang unik
    # Emiten1

    OpenRT1_Unique = df_OpenRT1.groupby('SelecEm1').first() #Emiten dengan Open Price awal
    # print(OpenRT1_Unique)
    #Proses Pemindahan OpenRT1 ke DataOpenPrice
    for i in range (len(DataOpenPrice)) :
        for j in range (len(OpenRT1_Unique)) :
            if (DataOpenPrice.Emiten[i] == OpenRT1_Unique.Emiten[j]) :
                DataOpenPrice.OpenPrice[i] = OpenRT1_Unique.OpenPrice[j]
#     print(DataOpenPrice)


# In[123]:


def OpenOrdDua(OpenRT2, DataOpenPrice) :    
    df_OpenRT2 = pd.DataFrame(OpenRT2,columns = ['SelecEm2', 'Emiten', 'OpenPrice'])
    Emiten2 = df_OpenRT2.SelecEm2.unique() #Mencari emiten yang unik
    # Emiten1

    OpenRT2_Unique = df_OpenRT2.groupby('SelecEm2').first() #Emiten dengan Open Price awal
    # OpenRT2

    #Proses Pemindahan OpenRT2 ke DataOpenPrice
    
    for i in range (len(DataOpenPrice)) :
        for j in range (len(OpenRT2_Unique)) :
            if (DataOpenPrice.Emiten[i] == OpenRT2_Unique.Emiten[j]) and (DataOpenPrice.OpenPrice[i] == None) :
                DataOpenPrice.OpenPrice[i] = OpenRT2_Unique.OpenPrice[j]
     #DataOpenPrice.values.tolist()


# In[124]:


#get open berdasatkan emiten yang masuk 
def getOpen(emiten, DataOpenPrice) :
    # DataOPenPrice = DataOpenPrice.values.tolist()
    for i in range (len(DataOpenPrice)) :
        if (DataOpenPrice.Emiten[i] == emiten) :
            return DataOpenPrice.OpenPrice[i]


# In[138]:


OpenRT1 = []
OpenRT2 = []
dataSeries = []
Val = []
EM =[]
open = 'OK'



start = True
j = 0
while(start) :
    if (r.get(j) == b'OK 172.16.3.158\\r\\n') or (r.get(j) == b"b'Welcome to Datafeed Server 172.16.3.158\\nOK 172.16.3.158\\r\\n") :
        i = j+1
        while(start) :
            #time.sleep(0.3)
            getRow = r.get(i)
            if (getRow != b"'") and (getRow != b"b''") and (getRow != b"\\n'") and (getRow != b""):
                getRow = getRow.decode('utf-8','ignone')
                getRow = re.sub("\b'","", str(getRow)) #jika ada data yang masih belum terdecoding
                getRow = re.sub('\s','', str(getRow)) #menghapus <space>
                rowData = re.split('\|',str(getRow)) #delimiter <|> sebagai pemisah antar colom
                if rowData[5] == "Endsendingrecords" : #Memberhentikan Looping
                    start = False
                DataOpenPrice = pd.DataFrame({'Emiten' : EM, 'OpenPrice' : None})
                if (rowData[3] == '1') or (rowData[3] == '2') or (rowData[3] == '5') :
                    
                   # Stock Code / Security Code  :
                   # (RT 5 field 5.1) (all stock code akan di feed sebelum jam perdagangan dibuka)
                    
                    if (rowData[3] == '5') and (rowData[1] < '085500') and (rowData[5] == 'RG') :  #Mengumpulkan emiten
                        appendEmit(rowData[4], EM)
                        
                   # Open:
                   # Aturan 1 : ambil pada kolom Price (RT 1 field 1.6) emiten yang di jam 8:55 
                   #         terdapat harga pada field price nya, harga tersebut digunakan sebagai harga Open
                   # Aturan 2: Untuk emiten yang tidak ada pada field Price (RT 1 field 1.6), 
                   #     gunakan (RT 2, field 2.2) yang memiliki nilai "0" lalu ambil data pada 
                   #     kolom (RT 2 field 2.6) yang  pertama tampil setelah jam 08.59.59 untuk digunakan sebagai harga Open

                    if (rowData[3] == '1') and (rowData[1] <= '085959') and (rowData[1] >= '085500') and (rowData[7] == 'RG') and (rowData[5] == '0') :
                        appendOpen(rowData[6], rowData[6], rowData[9], OpenRT1)
                        
                        
                    if (rowData[3] == '2') and (rowData[1] > '085959') and (rowData[7] == 'RG') and (rowData[5] == '0') :
                        appendOpen(rowData[6], rowData[6], rowData[9], OpenRT2)
                       
                    # High: Gunakan data pada RT 5 field 5.4 (Jika 0 maka gunakan harga Open)
                    # Low: Gunakan data pada RT 5 field 5.5 (Jika 0 maka gunakan harga Open)
                    # Close: Gunakan data pada RT 5 field 5.6 (Jika 0 maka gunakan harga Open)
                    # Volume: Gunakan data pada RT 5 field 5.8 (Jika 0 maka tetap 0)
                    # Value: Gunakan data pada RT 5 field 5.9 (Jika 0 maka tetap 0)
                    # Frequency: Gunakan data pada RT 5 field 5.10 (Jika 0 maka tetap 0)
                    
                    
                    
                    if (rowData[3]=='5') and (rowData[1] > '085959') and (rowData[5] == 'RG') :
                        #Masih Belum ada data Open_Pricenya
                        appendData(rowData[0], rowData[1], rowData[2], rowData[4], rowData[7], rowData[8], rowData[9], rowData[11], rowData[12], rowData[13], dataSeries)
                        
                        #Database paket
                        
                        open = getOpen(rowData[4], DataOpenPrice)
                        if open == None :
                            open = rowData[6] #Prev_Price dari RT5
                        if rowData[7] == '00000000000.00' :
                            high = open
                        if rowData[8] == '00000000000.00' :
                            low = open
                        if rowData[9] == '00000000000.00' :
                            close = open
                            
                        getData = [rowData[0], rowData[1], rowData[2], rowData[4], open, high, low, close, rowData[11], rowData[12], rowData[13]]
                        # time.sleep(0.5)
                        r.publish('testing', str(getData))
                        # print(getData)        
                        
                #Kalau pakai pubsub
                    # Net Value <ON THE WAY>
                    if (rowData[3]=='2') and (rowData[7]=='RG') and (rowData[5] == '0') :
                        netValue = float(rowData[9]) * float(rowData[10])
                        appendNet(rowData[0], rowData[1], rowData[2], rowData[6], netValue, rowData[12], rowData[14], Val)
                  
                    
            i+=1
        DataEmiten(EM) #Udah OK, kalau mau liat hasilnya print di bagian functionnya aja
        # DataOpenPrice = pd.DataFrame({'Emiten' : EM, 'OpenPrice' : None})
        OpenOrdSatu(OpenRT1, DataOpenPrice)
        OpenOrdDua(OpenRT2,DataOpenPrice)
        
        
    j+=1
# 


# In[ ]:


# gausah di pake lagi ini < kalau ga di pake nanti bsa di hapus appendData, biar efektif
# df_dataSeries = pd.DataFrame(dataSeries, columns = ['date', 'time', 'sequence', 'emiten', 'high',  'low', 'close','volume', 'value', 'frequency'])
# df_dataSeries


# In[ ]:


# Keperluan Net Value
# df_Val = pd.DataFrame(Val, columns = ['Date', 'Time', 'Sequence', 'Emiten', 'netValue', 'buyType', 'sellType'])
# df_Val


# In[ ]:


# df_Val.groupby('Emiten')[['netValue']].sum()


# In[ ]:


# emitenGroup = df_Val.groupby('Emiten')
# for df_Val in emitenGroup :
#     print(df_Val)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




