import redis
import re
import numpy as np
import pandas as pd
import time
import array 

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
        if ((gi == None) or (gi == '')):
            golive = False
        data = go.decode('utf-8','ignore')
        data = re.sub('\s','', str(data))
        data = re.sub("\b'","", str(data))
        rowdata = re.split('\|',str(data))
        recordtype = rowdata[4:5]
        # if (recordtype==['1']) :
            # data = [rowdata[1], rowdata[2], rowdata[3], rowdata[7], rowdata[10]]
            # if (rowdata[2]>= '085500') and (rowdata[2] <='085559') :
            #     d = [rowdata[3], rowdata[7], rowdata[10]]
            #     rt1.append(d)
        if(recordtype==['2']) :
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

def f_one(ListEm) :
    data = []
    for i in (ListEm) :
        i = re.sub('\[','', str(i))
        i = re.sub('\]','', str(i))
        i = re.sub('\'','', str(i))
        data.append(i)
    return data

def get_openPrice(List_Em_Open, emiten) :
    for x in range (len(List_Em_Open.Emiten)) :
        if (List_Em_Open.Emiten[x] == emiten) :
#             Open_Price = List_Em_Open.OpenPrice[x]
            return List_Em_Open.OpenPrice[x]

def datafet(roll_data, List_Em_Open, r):
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
    
    # for i in datafeed :
    #     r.publish('Paket_Satu', str(i))
    # # datafeed #Pengisian OpenPrice

    return datafeed


def publish_redis(datafeed,r) :
    data = datafeed.values.tolist()
    for i in range (999999999) :
        row = data[i]
        if (row != None ) :
            time.sleep(0.5)
            r.publish('Paket_Satu',str(row[0:9]))
            print(str(row[0:9]))


# def pub(r, data) :
#     data = row[0:9]
#     return r.publish('Paket_Satu',str(row[0:9]))

def main() :
    r = redis.Redis()
    reType1 = []
    reType2 = []
    reType5 = []
    roll_data = []
    reType1, reType2, reType5, roll_data = filter_redis_paket(r)

    list_emiten, ListEm = get_list_emiten(reType5) #588

    data = fill_open(reType1, list_emiten)

    for i in (data.Emiten) :
        indexNames = ListEm[ListEm.Emiten == i].index
        ListEm.drop(indexNames, inplace=True)

    list_emiten_now = ListEm.values.tolist()

    list_emiten_now = f_one(list_emiten_now)

    now_data = fill_open(reType2, list_emiten_now)

    for i in (now_data.Emiten) :
        indexNames = ListEm[ListEm.Emiten == i].index
        ListEm.drop(indexNames, inplace=True)

    List_Em_Open = data.append(now_data, ignore_index = True)

    datafeed = datafet(roll_data, List_Em_Open, r)

    # data = datafeed.values.tolist()
    # for i in data:
    #     print(i)
  
    publish_redis(datafeed, r)

main()
