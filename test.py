
import pandas as pd
import redis
import re
import numpy as np

def filter_redis_paket(r):
    rt1=[]
    rt2=[]
    rt5=[]

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
        if (rowdata[4]=='1') :
            data = [rowdata[1], rowdata[2], rowdata[3], rowdata[7], rowdata[10]]
            if (rowdata[2]>= '085500') and (rowdata[2] <='085559') :
                d = [rowdata[3], rowdata[7], rowdata[10]]
                rt1.append(d)
        elif(rowdata[4]=='2') :
            if (rowdata[2]>'085959') and (rowdata[6]=='0') :
                e = [rowdata[7], rowdata[10]]
                rt2.append(e)
        elif (rowdata[4]=='5') :
            # print(rowdata)
            if ((int(rowdata[2]) <= 85959) and (rowdata[6]=='RG')) :
                f =[rowdata[5]]
                rt5.append(f)
    return rt1, rt2, rt5

def get_list_emiten(rt5) :
    e = ""
    data = []
    rt5.sort()
    for i in (rt5) :
        if (i != e) :
            data.append(i)
            e = i
    # data = pd.DataFrame({'Emiten' :data})
    return data

def get_list_open1(rt1) :
    for i in range (len(rt1)) :
        row_rt1 = re.split('\,', str(rt1[i]))
        print (row_rt1[1])


def fill_open1(rt1, list_emiten) :
    for i in (list_emiten) :
        em = re.sub('\]','', str(i))
        em = re.sub('\[','', str(em))
        for j in range (len(rt1)) :
            row_rt1 = re.split('\,', str(rt1[j]))
            if (em == 'AGRO') :
                print("OK")
                

def

r = redis.Redis()
reType1 = []
reType2 = []
reType5 = []
reType1, reType2, reType5 = filter_redis_paket(r)

# print(len(reType1))
# for i in reType1 :
#     print(i)

list_emiten = get_list_emiten(reType5) #588
# print(len(list_emiten))
# for i in range (len(list_emiten)) :
#     print(list_emiten)

fill_open1(reType1, list_emiten)

# get_list_open1(reType1)


