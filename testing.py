import pandas as pd
import re 
import redis
import time

r = redis.Redis()

# proses untuk mendapatkan list emiten, sebelum jam 9.00
def get_list_emiten(rt5) :
    e = ""
    data = []
    rt5.sort()
    for i in (rt5) :
        if (i != e) :
            data.append(i)
            e = i
    return data

def crowing_data(rowdata) :
    nEmiten = []
    
    rt1 = []
    rt2 = []
    rt5 = []
    dataa = []
    net = []
    index = rowdata[3]
    # print(index)
    if (index == '1') :
        # data open >> bisa di eksekusi disaat 08.55 sekian
        if (rowdata[1]>= '085500') and (rowdata[1] <='085559') :
            d = [rowdata[2], rowdata[6], rowdata[9]]
            # print(d)
            rt1.append(d)
    elif (index =='2') :
        # data open diatas jam 9 pagi 
        if (rowdata[1] >= '090000') and (rowdata[5]=='0') :
            e = [rowdata[2],rowdata[6], rowdata[9]]
            # print(e)
            rt2.append(e)
        #data untuk perhitungan net value 
            if (rowdata[5] == '0') and (rowdata[7]=='RG') :
                h = [rowdata[0], rowdata[1], rowdata[2], rowdata[6], rowdata[9], rowdata[10],rowdata[12], rowdata[14]]
                # print(h)
                net.append(h)
    elif (index =='5') :
        #data emiten >> bisa di eksekusi disaat 08.55
        if (rowdata[1] <= '085500') and (rowdata[5]=='RG') :
            f =rowdata[4]
            # print(f)
            rt5.append(f)
        #data persiapan paket 
        if (rowdata[1] >= '085500') :
            g = [rowdata[0], rowdata[1], rowdata[2], rowdata[4],rowdata[7], rowdata[8], rowdata[9], rowdata[11], rowdata[12], rowdata[13]]
            # print(g)
            dataa.append(g)

    return rt5

data = []
start = True
j = 0
while(start) :
    if (r.get(j) == b'OK 172.16.3.158\\r\\n') or (r.get(j) == b"b'Welcome to Datafeed Server 172.16.3.158\\nOK 172.16.3.158\\r\\n") :
        i = j+1
        while(start) :
#             time.sleep(0.3)
            getRow = r.get(i)
            if (getRow != b"'") and (getRow != b"b''") and (getRow != b"\\n'") and (getRow != b""):
                getRow = getRow.decode('utf-8','ignone')
                getRow = re.sub("\b'","", str(getRow)) #jika ada data yang masih belum terdecoding
                getRow = re.sub('\s','', str(getRow)) #menghapus <space>
                rowData = re.split('\|',str(getRow)) #delimiter <|> sebagai pemisah antar colom
                rt5 = crowing_data(rowData) #proses seleksi per recordtype
                
                #selekse emiten
                data.append(get_list_emiten(rt5))
                print(data)                     
            i+=1

    j+=1



        