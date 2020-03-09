import re 
import redis
import time
import csv

r = redis.Redis()
# file : nama file txt yang akan di simpan
file = 'data_belum_divalidasi/Marc09-12Siang.txt'

# tipe 'a' = append artinya row yang akan di baca kemudian di tambahkan.
with open(file,'a') as files :
    write = csv.writer(files)

    start = True
    j = 0
    while(start) :
        if (r.get(j) == b'OK 172.16.3.158\\r\\n') or (r.get(j) == b"b'Welcome to Datafeed Server 172.16.3.158\\nOK 172.16.3.158\\r\\n") :
            i = j+1
            print("Start get data ...")
            while(start) :  
                # time.sleep(0.3)
                getRow = r.get(i)
                #filter keys data sampah
                if (getRow != b"'") and (getRow != b"b''") and (getRow != b"\\n'") and (getRow != b"") and (getRow != "b""") and (getRow != b"b'") and (getRow != None): #bisa ditambahkan lagi filternya
                    getRow = getRow.decode('utf-8','ignone')
                    getRow = re.sub("\b","", str(getRow)) #jika ada data yang masih belum ter-decoding
                    getRow = re.sub('\s','', str(getRow)) #menghapus <space>
                    rowData = re.split('\|',str(getRow)) #delimiter <|> sebagai pemisah antar colom
                    
                    #proses penulisan data dalam txt per row
                    write.writerow(rowData)    
                i+=1
        j+=1
    print("All data in redis ... OK!")


# "b""" masih belum bisa
