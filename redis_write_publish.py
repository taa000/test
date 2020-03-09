import redis
import time
r = redis.Redis()
start = True
i = 0
data = datafeed.values.tolist()
while(start):
    row = data[i]
    time.sleep(0.5)
    # date, time, seq, emiten, open, high, low, close, volume
    r.publish('Paket_Satu',str(row[0:9]))
    # date, time, seq, emiten, open, high, low, close, volume, value
    r.publish('Paket_Dua',str(row[0:10]))
    # date, time, seq, emiten, open, high, low, close, volume, value, frequency
    r.publish('Paket_Tiga',str(row[0:11]))
    # date, time, seq, emiten, open, high, low, close, volume, value, frequency, net value foreign
    r.publish('Paket_Empat',str(row[0:12]))
    # date, time, seq, emiten, open, high, low, close, volume, value, frequency, net value foreign, net value local,
    r.publish('Paket_Lima',str(row[0:13]))
    # date, time, seq, emiten, open, high, low, close, volume, value, frequency, net value foreign, net value local, total net value
    r.publish('Paket_Enam',str(row[0:14]))
    i+=1


# on-the-way
# butuh redis dengan port baru :
# redis yang nyimpan data mentah : 127.0.0.1:6372
