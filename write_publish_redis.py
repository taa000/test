import redis
import time
r = redis.Redis()
data = datafeed.values.tolist()
for i in range (999999999) :
    row = data[i]
    time.sleep(0.5)
    r.publish('Paket_Satu',str(row[0:9]))