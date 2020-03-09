#!/usr/bin/env python
# coding: utf-8

# In[11]:


import telnetlib 
import time 
import redis


# In[12]:

#   Konksi telnet ke Host dan Port
def telnet_conn(HOST,PORT) :
    tn = telnetlib.Telnet(HOST,PORT) 
    #input password cred, salah satu saja yang di gunakan
    tn.write(b'tcsre121|jakarta123|1 r\n') 
    # tn.write(b'tcsre122|jakarta123|2 r\n') 
    # tn.write(b'tcsrd121|jakarta123|3 r\n') 
    # tn.write(b'tcsrd122|jakarta123|4 r\n') 
    tn.set_debuglevel(1) 
    return tn


# In[13]:

#   membaca output telnet dam set ke key dalam redis
def read_data_to_redis(tn, r):
    i = 0
    while True: 
        tn_read = tn.read_very_eager()
        # output adalah hasil pembacaan telnet 
        # exc : running code di 8.30 maka telnet akan di set dari jam awal data sampai jam 8.30.
        #       kemudian akan ter-update sesuai dengan waktunya, jika pada detik tertentu tidak
        #       terdapat data makan outputnya adalah b'"'"
        output = repr(tn_read)
        output = output.split('IDX|')
        for x in range (len(output)):
            time.sleep(0.1)
            if ((output[x] != b'') or (output[x] != "\"") or (output[x] != '') or (output[x] != None) ): 
                r.mset({i:output[x]}) # set output kedalam key <i>
                i+=1


# In[14]:


def main() :
    r = redis.Redis()
    r.flushall() #penghapusan all data dalam redis

    # Data HOST : 172.18.2.213
    # Data PORT : 9010, 9040

    HOST = '172.18.2.213'
    PORT = 9010
    # PORT = 901=40

    tn = telnet_conn(HOST,PORT)
    read_data_to_redis(tn,r)

main()



