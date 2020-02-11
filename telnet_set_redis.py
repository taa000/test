#!/usr/bin/env python
# coding: utf-8

# In[11]:


import telnetlib 
import time 
import redis


# In[12]:


def telnet_conn(HOST,PORT) :
    tn = telnetlib.Telnet(HOST,PORT) 
    tn.write(b'tcsre122|jakarta123|2 \r\n') 
    tn.set_debuglevel(1) 
    return tn


# In[13]:


def read_data_to_redis(tn, r):
    while True: 
        tn_read = tn.read_very_eager()
        time.sleep(1)  
        output = repr(tn_read)
        output = output.split('\\n')
        for x in range (len(output)):
            r.mset({x:output[x]})


# In[14]:


def main() :
    r = redis.Redis()
    # r.flushall()

    HOST = '172.18.2.213'
    PORT = 9010
    tn = telnet_conn(HOST,PORT)
    read_data_to_redis(tn,r)


# In[15]:


main()


# In[ ]:




