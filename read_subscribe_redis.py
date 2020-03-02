#!/usr/bin/env python
# coding: utf-8



import redis


# In[4]:


r = redis.Redis()
p = r.pubsub()
p.subscribe('Paket_Satu')
start = True
while (start) :
    message = p.get_message() 
    if message: 
        command = message['data']
        print(command)
    else :
        start == False


# In[ ]:




