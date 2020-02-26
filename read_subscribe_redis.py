#!/usr/bin/env python
# coding: utf-8

# In[2]:


import redis


# In[4]:


r = redis.Redis()
p = r.pubsub()
p.subscribe('Paket_Satu')
for i in range (9999999):
    message = p.get_message() 
    if message: 
        command = message['data']
        print(command)


# In[ ]:




