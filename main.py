import os

#file python
import database as dbase
import telnet_set_redis as set_data
import telnet_get_redis_to_db as get_data


### using scheduler to running set_data and get_data (pipelibn)
dbase.main()
# set_data.main()
