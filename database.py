#!/usr/bin/env python
# coding: utf-8

# In[49]:


import time
import mysql.connector


# In[50]:


def DB_createDB(nameDB):
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = ''
    )
    curr = mydb.cursor()
    curr.execute("CREATE DATABASE "+nameDB)
    return print('Membuat '+nameDB+' Database : Sukses !!')


# In[51]:


def DB_testConn(nameDB):
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        database = nameDB
        )
    curr = mydb.cursor()
    return print ('Tes Koneksi '+nameDB+ ' Database : Sukses !!')


# In[59]:


def DB_Insert(nameDB):
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        database = nameDB
        )
    curr = mydb.cursor()
    return curr, mydb  


# In[60]:


def DB_createTable(nfield, nameDB) :
    curr, mydb = DB_Insert(nameDB)
    for field in range (nfield) :
        if (field==0) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, status  INT NOT NULL, messag VARCHAR (100))"
        elif (field==1) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, ord_time INT NOT NULL, ord_comm INT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), broker_code VARCHAR(100), prices INT NOT NULL, volume INT NOT NULL, balance INT NOT NULL, SB VARCHAR(100), ord_num INT NOT NULL, best_bp INT NOT NULL, best_bv INT NOT NULL, best_op INT NOT NULL, best_ov INT NOT NULL, link_order INT NOT NULL)"
        elif (field==2) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, trade_time INT NOT NULL, trade_comm INT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), trade INT NOT NULL, prices INT NOT NULL, volume INT NOT NULL, buy_code VARCHAR(100), buy_type VARCHAR(100), sell_code VARCHAR(100), sell_type VARCHAR(100), BBP INT NOT NULL, BBV INT NOT NULL, BOP INT NOT NULL, BOV INT NOT NULL, buy_ord_no INT NOT NULL, sell_ord_no INT NOT NULL)"
        elif (field==3) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, sec_code VARCHAR(100), sec_name VARCHAR(100), sec_status INT NOT NULL, sec_type VARCHAR(100), sub_sector INT NOT NULL, ipo_prices INT NOT NULL, base_price INT NOT NULL, listed_shared INT NOT NULL, trade_list_shared INT NOT NULL, shared_lot INT NOT NULL, remarks VARCHAR(100), sec_remark VARCHAR(100), weight INT NOT NULL)"
        elif (field==4) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, broker_code VARCHAR(100), broker_name VARCHAR(100),broker_stat INT NOT NULL)"
        elif (field==5) :  
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), prev_price INT NOT NULL, high_price INT NOT NULL, low_price INT NOT NULL, close_price INT NOT NULL, change_val INT NOT NULL, trade_vol INT NOT NULL, trade_val INT NOT NULL, trade_freq INT NOT NULL, indi_index INT NOT NULL, ofavail INT NOT NULL, opening_price INT NOT NULL, BBP INT NOT NULL, BBV INT NOT NULL, BOP INT NOT NULL, BV INT NOT NULL, average_pr INT NOT NULL, sec_board_st INT NOT NULL)"
        elif (field==6) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, index_code VARCHAR(100), ex_base_val INT NOT NULL, ex_mark_val INT NOT NULL, indexx INT NOT NULL, open INT NOT NULL, high INT NOT NULL, low INT NOT NULL, prev_index INT NOT NULL )"
        msg = 'Membuat tabel recordType_' + str(field) + ' : Sukses !!'
        curr.execute(sql)
        mydb.commit()
        print(msg)


# In[ ]:


def DB_createTable(nfield, nameDB) :
    curr, mydb = DB_Insert(nameDB)
    for field in range (nfield) :
        if (field==0) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, status  INT NOT NULL, messag VARCHAR (100))"
        elif (field==1) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, ord_time INT NOT NULL, ord_comm INT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), broker_code VARCHAR(100), prices INT NOT NULL, volume INT NOT NULL, balance INT NOT NULL, SB VARCHAR(100), ord_num INT NOT NULL, best_bp INT NOT NULL, best_bv INT NOT NULL, best_op INT NOT NULL, best_ov INT NOT NULL, link_order INT NOT NULL)"
        elif (field==2) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, trade_time INT NOT NULL, trade_comm INT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), trade INT NOT NULL, prices INT NOT NULL, volume INT NOT NULL, buy_code VARCHAR(100), buy_type VARCHAR(100), sell_code VARCHAR(100), sell_type VARCHAR(100), BBP INT NOT NULL, BBV INT NOT NULL, BOP INT NOT NULL, BOV INT NOT NULL, buy_ord_no INT NOT NULL, sell_ord_no INT NOT NULL)"
        elif (field==3) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, sec_code VARCHAR(100), sec_name VARCHAR(100), sec_status INT NOT NULL, sec_type VARCHAR(100), sub_sector INT NOT NULL, ipo_prices INT NOT NULL, base_price INT NOT NULL, listed_shared INT NOT NULL, trade_list_shared INT NOT NULL, shared_lot INT NOT NULL, remarks VARCHAR(100), sec_remark VARCHAR(100), weight INT NOT NULL)"
        elif (field==4) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, broker_code VARCHAR(100), broker_name VARCHAR(100),broker_stat INT NOT NULL)"
        elif (field==5) :  
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), prev_price INT NOT NULL, high_price INT NOT NULL, low_price INT NOT NULL, close_price INT NOT NULL, change_val INT NOT NULL, trade_vol INT NOT NULL, trade_val INT NOT NULL, trade_freq INT NOT NULL, indi_index INT NOT NULL, ofavail INT NOT NULL, opening_price INT NOT NULL, BBP INT NOT NULL, BBV INT NOT NULL, BOP INT NOT NULL, BV INT NOT NULL, average_pr INT NOT NULL, sec_board_st INT NOT NULL)"
        elif (field==6) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates INT NOT NULL, timee INT NOT NULL, sequence INT NOT NULL, record_type INT NOT NULL, index_code VARCHAR(100), ex_base_val INT NOT NULL, ex_mark_val INT NOT NULL, indexx INT NOT NULL, open INT NOT NULL, high INT NOT NULL, low INT NOT NULL, prev_index INT NOT NULL )"
        msg = 'Membuat tabel recordType_' + str(field) + ' : Sukses !!'
        curr.execute(sql)
        mydb.commit()
        print(msg)


# In[61]:


def main():
    nameofDB = 'anotherDB'
    DB_createDB(nameofDB)
    DB_testConn(nameofDB)
    DB_createTable(7, nameofDB)


# In[62]:


main()


# In[ ]:




