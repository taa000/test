#!/usr/bin/env python
# coding: utf-8

# In[49]:


import time
import mysql.connector


# In[50]:
# Membuat database sesuai dengan <nameDB>

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
# Tes koneksi database sesuai dengan <nameDB>

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
#KOneksi untuk meng-insert tabel baru kedalam database
# untuk <DB_insert> nanti akan di call di <DB_createTable>

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



# Definisi create Tabel
# Tabel yang di buat ada 8 Tabel :
#     1. Tabel yang isinya semua data Record Type
#           Date, Time, Sequence, RecordType
#     2. Tabel RecordType 0-6
#           Sesuai dengan kategori RecordTypenya


def DB_createTable(nfield, nameDB) :
    curr, mydb = DB_Insert(nameDB)

    #Tabel untuk cek semua data
    sql_com = "CREATE TABLE recordtype (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL)"
    curr.execute(sql_com)
    mydb.commit()
    print ('Membuat tabel recordType : Sukses !!')

    #Tabel untuk record Type
    for field in range (nfield) :
        if (field==0) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, status  BIGINT NOT NULL, messag VARCHAR (100))"
        elif (field==1) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, ord_time BIGINT NOT NULL, ord_comm BIGINT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), broker_code VARCHAR(100), prices BIGINT NOT NULL, volume BIGINT NOT NULL, balance BIGINT NOT NULL, SB VARCHAR(100), ord_num BIGINT NOT NULL, best_bp BIGINT NOT NULL, best_bv BIGINT NOT NULL, best_op BIGINT NOT NULL, best_ov BIGINT NOT NULL, link_order BIGINT NOT NULL)"
        elif (field==2) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, trade_time BIGINT NOT NULL, trade_comm BIGINT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), trade BIGINT NOT NULL, prices BIGINT NOT NULL, volume BIGINT NOT NULL, buy_code VARCHAR(100), buy_type VARCHAR(100), sell_code VARCHAR(100), sell_type VARCHAR(100), BBP BIGINT NOT NULL, BBV BIGINT NOT NULL, BOP BIGINT NOT NULL, BOV BIGINT NOT NULL, buy_ord_no BIGINT NOT NULL, sell_ord_no BIGINT NOT NULL)"
        elif (field==3) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, sec_code VARCHAR(100), sec_name VARCHAR(100), sec_status BIGINT NOT NULL, sec_type VARCHAR(100), sub_sector BIGINT NOT NULL, ipo_prices BIGINT NOT NULL, base_price BIGINT NOT NULL, listed_shared BIGINT NOT NULL, trade_list_shared BIGINT NOT NULL, shared_lot BIGINT NOT NULL, remarks VARCHAR(100), sec_remark VARCHAR(100), weight BIGINT NOT NULL)"
        elif (field==4) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, broker_code VARCHAR(100), broker_name VARCHAR(100),broker_stat BIGINT NOT NULL)"
        elif (field==5) :  
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, sec_code VARCHAR(100), board_code VARCHAR(100), prev_price BIGINT NOT NULL, high_price BIGINT NOT NULL, low_price BIGINT NOT NULL, close_price BIGINT NOT NULL, change_val BIGINT NOT NULL, trade_vol BIGINT NOT NULL, trade_val BIGINT NOT NULL, trade_freq BIGINT NOT NULL, indi_index BIGINT NOT NULL, ofavail BIGINT NOT NULL, opening_price BIGINT NOT NULL, BBP BIGINT NOT NULL, BBV BIGINT NOT NULL, BOP BIGINT NOT NULL, BV BIGINT NOT NULL, average_pr BIGINT NOT NULL, sec_board_st BIGINT NOT NULL)"
        elif (field==6) :
            sql = "CREATE TABLE recordType_"+str(field)+" (dates BIGINT NOT NULL, timee BIGINT NOT NULL, sequence BIGINT NOT NULL, record_type BIGINT NOT NULL, index_code VARCHAR(100), ex_base_val BIGINT NOT NULL, ex_mark_val BIGINT NOT NULL, indexx BIGINT NOT NULL, open BIGINT NOT NULL, high BIGINT NOT NULL, low BIGINT NOT NULL, prev_index BIGINT NOT NULL )"
        msg = 'Membuat tabel recordType_' + str(field) + ' : Sukses !!'
        curr.execute(sql)
        mydb.commit()
        print(msg)


# In[61]:


def main():

    # NameofDB = nameDB ==> Jadi harus di ganti namanya semisalnya nama DB sebelumnya sudah ada
    nameofDB = '10March20'
    DB_createDB(nameofDB)
    DB_testConn(nameofDB)
    DB_createTable(7, nameofDB)

main()




