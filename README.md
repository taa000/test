#PROGRES 
<p>
__[DONE]__
database.py = membuat DB, cek koneksi DB, Membuat Tabel RT
>nameofDB>: nama DB yang akan di eksekusi
<p>   
__[DONE]__
telnet_set_redis.py = koneksi telnet, set output ke dalam key redis
>connection refused : cek PORT, dan passwordnya, cek ping HOST, cek VPN sudah aktif
>untuk tau data sudah tersimpan ke redis :
>      cmd -> redis-cli -> monitor 
<p>   
__[DONE]__   
get_redis_to_txt.py = export all key di redis ke dalam file .txt
  jika ada data yang di kategorikan sampah bisa di masukan ke filter >> cek code >> getRow != <filter>
  <file> : nama file yang akan di eksekusi, sifat penyimpanan 'append' artinya row akan di tambahkan
<p>     
__[OTW]__
redis_write_publish.py = publish redis sesuai dengan paketnya.
   lihat paket >> Output Paket
<p>   
__[OTW]__
redis_read_subscribe.py = membaca/ subs redis berdasarkan channel (atau paket)
    channel = paket >> Output Paket
<p>   
__[OTW]__
filter_paket_from_db.py = filter paket 1-6 dari DB
  <nameDB> : nama DB yang akan di eksekusi
  NamaFile, PaketData = bisa di pilih mau yang mana di export ke TXT
  paket yang ready masih paket 1-3
<p>   
__[OTW]__
get_redis_to_db.py = get redis hasil filter kemudian insert ke DB, filter berdasarkan RecordType
<p>    
__[FAILED]__
telnet_to_db.py ==> ga bisa baca decode hasil telnet.
  

#Chaneel >> Output Paket : 
    Paket_Satu  : date, time, seq, emiten, open, high, low, close, volume
    Paket_Dua   : date, time, seq, emiten, open, high, low, close, volume, value
    Paket_Tiga  : date, time, seq, emiten, open, high, low, close, volume, value, frequency
    Paket_Empat : date, time, seq, emiten, open, high, low, close, volume, value, frequency, net value foreign
    Paket_Lima  : date, time, seq, emiten, open, high, low, close, volume, value, frequency, net value foreign, net value local,
    Paket_Enam  : date, time, seq, emiten, open, high, low, close, volume, value, frequency, net value foreign, net value local, total net value

#Spec code :
Python3
Lib : 
  - time
  - mysql.connector
  - telnetlib
  - redis
  - re
  - csv
  - array
 
 #Redis :
    redis-server -> cmd : untuk menyalakan redis
    redis-cli -> cmd : untuk cek status di redis
        command : 
            keys * : cek banyak key yang ada di redis
            monitor : cek apa yang terjadi/ proses yang berlangsung di redis
            get <key> : cek data yang ada di key <key>
            flushall : menghapus semua key

#Running :
	database.py >> telnet_set_redis.py >> get_redis_to_db.py >> filter_paket_from_db >> .... pubsub
	telnet_set_redis.py >> get_redis_filterpaket.py >> .... pubsub
