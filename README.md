<DONE>
  <database.py> = membuat DB, cek koneksi DB, Membuat Tabel RT \n
  <nameofDB> : nama DB yang akan di eksekusi
    
<DONE>
telnet_set_redis.py = koneksi telnet, set output ke dalam key redis
  connection refused : cek PORT, dan passwordnya, cek ping HOST, cek VPN sudah aktif
  untuk tau data sudah tersimpan ke redis :
      cmd -> redis-cli -> monitor 

<DONE>      
get_redis_to_txt.py = export all key di redis ke dalam file .txt
  jika ada data yang di kategorikan sampah bisa di masukan ke filter >> cek code >> getRow != <filter>
  <file> : nama file yang akan di eksekusi, sifat penyimpanan 'append' artinya row akan di tambahkan





  



Spec code :
Python3
Lib : 
  - time
  - mysql.connector
  - telnetlib
  - redis
  - re
  - csv
  
 
 Redis :
    redis-server -> cmd : untuk menyalakan redis
    redis-cli -> cmd : untuk cek status di redis
        command : 
            keys * : cek banyak key yang ada di redis
            monitor : cek apa yang terjadi/ proses yang berlangsung di redis
            get <key> : cek data yang ada di key <key>
            flushall : menghapus semua key
