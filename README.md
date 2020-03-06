1. python3 database.py
    buat database, tabel recordtype 0-6
    (-) belum fix untuk nilai int db -> harusnya pakai longint
    (-) belum buat database untuk tabel paketdata

2. python3 telnet_set_redis.py
    koneksi telnet + nyimpen data telnet ke key redis (inisialisasi key = i )
    (-) 1x running, ga ada set-time untuk expire
    DONE (-) harusnya pake perintah 'flushall' dalam redis untuk menghapus value key

3. python telnet_get_redis_to_db.py
    read value dari key pada (tahap 2.) filter data sesuai dengan recordtype 0 - 6
    (-) belum ada filtering secara kolom 'sequence' biar data tidak tersimpan dobel di db
    (-) belum ada filtering untuk paketdata 

(-) main.py -> belum bs di pake

Belum update lagi
========================================================================================
