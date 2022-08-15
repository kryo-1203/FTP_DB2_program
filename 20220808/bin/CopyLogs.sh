#!/bin/bash

# 多重起動防止
if [ -e /IoT/bin/copylogs.sh.lck ]; then
    exit 0
fi

touch /IoT/bin/copylogs.sh.lck
cd /IoT/bin

# NAS　umount確認
sudo /bin/umount /IoT/bin/nas
#NAS
sudo /bin/mount -t cifs -o vers=2.0,domain='AD',user='',password='Zaq12wsx' //IP/01_Data/Bldg3/B01_LAC10001 /IoT/bin/nas


# データ収集対象 tmp/ -> nas
# A && B：Aが成功したらBを実行
# 　option : -r ディレクトリ　　-u タイムスタンプが新しいファイルのみコピー
#PLC設備　----→　NAS
sudo cp -r -u /IoT/bin/ftp/tmp/010 /IoT/bin/nas && sudo cp -r -u /IoT/bin/ftp/tmp/010 /IoT/bin/swap && rm -r /IoT/bin/ftp/tmp/010/*
sudo cp -r -u /IoT/bin/ftp/tmp/020 /IoT/bin/nas && sudo cp -r -u /IoT/bin/ftp/tmp/020 /IoT/bin/swap && rm -r /IoT/bin/ftp/tmp/020/*
sudo cp -r -u /IoT/bin/ftp/tmp/030 /IoT/bin/nas && sudo cp -r -u /IoT/bin/ftp/tmp/030 /IoT/bin/swap && rm -r /IoT/bin/ftp/tmp/030/*


#umount
sudo /bin/umount /IoT/bin/nas
# ロックファイル削除
rm /IoT/bin/copylogs.sh.lck