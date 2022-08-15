#!/bin/bash

# 多重起動防止
if [ -e /IoT/bin/ftp/ftp.sh.lck ]; then
    exit 0
fi

touch /IoT/bin/ftp/ftp.sh.lck
cd /IoT/bin/ftp

python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/010/ALM.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/010/STS.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/010/STS_raw.json

python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/020/ALM.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/020/STS.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/020/STS_raw.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/020/QC_log1.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/020/COUNT.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/020/TS_log1.json


python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/030/ALM.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/030/STS.json
python3 /IoT/bin/ftp/FTPManager.py /IoT/bin/ftp/config/030/STS_raw.json

rm -f /IoT/bin/ftp/ftp.sh.lck
