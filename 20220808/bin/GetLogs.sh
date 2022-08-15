#!/bin/bash

# 多重起動防止
if [ -e /IoT/bin/getlogs.sh.lck ]; then
    exit 0
fi

touch /IoT/bin/getlogs.sh.lck

# cd /IoT/bin/smb
# ./Smb_GetLogs.sh 

cd /IoT/bin/ftp
./Ftp_GetLogs.sh

cd /IoT/bin/DB
./DB_insert.sh

cd /IoT/bin
./CopyLogs.sh

rm /IoT/bin/getlogs.sh.lck