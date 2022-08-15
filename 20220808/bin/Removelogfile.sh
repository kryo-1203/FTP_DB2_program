#!/bin/bash

# 多重起動防止
if [ -e /IoT/bin/removelog.sh.lck ]; then
    exit 0
fi

touch /IoT/bin/removelog.sh.lck
cd /IoT/bin/

rm -f /IoT/bin/ftp/log/debug.log && touch /IoT/bin/ftp/log/debug.log

rm /IoT/bin/removelog.sh.lck
