import sys
import glob
import json
from pathlib import Path
import logging
import logging.handlers
import pandas as pd
import ibm_db
import db2lib
import datetime

#jsonファイル読み込み
args = sys.argv
fd = open(args[1], mode='r')
j_data = json.load(fd)
fd.close()

# DB接続
server = j_data["server"]
database = j_data["database"]
username = j_data["username"]
password = j_data["pw"]
conn = ibm_db.connect("DATABASE=" + database + ";HOSTNAME=" + server + ";PORT=50000;PROTOCOL=TCPIP;UID=" + username + ";PWD=" + password + ";", "", "")
print('DB接続OK')
ibm_db.exec_immediate(conn,"ALTER TABLE IOT_DATA.B999_PROCESS DROP COLUMN A1,A2" ) 
