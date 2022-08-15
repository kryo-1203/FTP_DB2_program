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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("/IoT/bin/DB/debug.log")
file_handler.setLevel(logging.DEBUG)
file_handler_format = logging.Formatter('%(asctime)s : %(levelname)s - %(message)s')
file_handler.setFormatter(file_handler_format)
file_handler.setFormatter(file_handler_format)
logger.addHandler(file_handler)

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

logger.debug("************"+j_data["unit_num"]+"  "+j_data["unit_name"]+" QCの書き込み START*******************")
try:
    conn = ibm_db.connect("DATABASE=" + database + ";HOSTNAME=" + server + ";PORT=50000;PROTOCOL=TCPIP;UID=" + username + ";PWD=" + password + ";", "", "")
    logger.debug('DB接続OK')
except:
    logger.exception("DB接続失敗")
#csvファイル読み込み  
files = glob.glob(j_data["dst_dir"])
kousin_time =[]
#ファイルの数だけループ
for f in files:
    try:
        df = pd.read_csv(f,skiprows =13, header=None,encoding="cp932", dtype=object)
        abc =pd.Series()
        for index, row in df.iterrows():
            try:
                df_starttime = row[6] +"-"+row[7]+"-"+row[8]+" "+row[9]+":"+row[10]+":"+row[11]
                # print(df_starttime)
                df_endtime = row[12] +"-"+row[13]+"-"+row[14]+" "+row[15]+":"+row[16]+":"+row[17]
                # print(df_endtime)
                df_starttime = datetime.datetime.strptime(df_starttime, '%Y-%m-%d %H:%M:%S')
                df_starttime = df_starttime.strftime('%Y-%m-%d %H:%M:%S')
                df_endtime = datetime.datetime.strptime(df_endtime, '%Y-%m-%d %H:%M:%S')
                df_endtime = df_endtime.strftime('%Y-%m-%d %H:%M:%S')
                abc["EQP_ID"] = j_data["eqp_id"]
                abc['UNIT_NUM'] = j_data["unit_num"]
                abc['UNIT_NAME'] = j_data["unit_name"]
                abc["MEASURED_DT"] = row[0].replace("/","-")
                abc["LOG_NUM"] = j_data["log_num"]
                abc["ST_TIME"] = df_starttime
                abc["END_TIME"] = df_endtime
                abc["MATERIAL_ID"] = row[4]
                abc["RECIPE_CODE"] = row[5]
                json_lastup_time =j_data["last_update_datatime"].replace("/","-")
                measured_time = datetime.datetime.strptime(abc['MEASURED_DT'],'%Y-%m-%d %H:%M:%S')
                last_uptime = datetime.datetime.strptime(json_lastup_time,'%Y-%m-%d %H:%M:%S')
                if measured_time > last_uptime:
                    a=[]
                    for num in range(17):
                        string = int(num)
                        a.append(string)
                    row = row.drop(a)
                    y = len(row)
                    h_name=[]
                    for i in range(y):
                        db_num = 'a' + str(i+1)
                        h_name.append(db_num)
                    row.index = h_name
                    row = row.append(abc)
                    str_columnname = ','.join(row.index)
                    row = "'"+ row +"'"
                    str_value = ','.join(map(str,row))
                    try:
                        str_value = str_value.replace("nan", "NULL")
                    except:
                        pass
                    ibm_db.exec_immediate(conn,"INSERT INTO IOT_DATA.B01_ID_PROCESS ("+ str_columnname +") values(" + str_value + ")" )  

                    update_time = row['MEASURED_DT'] 
                    update_time = row['MEASURED_DT'].replace("'","")
                    logger.debug("MEASURED_DTが "+update_time+" のデータを書き込み完了")
                    kousin_time.append(measured_time) 
                else:
                    pass
            except:
                pass 
    except:
        pass
try:
    kousintime_max = max(kousin_time)
    j_data["last_update_datatime"] = kousintime_max.strftime('%Y-%m-%d %H:%M:%S')
    # 辞書オブジェクトをJSONファイルへ出力
    with open(args[1], mode='wt', encoding='utf-8') as file:
        json.dump(j_data, file, ensure_ascii=False, indent=2)
        logger.debug("last_update_datatimeを "+j_data["last_update_datatime"]+" に変更")
except:
    pass
logger.debug("************"+j_data["unit_num"]+"  "+j_data["unit_name"]+" QCの書き込み END*********************")
    