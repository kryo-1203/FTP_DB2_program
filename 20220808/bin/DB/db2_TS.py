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
logger.debug("***************" + j_data["unit_num"]+"  "+j_data["unit_name"]+" TSの書き込みStart*****************")

# DB接続
server = j_data["server"]
database = j_data["database"]
username = j_data["username"]
password = j_data["pw"]
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
        y =len(df.columns)
        h_name=[]
        for i in range(y):
            db_num = 'a' + str(i)
            h_name.append(db_num)
        df.columns = h_name
        
        df['EQP_ID'] = j_data["eqp_id"]
        df['UNIT_NAME'] = j_data["unit_name"]
        df['UNIT_NUM'] = j_data["unit_num"]
        df['LOG_NUM'] = j_data["log_num"]
        df_new = df.rename(columns={'a0': 'MEASURED_DT'})
        json_lastup_time =j_data["last_update_datatime"].replace("/","-")
        last_uptime = datetime.datetime.strptime(json_lastup_time,'%Y-%m-%d %H:%M:%S')
        str_columnname = ','.join(df_new)
        df_new = df_new.astype(str)
    
        try:
            for index, row in df_new.iterrows():
                row['MEASURED_DT'] =row['MEASURED_DT'].replace("/","-")
                measured_time = datetime.datetime.strptime(row['MEASURED_DT'],'%Y-%m-%d %H:%M:%S')
                if measured_time > last_uptime:
                    try:
                        row = "'"+ row +"'"
                        str_value = ','.join(map(str,row))
                        try:
                            str_value = str_value.replace("'nan'", "NULL")
                        except:
                            pass
                        ibm_db.exec_immediate(conn,"INSERT INTO IOT_DATA.B01_PROCESS ("+ str_columnname +") values(" + str_value + ")" ) 
                        
                        update_time = row['MEASURED_DT'] 
                        update_time = row['MEASURED_DT'].replace("'","")
                        logger.debug("MEASURED_DTが "+update_time+" のデータを書き込み完了")
                        kousin_time.append(measured_time) 

                    except:
                        # logger.exception("重複エラー")     
                        pass
                else:
                    pass
        except:
            logger.exception("エラー")
    except:
        pass
try:
    kousintime_max = max(kousin_time)
    j_data["last_update_datatime"] = kousintime_max.strftime('%Y-%m-%d %H:%M:%S')
    # 辞書オブジェクトをJSONファイルへ出力
    with open(args[1], mode='wt', encoding='utf-8') as file:
        json.dump(j_data, file, ensure_ascii=False, indent=2)   
except:
    pass
logger.debug("************"+j_data["unit_num"]+"  "+j_data["unit_name"]+" TSの書き込み END*********************")
   