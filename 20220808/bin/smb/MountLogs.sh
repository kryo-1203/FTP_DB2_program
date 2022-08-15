#!/bin/bash

# 多重起動防止
if [ -e /IoT/bin/smb/MountLogs.sh.lck ]; then
    exit 0
fi

touch /IoT/bin/smb/MountLogs.sh.lck
cd /IoT/bin

# ＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊
#   露光CSV保存サーバー　→　PC　データ収集　手順
#       1.サーバーとの接続解除確認(umount)
#       2.サーバーへmount
#       3.CSVをローカルへ差分コピーしtest.txtを作成
#       （test.txt：次回収集時に作成時間から差分でCSVをコピーするために使用）
#       （A && B：Aが成功したらBを実行）
# 　    （option : -r ディレクトリ　　-u タイムスタンプが新しいファイルのみコピー）
#       4.サーバーとの接続解除(umount)
# ＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊＊

# 露光(裏)
sudo /bin/umount -l /IoT/bin/smb/mountfolder
sudo /bin/mount -t cifs -o vers=2.0,user='',password='' //192.168.100.150/DatamationLog/DATA /IoT/bin/smb/mountfolder
cd /IoT/bin/smb/mountfolder
find . -type f -newer /IoT/bin/smb/test.txt -print0 |xargs -0 cp -u --parents -t  /IoT/bin/smb/tmp/110/log1_QC/fromPC && touch /IoT/bin/smb/test.txt
sudo /bin/umount -l /IoT/bin/smb/mountfolder

# 露光(表)
sudo /bin/umount -l /IoT/bin/smb/mountfolder
sudo /bin/mount -t cifs -o vers=2.0,user='',password='' //192.168.100.160/DatamationLog/DATA /IoT/bin/smb/mountfolder
cd /IoT/bin/smb/mountfolder
find . -type f -newer /IoT/bin/smb/test2.txt -print0 |xargs -0 cp -u --parents -t  /IoT/bin/smb/tmp/140/log1_QC/fromPC && touch /IoT/bin/smb/test2.txt
sudo /bin/umount -l /IoT/bin/smb/mountfolder


# ロックファイル削除
rm /IoT/bin/smb/MountLogs.sh.lck