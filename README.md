# SRC_Cloud


主要功能分析股市籌碼集中度、股價變化，並設定條件可透過LINE BOT通知 
## 流程
分為X個流程，以下將分別詳細說明

1. 更新本國上市證券(Google app script ) https://isin.twse.com.tw/isin/C_public.jsp?strMode=2
2. 爬蟲 定時下載各股當日籌碼及股價  download_SRC.py
3. 過濾出所需資料 寫入DB 並上傳至雲端資料
4. 分析 股價數據之變化
5. 發送LINE BOT


##
##
## 上傳Google Drive API 啟用方式
https://d35mpxyw7m7k7g.cloudfront.net/bigdata_1/Get+Authentication+for+Google+Service+API+.pdf
##

### 第三方套件 驗證碼相關 preprocessBatch.py 
https://github.com/maxmilian/twse_bshtm_captcha


