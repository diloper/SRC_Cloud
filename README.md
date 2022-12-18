# SRC_Cloud


主要功能分析股市籌碼集中度、股價變化，並設定條件可透過LINE BOT通知 
## 流程
分為5個流程，以下將分別詳細說明

1. 更新本國上市證券 https://isin.twse.com.tw/isin/C_public.jsp?strMode=2
2. 定時爬蟲下載各股劵商當日籌碼及 並 上傳雲端
3. 計算個股籌碼集中度並下載對應股價
4. 分析數據之變化
5. 發送LINE BOT通知


## 本國上市證券
考量股票上市與下市情況，須動態更新； 透過(Google App script) 每日至`https://isin.twse.com.tw/isin/C_public.jsp?strMode=2` 進行下載並更新，存放於Google Drive。
```
function getstockFile(fileURL) {
 var response = UrlFetchApp.fetch('https://isin.twse.com.tw/isin/C_public.jsp?strMode=2');
  var fileBlob = response.getBlob()
//  Logger.log(response.getContentText("Big5"));
  
  
  var id = DriveApp.getFoldersByName('Stock db').next().getId();
  var folder = DriveApp.getFolderById(id);
  //DocsList.createFolder('Folder1')
  var files = DriveApp.searchFiles(
    'title = "isin.html" and trashed=false');
  while (files.hasNext()) {
  var file = files.next();
//    In order to use DriveAPI, you need to add it through the Resources, 
//    Advanced Google Services menu. Set the Drive API to ON. AND make sure that the Drive API is turned on in your Google Developers Console. 
//    If it's not turned on in BOTH places, it won't be available.
    rtrnFromDLET = Drive.Files.remove(file.getId());
  Logger.log(file.getName());
}
//    Logger.log(files);
  var result = folder.createFile('isin.html',response.getContentText("Big5"));
//   Logger.log(result);

}
```
配置每日執行

<img width="833" alt="image" src="https://user-images.githubusercontent.com/15354113/188384073-ffc6aad9-d3e6-4400-a1ed-fb2116e7704a.png">

## 爬蟲 
<img width="957" alt="image" src="https://user-images.githubusercontent.com/15354113/188393783-a58c87fb-362b-420e-8342-3ca8ce76f5fc.png">
<img width="686" alt="image" src="https://user-images.githubusercontent.com/15354113/188401263-0c7a46a6-d928-4e52-a68b-5525ef4c5b97.png">


### 載入驗證碼模型 demo.py
```
print('model loading...')
model = load_model("twse_cnn_model.hdf5")
print('loading completed')
```
### 計算籌碼集中度，須下載各股買賣股票成交價量資訊

1. 當日買賣股票成交價量資訊(無提供歷史紀錄，需要每日儲存)
`https://bsr.twse.com.tw/bshtm/bsMenu.aspx`，透過`twse_cnn_model.hdf5` model 解析驗證碼後，下載並壓縮檔案
```,,,
CREATE TABLE `YYYY-MM-DD` (
	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	`BrkId`	TEXT, #券商
	`BuyNetAmt`	REAL,# 買進股數*價格-賣出股數*價格
	`BuyNetVol`	INTEGER # 買進股數-賣出股數
	`BuyAvg`	REAL,  #平均買進價格
	`SellAvg`	REAL  #平均賣出價格
);
```
2. 取得成交量

	下載歷史成交量並將資料寫入DB (SQLite 使用stockid作為檔名)
```
CREATE TABLE `OHLC` (
	`Date`	TEXT, #日期
	`Open`	REAL, #開盤價
	`High`	REAL, #最高價
	`Low`	REAL, #最低價
	`Close`	REAL, #收盤價
	`Volume`	INTEGER  #成交量股數
);
```
3. SRC籌碼集中度計算

	定義: 籌碼集中度算法=(區間買超前15的買張合計-區間賣超前15名的賣張合計) date_size / 區間成交量(period_vol)
	
	date_size=15 及  period_vol=60 參數可調整
	
	SRC_20 : 20天區成交量 的資料進行計算 
	
	SRC_60:  60天區成交量 的資料進行計算

```
def get_stock_SRC_(dir,stockid,start,current_date,period_vol,date_size=15,date_source=None):
    
    time_interval="sd="+str(start)+"&ed="+str(current_date)
    # print (time_interval)
    # 籌碼集中度=(區間買超前15的買張合計-區間賣超前(_date_size)15名的賣張合計) / 區間成交量
    _date_size=str(date_size)

    if date_source == "sqlite":
        sqlite_name=dir+str(stockid)+".sqlite"
        db_handler = sqlite3.connect(sqlite_name)
        db_handler.create_function("REGEXP", 2, regexp)
        cursor = db_handler.cursor()
        cursor.execute('SELECT name FROM  sqlite_master         WHERE type ="table" And name REGEXP "^[0-9\-]+$"          And name >= "'+start+'"         And name <= "'+current_date+'"         ORDER BY name ASC;')
        # cursor.fetchone()
        # remaining_rows = cursor.fetchall()
        rows = cursor.fetchall()
        table_name_list = rows
        select_template = 'SELECT BrkId , BuyNetVol FROM "{table_name}"'
        Fal_df={}
        # for idx, val in enumerate(ints):
        for index,item in enumerate(table_name_list):
            query = select_template.format(table_name = item[0])
            if index==0:
                Fal_df=pd.read_sql(query, db_handler)
            else:
                tmp=pd.read_sql(query, db_handler)
                Fal_df = pd.concat([Fal_df, tmp],sort=False)

        A=Fal_df.groupby('BrkId',sort=False).sum().sort_values(by=['BuyNetVol'])
        # print(A.head(15))
        Z=A.head(int(15)).div(1000).values.sum()
        Q=A.tail(int(15)).div(1000).values.sum()
        sell_top_data_size=A.head(int(_date_size)).div(1000).values.sum()
        buy_top15_data_size=A.tail(int(_date_size)).div(1000).values.sum()
```

## 分析籌碼連續性

1.採用斜率分析籌碼每日之變化，為觀察斜率連續為正或負特性，透過數學式 $\frac{X}{|X|}$ `df['M20']/abs(df['M20'])`簡化斜率正負(+1 或 -1)

2.後續透過(rolling)移動平均方式計算總合 e.g. 若+1連續五次，則表示籌法斜率連續五次為正

```
# SRC 變化分析
def SRC_green_red_check(dir,stockid):
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)

    cursor = db_handler.cursor()
    cursor.execute('select * from SRC order by Date asc')
    # rows = cursor.fetchall()
    df = pd.DataFrame.from_records(cursor.fetchall(),
    columns = [desc[0] for desc in cursor.description])
    df['green_red_Condiction']=df['M20']/abs(df['M20'])#定義正為1 負為-1
    # 採用累加的方式進行統計 10 與 5 的週期
    
    df['GR10']=df['green_red_Condiction'].rolling(window=10,min_periods=10).sum()
    df['GR5']=df['green_red_Condiction'].rolling(window=5,min_periods=5).sum()
   
    # for i in df['green_red_Condiction']:
    # df.to_csv(str(stockid)+".csv")
    
    # -----+++++ GR10 總合為0、GR5 總和為5
    cond0=df['GR10']>=0
    cond1=df['GR5']==5
    cond2=df['GR10']<3
    # cond1=df['M20']>=-0.03
    A=df.loc[cond0 & cond1 & cond2 ]
    db_handler.close()
#     print(A)
    return A
```


## 上傳Google Drive API 啟用方式
https://d35mpxyw7m7k7g.cloudfront.net/bigdata_1/Get+Authentication+for+Google+Service+API+.pdf
## 發送LINE BOT通知
建立LINE BOT
1. 取得Channel access token 存檔後從程式碼讀取，‵sendMessage()‵進行發送
2. 加入建置頻道
```
class M_line_Bot_API:
#     self.drive_service
    def __init__(self):
        config = configparser.ConfigParser()
        A=config.read('line_bot.ini')
        access_token=config.get('line-bot', 'channel_access_token')
        self.to=config.get('line-bot', 'user_id')
        self.line_bot_api = LineBotApi(access_token)
        
    def sendMessage(self,text):
        try:
            self.line_bot_api.push_message(self.to, TextSendMessage(text=text))
        except LineBotApiError as e:
            print(e)
```
<img width="379" alt="image" src="https://user-images.githubusercontent.com/15354113/188402690-843d85e2-d3be-43c0-b9ec-0d815dbca30f.png">

<img width="533" alt="image" src="https://user-images.githubusercontent.com/15354113/188402327-b731589c-ed95-44bb-881a-6231119cbacd.png">

### 第三方套件 驗證碼相關 preprocessBatch.py 
https://github.com/maxmilian/twse_bshtm_captcha

## Windows run docker and mount directory

```
docker run --rm -v D:\Programming\STOCK\20220914:/tmp  --name SRC_Server  diloper/tensorflow_evn:latest

docker run  --rm -it --mount type=bind,source=D:\Programming\STOCK\20220914,target=/tmp --name SRC_Server diloper/tensorflow_evn:latest
```
### troubleshooting
```
Error message => docker: Error response from daemon: status code not OK but 500: {"Message":"Unhandled exception: Filesharing has been cancelled","StackTrace":"
```
打开docker桌面-》设置，如图：添加要挂载的文件目录，应用就可以了，再执行上面的命令就可以了
![image](https://user-images.githubusercontent.com/15354113/208278734-bdfcd178-896a-43f3-88ed-e7bbbf60fc88.png)

參考資料來自https://www.cnblogs.com/liangyy/p/13602053.html
