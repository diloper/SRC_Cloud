# SRC_Cloud


主要功能分析股市籌碼集中度、股價變化，並設定條件可透過LINE BOT通知 
## 流程
分為X個流程，以下將分別詳細說明

1. 更新本國上市證券 https://isin.twse.com.tw/isin/C_public.jsp?strMode=2
2. 爬蟲 定時下載各股當日籌碼及股價  download_SRC.py
3. 過濾出所需資料 寫入DB 並上傳至雲端資料
4. 分析 股價數據之變化
5. 發送LINE BOT


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


### 載入驗證碼模型 demo.py
```
print('model loading...')
model = load_model("twse_cnn_model.hdf5")
print('loading completed')
```
### 計算籌碼集中度，須下載各股買賣股票成交價量資訊及股價

1. 當日買賣股票成交價量資訊(無提供歷史紀錄，需要每日儲存)
`https://bsr.twse.com.tw/bshtm/bsMenu.aspx`，透過`twse_cnn_model.hdf5` model 解析驗證碼後，下載並壓縮檔案
2. 取得股價
修改`stockid`、`DATE` 可下載歷史股價並將資料寫入DB (SQL)使用stockid 為 Primary key
`'https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid) +'&CHT_CAT2=DATE'`

## 分析
固數值可能變化大，因此透過 移動平均的方式 running average，取得固定周期之平均值。
計算斜率的變化率
將資料寫入DB (SQL)
## 上傳Google Drive API 啟用方式
https://d35mpxyw7m7k7g.cloudfront.net/bigdata_1/Get+Authentication+for+Google+Service+API+.pdf
## LINE BOT

### 第三方套件 驗證碼相關 preprocessBatch.py 
https://github.com/maxmilian/twse_bshtm_captcha


