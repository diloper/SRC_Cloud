# -*- coding: utf-8 -*-

#!/usr/bin/env python
# coding: utf-8


# WEB
# https://www.twse.com.tw/zh/page/trading/exchange/MI_MARGN.html
# API query 
# https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20220104&selectType=ALL
#  目標 : 融資維持率＝收盤價/(融資成本線*0.6)*100%
# 成本線的計算概念大概如下:
# 賣掉的直接去除，買進的用今日的價格，持有的用昨日的成本來計算。
# 用算式來表示的話: # 昨日剩下的部位 * 昨日的成本 + 今日買進的 * 今日的平均價格
# -----------------------------------------
#先進行少量驗證
#(https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20220104&selectType=ALL)資料最早可追朔2016年
#將 2016年至今的資料 下載存至DB

import requests

import pandas as pd
from datetime import date, timedelta, datetime
import sqlite3
from time import sleep
import json
import os
import goodinfo_t

import upload_file
import gzip


# can't use => "import import goodinfo_t as A"

def get_financing_v1(date='20220104'):
    sleep(20)
    my_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                 }
    #     url='https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid) +'&CHT_CAT2=DATE'
    url='https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date='+str(date) +'&selectType=ALL'
    s = requests.Session()
    try:

        Resp=s.post(url,headers =my_headers)
        sleep(0.15)
        Resp.encoding =  'utf-8' #for chinese encode
        # print(Resp.text)
        # parse Resp.text:
        if len(Resp.text)<1:
            print("data size <0")
            return None

        
    except (ValueError):
#         print(Resp+'size'+len(Resp.text))
        return None 
    except (OSError) as inst:
        print(inst)
        return None   
    return Resp

def get_financing_v2(date='20220104',stockid='0050'):

   
    Resp=get_financing_v1(date)
    y = json.loads(Resp.text)
    df = pd.DataFrame (y['data'], columns = 
              ['股票代號', '股票名稱', '買進', '賣出','現金償還', '前日餘額', '今日餘額', '限額',
                            '買進V', '賣出V', '現券償還', '前日餘額V', '今日餘額V', '限額V', '資券互抵', '註記'])

    del_target=[ '股票名稱', '現金償還','買進V', '賣出V', '現券償還', '前日餘額V', '今日餘額V', '限額', '限額V', '資券互抵', '註記']

    for i in del_target:
        del df[i]
    
    return df.query('股票代號 == "'+stockid+'"')



# get_financing_v2(date='20220923',stockid=stockid)


def readOHCLsql(stockid):
    dir='./small_test/'

    price_db_name=dir+str(stockid)+".sqlite"
    if os.path.isfile(price_db_name) is not True:
        return
    price_db_handler = sqlite3.connect(price_db_name)
    # tat='SELECT OHLC.Date,OHLC.Close,SRC.M20 from OHLC WHERE Date <="'+ str(date) +'"order by Date desc  LIMIT "'+str(boundary)+'"'
    tat='SELECT OHLC.Date,OHLC.Close from OHLC'
    # df = pd.read_sql_query('SELECT OHLC.Date,OHLC.Close, SRC.M20 from SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date ASC',price_db_handler)
    # df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    dfA = pd.read_sql_query(tat,price_db_handler)
    price_db_handler.close()
    return dfA


def savetosql(stockid,dfH):
    db_handler = sqlite3.connect('financing'+stockid+".sqlite")
    cursor = db_handler.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS "financing" (`買進` INTEGER  NOT NULL,`賣出` INTEGER NOT NULL,\
    `前日餘額`TEXT NOT NULL,`今日餘額`TEXT NOT NULL,`Date`TEXT  PRIMARY KEY NOT NULL)')
#     cursor.execute('SELECT name FROM  sqlite_master    WHERE type ="table" And name = "financing"')
#     rows = cursor.fetchall()
#     if len(rows)>0:
#     dfH.to_sql('financing', db_handler, if_exists='append', index = False)
#     else:
    try:
        cursor.executemany('insert into financing values(?,?,?,?,?)', dfH.values.tolist())
        db_handler.commit()
    except Exception as E:
        print('Error : ', E)
        db_handler.close()
#         dfH.to_sql('financing', db_handler, if_exists='replace', index = False)
    db_handler.close()

def readfromsql(stockid,dateformate=False):
    db_handler = sqlite3.connect('financing'+stockid+".sqlite")
    cursor = db_handler.cursor()
    cursor.execute('SELECT name FROM  sqlite_master WHERE type ="table" And name = "financing"')
    rows = cursor.fetchall()
    if len(rows)>0:
        cursor.execute(' SELECT * FROM financing' )
        # for row in cursor.fetchall():    
        #     print (row)
        dfB = pd.DataFrame(cursor.fetchall(), columns=['買進','賣出','前日餘額','今日餘額','Date'])   
        if(dateformate):
            dfB['Date'] = pd.to_datetime(dfB.Date)# change the datetime format
        db_handler.close()
        return dfB
    else:
        db_handler.close()
        return 0



def update_financingsql(stockid):
# downlaod / update new data

    dfB=readfromsql(stockid)
    dfA=readOHCLsql(stockid)
    dfA.replace({'-':''}, regex=True,inplace=True)
    _columns=['股票代號','買進','賣出','前日餘額','今日餘額','Date']
    if dfB is not 0:
        df_1notin2 = dfA[~(dfA['Date'].isin(dfB['Date']) )].reset_index(drop=True)
    else:
        df_1notin2=dfA

    dfG = pd.DataFrame(columns=_columns)
    # dfG
    for date in df_1notin2['Date']:
    #     print(date)
        row=get_financing_v2(date=date,stockid=stockid)
        if row is not None:
            row.reset_index(drop=True,inplace=True)
            row['Date'] = date
        #     print(row)

            dfG=pd.concat([dfG, row])
    if  dfG.shape[0]>0:
        dfH=dfG.drop(columns=['股票代號'])
        savetosql(stockid,dfH)

# dfB=readfromsql(stockid,dateformate=True)
# dfA=readOHCLsql(stockid)


# dfA
# dfA.join(dfB, on='Date')
# R=dfA.set_index('Date').join(dfB.set_index('Date'))
# pd.merge(dfA,dfB,how='inner',left_on=['Date'],right_on=['Date'])

# update_financingsql(str(2412))

# R.to_csv(stockid+'.csv')

# _start=date.fromisoformat('2021-05-24')

# A['Date']

# get_OHLC_goodinfo_date(STOCK_ID,start,current_date)

dir="./financing/"
def upload_finace_data(date,D_Handel,folder_id):
  dt_obj = datetime.strptime(date,'%Y%m%d')
  yearfolder=str(dt_obj.year)
  # create folder if not exist
  uploadfolder_id=D_Handel.search_folder(yearfolder,folder_id)
  if uploadfolder_id is None:
    uploadfolder_id=D_Handel.createFolder(yearfolder,folder_id)
  
  Res=get_financing_v1(date)
  bytes_encoded = Res.text.encode(encoding='utf-8')
  # print(type(bytes_encoded))
  filename=dir+str(date)+'.gz'
  with gzip.open(filename, 'wb') as f:
      f.write(bytes_encoded)
      
  file_id=D_Handel.search_file(name=filename,folder_id=uploadfolder_id)
  if file_id is None:
    D_Handel.uploadFile(filename=str(date)+'.gz',filepath=filename
        ,mimetype="application/gzip"
        ,folder_id=uploadfolder_id)

  #os.remove(filename)

import progressbar
def main():
    _start = date(2020,1,1)
    today = date.today()
    STOCK_ID=2412
    start = _start.isoformat()
    
    current_date = today.isoformat()
    print(start)
    print(current_date)
    A=goodinfo_t.get_OHLC_goodinfo_date(STOCK_ID,start,current_date)
    A.replace({'-':''}, regex=True,inplace=True)
    D_Handel=upload_file.Google_Driver_API()
    folder_id=D_Handel.search_folder(name='financing')
    a_folder_id=D_Handel.search_folders(folder_id=folder_id)
    

#    filesdf = pd.DataFrame(files)
    tmp_list=[]
 #    retrive folder_id and get files   
#   使用找出file extension gz，再用反向比較
#    K_folderid=filesdf[~filesdf['name'].str.contains(r'gz', regex=True, na=False)]
    for _folder_id in a_folder_id:
#        print(_folder_id["id"])
        a_files=D_Handel.list_folder_files(_folder_id["id"])
        if a_files is not None:
            tmp_list.extend(a_files)
    
    
    filesdf = pd.DataFrame(tmp_list) 
    
    filesdf.replace({'.gz':''}, regex=True,inplace=True)
    
    t2=filesdf['name'].tolist()
    t1=A['Date'].tolist()
    # R = pd.DataFrame(u)
    T=list(set(t1) - set(t2))
    # R.drop_duplicates(inplace=True)
#    print(len(T))
    for item in progressbar.progressbar(T):
#      print(item)
      upload_finace_data(item,D_Handel,folder_id)


if __name__ == '__main__':
    main()
#with gzip.open(filename, 'rb') as f:
#    file_content=f.read()
#
#str_decoded = file_content.decode(encoding='utf-8')
#print(str_decoded)

