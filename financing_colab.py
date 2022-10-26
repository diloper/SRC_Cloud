#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# %load financing_colab.py

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


import gzip
import io
import numpy as np
import os
import requests
import pandas as pd
from datetime import date, timedelta, datetime
import sqlite3
from time import sleep
import json
import goodinfo_t
import upload_file
import progressbar
dir="./financing/"


dir='./small_test/'
stockid='2330'


def readOHCLsql(dir,stockid,sql=None):
    price_db_name=dir+str(stockid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    db_handler = sqlite3.connect(price_db_name)
    # tat='SELECT OHLC.Date,OHLC.Close,SRC.M20 from OHLC WHERE Date <="'+ str(date) +'"order by Date desc  LIMIT "'+str(boundary)+'"'
    tat='SELECT OHLC.Date,OHLC.Close from OHLC'
    # df = pd.read_sql_query('SELECT OHLC.Date,OHLC.Close, SRC.M20 from SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date ASC',db_handler)
    # df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    dfA = pd.read_sql_query(tat,db_handler)
    db_handler.close()
    return dfA


def savetosql(dir,stockid,dfH):
#  K_columns=['stockid','buy','sell','yesterday_balance','today_balance','Date']
    price_db_name=dir+str(stockid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    db_handler = sqlite3.connect(price_db_name)
    cursor = db_handler.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS "financing"      (`buy` REAL  NOT NULL,`sell` REAL NOT NULL,    `yesterday_balance`INTEGER NOT NULL,`today_balance`INTEGER NOT NULL,    `fin_cost`REAL NOT NULL,`fin_maintenance_rate`REAL NOT NULL,    `Date`TEXT  PRIMARY KEY NOT NULL)')
#     cursor.execute('SELECT name FROM  sqlite_master    WHERE type ="table" And name = "financing"')
#     rows = cursor.fetchall()
#     if len(rows)>0:
#     dfH.to_sql('financing', db_handler, if_exists='append', index = False)
#     else:
    try:
        
        sql=list(dfH.columns.values)
        columan=""
        values=""
        for i in range(len(sql)):
            columan=columan+',"'+sql[i]+'"'
            values=values+",?"
#         print(columan[1:len(columan)])
#         print(values[1:len(values)])
#         print(dfH.values.tolist())
#         """Date	buy	fin_cost	fin_maintenance_rate	sell	today_balance	yesterday_balanceinsert into XXXXXXXXXXXX("XXXXXXXXXX", "XXXXXXXXX", "XXXXXXXXXXX") values(?,?,?)"""
        cursor.executemany('insert into financing ('+columan[1:len(columan)]+') values ('+values[1:len(values)]+')', dfH.values.tolist())
#         cursor.executemany('insert into financing   values (?,?,?,?,?,?,?)', dfH.values.tolist())
        db_handler.commit()
    except Exception as E:
        print('Error : ', E)
        db_handler.close()
#         dfH.to_sql('financing', db_handler, if_exists='replace', index = False)
    db_handler.close()


def readfromsql(dir,stockid,dateformate=False,sql=None):

    price_db_name=dir+str(stockid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    db_handler = sqlite3.connect(price_db_name)
    cursor = db_handler.cursor()
    cursor.execute('SELECT name FROM  sqlite_master WHERE type ="table" And name = "financing"')
    rows = cursor.fetchall()
    if len(rows)>0:
        if sql is not None:
            cursor.execute(sql)
            dfB = pd.DataFrame(cursor.fetchall(), columns=['buy','sell','yesterday_balance',
                                                       'today_balance','fin_cost',
                                                       'fin_maintenance_rate','Date','Close'])   
        else:
            cursor.execute('SELECT * FROM financing' )
        # for row in cursor.fetchall():    
        #     print (row) K_columns=[','buy','sell','yesterday_balance','today_balance','Date']
            dfB = pd.DataFrame(cursor.fetchall(), columns=['buy','sell','yesterday_balance',
                                                       'today_balance','fin_cost',
                                                       'fin_maintenance_rate','Date'])   
        if(dateformate):
            dfB['Date'] = pd.to_datetime(dfB.Date)# change the datetime format
        db_handler.close()
        return dfB
    else:
        db_handler.close()
        return None


#  download from google drive
def D_from_drive(date):
    D_Handel=upload_file.Google_Driver_API()
    dt_obj = datetime.strptime(str(date),'%Y%m%d')
    yearfolder=str(dt_obj.year)
    folder_id=D_Handel.search_folder(name='financing')
    downloadfolder_id=D_Handel.search_folder(yearfolder,folder_id)
    if downloadfolder_id is not None:
        name=str(date)+".gz"
        file_id=D_Handel.search_file(name,folder_id=downloadfolder_id)
        if file_id is not None:
            _IO=D_Handel.downloadFile(file_id=file_id,local_filepath=None,IO=True)
            f=gzip.decompress(_IO)
            s = f.decode('UTF-8')
            return s
    return None

def get_financing_v2(date,stockid):
#     local file
    try:
        dt_obj = datetime.strptime(str(date),'%Y%m%d')
        yearfolder=str(dt_obj.year)
        file_path='./financing/'+date+'.gz'

        if os.path.isfile(file_path):

            with gzip.open(file_path, 'rb') as f:
                file_content = f.read()
                A = file_content.decode('UTF-8')
        else: 
            A=D_from_drive(date)

        result=A

        y = json.loads(result)
    except:
        print(result)
        return None
    df = pd.DataFrame (y['data'], columns = ['股票代號', '股票名稱', '買進', '賣出', '現金償還', '前日餘額', '今日餘額', '限額', '買進V', '賣出V', '現券償還', '前日餘額V', '今日餘額V', '限額', '資券互抵', '註記'])

    del_target=[ '股票名稱', '現金償還','買進V', '賣出V', '現券償還', '前日餘額V', '今日餘額V', '限額', '資券互抵', '註記']

    for i in del_target:
        del df[i]
    G=df.query('股票代號 == "'+stockid+'"')
    if G.shape[0] == 0:
        return None
    return G
_BN=120
# 列出 之前N筆資料
def find_BN_OHCL(stockid,date,n):
    A=goodinfo_t.get_OHLC_goodinfo(stockid)

    df=A
    # df.set_index('Date')

    cond0=df['Date'] == str(date)
    # cond1=df['Date']>= start
    # df.loc[start:end]
    x=df.loc[cond0 ].index[0]
    if (x-n)<0:
        return None
    
    return df.iloc[x-n:x]
# flag is True return back half value
def fin_maintenance_rate(df,flag):
    df.replace({',':''}, regex=True,inplace=True)
    df = df.astype({'yesterday_balance':'float'})
    df = df.astype({'buy':'float'})
    # df = df.astype({'financing':'float'})
    # 設定初始值
    
    # df['fin_cost'][0]=df["Close"][0] 會觸發SettingWithCopyWarning
    if flag:
        df['fin_cost']= np.nan
        df.loc[0:1,['fin_cost']]=df["Close"][0]

        
    df.reset_index(inplace=True)
    df.drop(['index'], axis=1,inplace=True)
    for i in range(len(df)-1):
        fin_cost=(df["buy"][i+1]*df["Close"][i+1]+df["fin_cost"][i]*df["yesterday_balance"][i+1])/(df["buy"][i+1]+df["yesterday_balance"][i+1])
        fin_maintenance_rate=df["Close"][i+1]/(np.round(fin_cost,4)*0.6)*100
        df.loc[i+1:i+2,"fin_cost"]=np.round(fin_cost,4)
        df.loc[i+1:i+2,"fin_maintenance_rate"]=np.round(fin_maintenance_rate,4)
    if flag:
        return df.loc[len(df)/2:len(df)-1]
    return df.loc[1:len(df)-1]
                                  

def update_financingsql(dir,stockid):
# downlaod / update new data

    dfA=readOHCLsql(dir,stockid)
    dfB=readfromsql(dir,stockid)
    find_BN=False
    Adf=None
    _columns=['股票代號','買進','賣出','前日餘額','今日餘額','Date']
    K_columns=['stockid','buy','sell','yesterday_balance','today_balance','Date']
    if dfB is not None and dfA.shape[0]>0: #     if financing table is exist
#         dfA.replace({'-':''}, regex=True,inplace=True)
        df_1notin2 = dfA[~(dfA['Date'].isin(dfB['Date']) )].reset_index(drop=True)
#         print(df_1notin2)
        if df_1notin2.shape[0] < 1:
            return 0
        _sql='select financing.* , OHLC.Close from  financing join OHLC  on OHLC.date=financing.date  WHERE financing.date < "'+str(df_1notin2['Date'][0])+'" order by  financing.date desc limit 1'

#         print(str(df_1notin2['Date'][0]))
        G=readfromsql(dir,stockid,sql=_sql)
        Adf = pd.DataFrame(G)


    else:
        df_1notin2=dfA
#         print(df_1notin2['Date'][0])
        tmp=find_BN_OHCL(stockid,df_1notin2['Date'][0],_BN)
        df_1notin2=pd.concat([tmp.loc[:,['Date','Close']],df_1notin2]).reset_index(drop=True)
        find_BN=True
    dfG = pd.DataFrame(columns=_columns)
   

    for index in range(len(df_1notin2)):
        date=df_1notin2['Date'][index]
#         print(date)
        row=get_financing_v2(date=date.replace("-", ""),stockid=stockid)# convert Y-M-D to YMD
        if row is not None:
            row.reset_index(drop=True,inplace=True)
            row['Date'] = date
            row['Close']=df_1notin2['Close'][index]
    #         print(row)
            dfG=pd.concat([dfG, row])
#         else:
#             print('cant find'+str(date))
            
            

    if  dfG.shape[0]>0:
        dfH=dfG.drop(columns=['股票代號'])
        dfH.rename(columns = {'買進' :'buy','賣出':'sell', '前日餘額':'yesterday_balance','今日餘額':'today_balance'}, inplace = True)
        
        if Adf is not None:
            dfH=pd.concat([Adf,dfH]).reset_index(drop=True)

        K=fin_maintenance_rate(dfH,find_BN)
        
        K.drop(['Close'], axis=1,inplace=True)

        savetosql(dir,stockid,K)
    else:
        print('cant find financing'+str(stockid))








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


def upload_finace_data(date,D_Handel,folder_id):
    dt_obj = datetime.strptime(date,'%Y%m%d')
    yearfolder=str(dt_obj.year)
    # create folder if not exist
    uploadfolder_id=D_Handel.search_folder(yearfolder,folder_id)
    if uploadfolder_id is None:
        uploadfolder_id=D_Handel.createFolder(yearfolder,folder_id)

    Res=get_financing_v1(date)
    bytes_encoded = Res.text.encode(encoding='utf-8')
#     check if data >0
    dictx = json.loads(Res.text)
    train = pd.DataFrame.from_dict(dictx, orient='index')
    train.reset_index(level=0, inplace=True)
    cond0=train['index']== 'data'
    # df.loc[start:end]
    A=train.loc[cond0 ]
    if len(A.iat[0, 1]) <1:
        return None
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


def main():
    _start = date(2020,1,1)
    today = date.today()
    STOCK_ID=2412
    start = _start.isoformat()
    
    current_date = today.isoformat()
    print(start)
    print(current_date)
    A=goodinfo_t.get_OHLC_goodinfo_date(STOCK_ID,start,current_date)
#     print(A)
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
#         print(item)
        upload_finace_data(item,D_Handel,folder_id)
dirp='small_test'
import download_SRC as SRCtool
if __name__ == '__main__':
    main()
    
    A=SRCtool.get_sqlite_local_file(dirp)
    for item in progressbar.progressbar(A):
        try:
#             print(item[0])
            update_financingsql(dir,stockid=item[0])
        except Exception as e: 
            print("except"+item[0])
            print(e)

