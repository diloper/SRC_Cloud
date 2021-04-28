#!/usr/bin/env python
# coding: utf-8

# In[26]:


# %load goodinfo_t.py

from datetime import date, timedelta, datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sqlite3
import time
import numpy as np
def create_cookies():
    fixed_value=';SCREEN_SIZE=WIDTH=1093&HEIGHT=615'
    today=date.today()
    Q=today.strftime("%Y%m%d%H%M%S")  # 格式化日期
    # print(Q)
    A=requests.get("https://api.ipify.org/?format=json")
    # A.text
    # my_ip = load(A.text)['ip']
    # jsDumps = json.dumps(A.text)  
    jsLoads = json.loads(A.text)
    public_ip=jsLoads['ip']
    # public_ip
    cookies=Q+'_'+public_ip+fixed_value
    cookies
    return cookies
def save2sqlite(dir,stockid,data):
    
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    cursor = db_handler.cursor()

    cursor.execute('SELECT name FROM  sqlite_master                WHERE type ="table" And name = "OHLC"')
    _OHLC = cursor.fetchall()
    if len(_OHLC)>0:
        cursor.execute('select Date from OHLC order by Date asc limit 1')
        head = cursor.fetchone()
        cursor.execute('select Date from OHLC order by Date desc limit 1')
        tail = cursor.fetchone()
        # print(head[0])
        cond0=data['Date']< head[0]
        cond1=data['Date']> tail[0]
        A=data.loc[cond0 | cond1]
#         suggest use "execute" to create table which has primary key and upadte data 
        A.to_sql("OHLC", db_handler, if_exists='append',index=False)
    else:
        data.to_sql("OHLC", db_handler, if_exists='replace',index=False)
    db_handler.close()
def get_OHLC_goodinfo_date(stockid,start,end):
#    df=get_OHLC_goodinfo(stockid)
    df=retry_download(stockid,counter=3)
    cond0=df['Date']<= end
    cond1=df['Date']>= start
    # df.loc[start:end]
    A=df.loc[cond0 & cond1]
    return A
#  Add referer in http header to get OHLC data
def get_OHLC_goodinfo(stockid):
#     user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36
# accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
#     cookie: CLIENT%5FID=20200322103548941%5F221%2E120%2E80%2E192; _ga=GA1.2.1822776169.1584844609; __gads=ID=309d3c3497b843f7:T=1584844609:S=ALNI_MYg4dVQ6Ha-FLVYPN0-Df0MpgpOKw; LOGIN=EMAIL=gool811209%40gmail%2Ecom&USER%5FNM=Herka+Lip&ACCOUNT%5FID=108894270159705601497&ACCOUNT%5FVENDOR=Google&NO%5FEXPIRE=T; SCREEN_SIZE=WIDTH=1280&HEIGHT=720; _gid=GA1.2.1377085740.1594899488; GOOD%5FINFO%5FSTOCK%5FBROWSE%5FLIST=20%7C8916%7C4564%7C2412%7C1434%7C8069%7C3003%7C3303%7C4760%7C2454%7C2633%7C3056%7C6177%7C9188%7C4303%7C2596%7C2317%7C4938%7C8086%7C9945%7C1303
#     cookie: CLIENT_ID=20200718101954015_36.228.106.174;SCREEN_SIZE=WIDTH=1093&HEIGHT=615
    

    my_headers = {'cookie': create_cookies()
        ,'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
        ,'referer': 'https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid)}
    url='https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid) +'&CHT_CAT2=DATE'
    s = requests.Session()
    Resp=s.get(url,headers =my_headers)
    
    Resp.encoding =  'utf-8' #for chinese encode
#     print(Resp.text)
    soup = BeautifulSoup(Resp.text, features="html.parser")
    _script_list = soup.findAll('script')
    # print(soup.find('script')['src'])
    Resp=""
    R=""
    for item in _script_list:
        Resp=item.get('src')
        if Resp is not None:
            if 'ShowK_ChartData.asp?' in Resp:
                u='https://goodinfo.tw/StockInfo/'+Resp
                R=s.get(u,headers =my_headers)
                R.encoding='utf-8'
                break
    # substring Javascipt with start_index end_index ,then store at data_list
    result=R.text
    start_index=result.find('Records:{RPT_TIME')
    end_index=result.find('};',start_index)
    result=result[start_index: end_index+1]
    search_key_word=["RPT_TIME","開盤價","最高價","最低價","收盤價","漲跌價"
    ,"成交量","成交額"]
    data_list={}
    for i in range(0,len(search_key_word),1):
        if "漲跌價" in search_key_word[i]:
            continue
        elif "成交額" in search_key_word[i]:
            break
        start_index =result.find(search_key_word[i])+len(search_key_word[i]+":")+1
        end_index =result.find(search_key_word[i+1])-2
        data_list[search_key_word[i]]=result[start_index:end_index].replace('"','').replace('/','-').split(',')
    
    
    
    # chage column name
    dataframe_column_name=["Date","Open","High","Low","Close","Volume"]
    Ksearch_key_word=["RPT_TIME","開盤價","最高價","最低價","收盤價","成交量"]
    df = pd.DataFrame()
    for i in range(0,len(dataframe_column_name),1):
        df[dataframe_column_name[i]] = data_list[Ksearch_key_word[i]]


#  check df rows
    rows=df.shape[0]
    if rows < 1:
        return False
    # chage column data type     
    try:
        for i in range(1,len(dataframe_column_name),1):
            
                if 'Volume' in  dataframe_column_name[i]:
                    df = df.astype({dataframe_column_name[i]: int})
                else:
                    df = df.astype({dataframe_column_name[i]: float})
#   check if nan or inf
                    
                    
        P=df.drop(['Date'], axis=1)
        count = np.isinf(P).values.sum()
        count_nan_in_df = P.isnull().sum().sum()
        if  count > 0 or count_nan_in_df > 0:
            col_name = P.columns.to_series()[np.isinf(P).any()]
            print(count)
            print(col_name) 
            return False
    except Exception as E:
        print('Error :', E)
        return False
#            print(df)
            
#            print(df.shape[0])
#            server error 500



    # 成交量*1000 換算單位將 張轉為股
    df['Volume']=df['Volume']*1000

    return df

def retry_download(stockid,counter):
#     counter=3
    while counter>0:
#    if counter>0:
        df=get_OHLC_goodinfo(stockid)
        if df is not False:
            break
        else:
            time.sleep(1)#sleep 1 s then retry 
        counter=counter-1
    if counter == 0:
        raise Exception('downlaod fail')
    return df
# get last_num
def get_data_last_row(stockid,last_row):
    A=retry_download(stockid=stockid,counter=3)    
    rows=A.shape[0]
    if last_row>rows:
        result=A
    else :
        result=A.tail(last_row)
    return result
def main():
    STOCK_ID=1303
    _start=date(2020,4,1)
    start = _start.isoformat()
    current_date = (_start+timedelta(days=10)).isoformat()
    count=1
    while True:
        A=get_OHLC_goodinfo(STOCK_ID)
        time.sleep(1)#sleep 1 s then retry 
        print(count)
        count=count+1
        if A is False:
            break
    A=get_OHLC_goodinfo_date(STOCK_ID,start,start)
    print(A.head())
    get_data_last_row(stockid=STOCK_ID,last_row=200)
if __name__ == '__main__':
    main()


# In[ ]:




