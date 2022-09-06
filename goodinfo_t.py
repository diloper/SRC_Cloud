#!/usr/bin/env python
# coding: utf-8

# In[14]:


# %load goodinfo_t.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, timedelta, datetime
import sqlite3
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
    df=get_OHLC_goodinfo(stockid)
    cond0=df['Date']<= end
    cond1=df['Date']>= start
    # df.loc[start:end]
    A=df.loc[cond0 & cond1]
    return A
#  Add referer in http header to get OHLC data
def get_OHLC_goodinfo(stockid):
    my_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',        'referer': 'https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid)}
    url='https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid) +'&CHT_CAT2=DATE'
    s = requests.Session()
    Resp=s.post(url,headers =my_headers)
    Resp.encoding =  'utf-8' #for chinese encode
    soup = BeautifulSoup(Resp.text, features="html.parser")
    _script_list = soup.findAll('script')
#     print(soup.find('script')['src'])
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
#     print(result)
    key_word='RPT_TIME:['
    start_index=result.find(key_word)
#     start_index=result.find('Records:{RPT_TIME')
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
        start_index =result.find(search_key_word[i]+":")+len(search_key_word[i]+":")+1
        end_index =result.index("],", start_index);
        data_list[search_key_word[i]]=result[start_index:end_index].replace('"','').replace('/','-').split(',')
    
    
#     print(data_list)
    # chage column name
    dataframe_column_name=["Date","Open","High","Low","Close","Volume"]
    Ksearch_key_word=["RPT_TIME","開盤價","最高價","最低價","收盤價","成交量"]
    df = pd.DataFrame()
    for i in range(0,len(dataframe_column_name),1):
        df[dataframe_column_name[i]] = data_list[Ksearch_key_word[i]]
#     print(df)
    # chage column data type     
    for i in range(1,len(dataframe_column_name),1):
        if 'Volume' in  dataframe_column_name[i]:
#             df = df.astype({'Discount':'int'})
            df = df.astype({dataframe_column_name[i]: float})
            df =df.round({dataframe_column_name[i]:0})
            df = df.astype({dataframe_column_name[i]: int})
        else:
            df = df.astype({dataframe_column_name[i]: float})


    # 成交量*1000 換算單位將 張轉為股
    df['Volume']=df['Volume']*1000

    return df
def main():
    STOCK_ID=2412
    _start=date(2022,5,24)
    start = _start.isoformat()
    current_date = (_start+timedelta(days=10)).isoformat()

    A=get_OHLC_goodinfo_date(STOCK_ID,start,start)
    print(A.head())
if __name__ == '__main__':
    main()

