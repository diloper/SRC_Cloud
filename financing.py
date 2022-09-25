#!/usr/bin/env python
# coding: utf-8

# In[16]:


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


def main():
    STOCK_ID=2412
    _start=date(2022,5,24)
    start = _start.isoformat()
    current_date = (_start+timedelta(days=10)).isoformat()

    A=get_financing_date(STOCK_ID,start,start)
    print(A.head())
if __name__ == '__main__':
#     main()





import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, timedelta, datetime
import sqlite3
from time import sleep


import json

def get_financing_v2(date='20220104',stockid='0050'):

    my_headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
                 }
    #     url='https://goodinfo.tw/StockInfo/ShowK_Chart.asp?STOCK_ID='+str(stockid) +'&CHT_CAT2=DATE'
    url='https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date='+str(date) +'&selectType=ALL'
    s = requests.Session()
    try:

        Resp=s.post(url,headers =my_headers)
        Resp.encoding =  'utf-8' #for chinese encode
        # print(Resp.text)
        # parse Resp.text:
        if len(Resp.text)<1:
            print("data size <0")
            return None
        y = json.loads(Resp.text)
    except (ValueError):
#         print(Resp+'size'+len(Resp.text))
        return None 
    except (OSError) as inst:
        print(inst)
        return None 
#     print(len(y['data']))
   
    df = pd.DataFrame (y['data'], columns = ['股票代號', '股票名稱', '買進', '賣出', '現金償還', '前日餘額', '今日餘額', '限額', '買進V', '賣出V', '現券償還', '前日餘額V', '今日餘額V', '限額', '資券互抵', '註記'])

    del_target=[ '股票名稱', '現金償還','買進V', '賣出V', '現券償還', '前日餘額V', '今日餘額V', '限額', '資券互抵', '註記']

    for i in del_target:
        del df[i]
    sleep(0.15)
    return df.query('股票代號 == "'+stockid+'"')


stockid='2347'
# get_financing_v2(date='20220923',stockid=stockid)


# In[25]:


def readOHCLsql(stockid):
    dir='./small_test/'

    price_db_name=dir+str(stockid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    price_db_handler = sqlite3.connect(price_db_name)
    # tat='SELECT OHLC.Date,OHLC.Close,SRC.M20 from OHLC WHERE Date <="'+ str(date) +'"order by Date desc  LIMIT "'+str(boundary)+'"'
    tat='SELECT OHLC.Date,OHLC.Close from OHLC'
    # df = pd.read_sql_query('SELECT OHLC.Date,OHLC.Close, SRC.M20 from SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date ASC',price_db_handler)
    # df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    dfA = pd.read_sql_query(tat,price_db_handler)
    price_db_handler.close()
    return dfA


# In[26]:


def savetosql(stockid,dfH):
    db_handler = sqlite3.connect('financing'+stockid+".sqlite")
    cursor = db_handler.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS "financing"                (`買進` INTEGER  NOT NULL,                `賣出` INTEGER NOT NULL,                `前日餘額`TEXT NOT NULL,                `今日餘額`TEXT NOT NULL,                `Date`TEXT  PRIMARY KEY NOT NULL)')
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


# In[27]:


def readfromsql(stockid,dateformate=False):
    db_handler = sqlite3.connect('financing'+stockid+".sqlite")
    cursor = db_handler.cursor()
    cursor.execute('SELECT name FROM  sqlite_master    WHERE type ="table" And name = "financing"')
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


# In[28]:


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


# In[37]:


dfB=readfromsql(stockid,dateformate=True)
dfA=readOHCLsql(stockid)
type(dfB['Date'])


# In[38]:


type(dfA['Date'])


# In[ ]:





# In[ ]:





# In[ ]:





# In[41]:


# dfA
# dfA.join(dfB, on='Date')
R=dfA.set_index('Date').join(dfB.set_index('Date'))
# pd.merge(dfA,dfB,how='inner',left_on=['Date'],right_on=['Date'])


# In[42]:


R.to_csv(stockid+'.csv')  


# In[43]:


R


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




