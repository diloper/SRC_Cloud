#!/usr/bin/env python
# coding: utf-8

# In[2]:


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
import progressbar
dir="./financing/"

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
        print("date ==0")
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
import search_tool
dirp='small_test'
import download_SRC as SRCtool
def updatefinancingsqlv2():
    A=SRCtool.get_sqlite_local_file(dirp)
    for item in progressbar.progressbar(A):
        try:
            print(item[0])
            update_financingsql(dir,stockid=item[0])
        except Exception as e: 
            print("except"+item[0])
            print(e)

         
def main():

    today = date.today()
    STOCK_ID=2412

    _start = date(2020,1,1)
    start = _start.isoformat()
    
    _list=search_tool.local_files_v1(dir_target="financing",reg="\d{4}\d{2}\d{2}.gz")
    _list.sort(reverse = False)
    _df = pd.DataFrame(_list,columns=["Date"])
    _df = _df.replace('financing/','', regex=True)
    _df = _df.replace('.gz','', regex=True)
    #print(_df)
    current_date = today.isoformat()
    if len(_list)>0:
        dt_obj = datetime.strptime(str(_df['Date'].iloc[0]),'%Y%m%d')
        yearfolder=str(dt_obj.year)
        start=dt_obj.strftime("%Y-%m-%d")

    print(current_date)
    


    print(start)
    A=goodinfo_t.get_OHLC_goodinfo_date(STOCK_ID,start,current_date)
#     print(A)
    A.replace({'-':''}, regex=True,inplace=True)
    D_Handel=upload_file.Google_Driver_API()
    folder_id=D_Handel.search_folder(name='financing')
#    a_folder_id=D_Handel.search_folders(folder_id=folder_id)
    

#    filesdf = pd.DataFrame(files)
    tmp_list=[]
 #    retrive folder_id and get files   
#   使用找出file extension gz，再用反向比較
#    K_folderid=filesdf[~filesdf['name'].str.contains(r'gz', regex=True, na=False)]
#    for _folder_id in a_folder_id:
#        print(_folder_id["id"])
#        a_files=D_Handel.list_folder_files(_folder_id["id"])
#        if a_files is not None:
#            tmp_list.extend(a_files)
#    
#    
#    filesdf = pd.DataFrame(tmp_list) 
#    
#    filesdf.replace({'.gz':''}, regex=True,inplace=True)
#    
    t2=_df['Date'].tolist()
    print(t2)
    t1=A['Date'].tolist()
    # R = pd.DataFrame(u)
    T=list(set(t1) - set(t2))
    # R.drop_duplicates(inplace=True)
    print(T)
    for item in progressbar.progressbar(T):
        print(item)
        upload_finace_data(item,D_Handel,folder_id)
    updatefinancingsqlv2()
    
    



if __name__ == '__main__':
    main()
    #updatefinancingsqlv2()
