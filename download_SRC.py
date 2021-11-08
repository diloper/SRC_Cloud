# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 09:05:42 2021

@author: user
"""
import gzip
import goodinfo_t as goodinfo
import sqlite3
import os

import re
import pandas as pd
from datetime import date, timedelta, datetime
import upload_file
D_Handel=upload_file.Google_Driver_API()
dDAY=120



def cal_twse_SRC_(df):
    column_names = ["Brk", "BuyNetAmt", "BuyNetVol","BuyAvg","SellAvg"]
# Brk","Price","NetBuyVol","BuyVol","SellVol"]
    Result = pd.DataFrame(columns = column_names)
#     df=pd.read_csv(filename)
    # remove nan row
    df.dropna(axis='index',inplace=True)
    buyer_name = df[["券商"]].copy()
    buyer_name = buyer_name.drop_duplicates()
    for i ,value in buyer_name.iterrows():
        
#         print(value['券商'])
        buyNetAmt,buyNetVol,buyAvg,SellAvg=item_SRC(df=df,name=value['券商'])
#         print(buyNetAmt)
#         dict1 = dict({"Brk":[value['券商']]
#                  , "BuyNetAmt":[buyNetAmt]
#                  , "BuyNetVol":[buyNetVol]
#                  ,"BuyAvg":[buyAvg]
#                  ,"SellAvg":[SellAvg]
#                 } )

        Result=Result.append({"Brk":value['券商']
                 , "BuyNetAmt":buyNetAmt
                 , "BuyNetVol":buyNetVol
                 ,"BuyAvg":buyAvg
                 ,"SellAvg":SellAvg},ignore_index=True)
    return Result


def jsontoSqlite(db_handler,_da,start,version=1):
    if version==1:
        dataset = json.loads(_da)
        dataframeA=list()
        for row in dataset['data']:
            # date = datetime.datetime.strptime(row[1], '%Y-%m-%d')
            # Brk;BuyNetAmt;BuyNetVol;BuyAvg;SellAvg"
            data = (str(row[0]), float(row[1]),int(row[2]), float(row[3]), float(row[4]))
            dataframeA.append(data)

    else :
#        dataframeA=_da
        dataframeA = _da.values.tolist()
#        selected_columns = _da[["Brk_id",  "BuyNetAmt" ,"BuyNetVol",  "BuyAvg",  "SellAvg"]]
#        dataframeA = selected_columns.copy()
    try:
        # db_handler = sqlite3.connect(sqlite_name)
        cursor = db_handler.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS "'+
        start+
        '"(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
        BrkId TEXT,BuyNetAmt REAL,BuyNetVol INTEGER,\
        BuyAvg REAL,SellAvg REAL)')
    except Exception as E:
        print('cursor Error :', E)



    try:
        cursor.executemany('insert into "'+
        start+
        '"(BrkId,BuyNetAmt,BuyNetVol,BuyAvg,SellAvg)\
        values(?,?,?,?,?)', dataframeA)
    except Exception as E:
        print('executemany Error : ', E)
    else:
        db_handler.commit()

def transform_to_(dataframe,Brk=False):
    
    V=dataframe['Brk']
    Brk_id=list()
    D=list()

    for i in V:
        x = re.search(r"[a-zA-Z0-9]{4}", str(i))
    #     for match in re.split(r"[a-zA-Z0-9]{4}", str(i)):
    # match now holds some text that doesn't match the regex
        if x is not None:
            Brk_id.append(x.group(0))
#             unmark to insert  Brk
            if Brk:
                Brk=re.split(r"[a-zA-Z0-9]{4}", str(i))
                D.append(Brk[1])
    dataframe.insert(loc=0, column='Brk_id', value=Brk_id, allow_duplicates=False)
    dataframe.drop(['Brk'], inplace=True, axis=1)
    dataframe.reset_index(drop=True, inplace=True)
    if Brk:
        dataframe.insert(loc=1, column='Brk', value=D, allow_duplicates=False)
    return dataframe


def get_D_dat_with_d(dir,stockid,d):
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    db_handler.create_function("REGEXP", 2, regexp)
    query='SELECT * from (select Date from OHLC order by date desc LIMIT "'+str(d)+'") ORDER by date asc LIMIT 1'
    df=pd.read_sql(query, db_handler)
    D_dat=df['Date'].iloc[0]
    db_handler.close()
    return D_dat
def regexp(expr, item):
    reg = re.compile(expr)
    return reg.search(item) is not None
def read_OHLC_From_sqlite(dir,stockid):

    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    cursor = db_handler.cursor()

    cursor.execute('SELECT name FROM  sqlite_master    WHERE type ="table" And name = "OHLC"')
    _OHLC = cursor.fetchall()
    if len(_OHLC)>0:
        cursor.execute('select Date from OHLC order by Date desc limit 1')
        tail = cursor.fetchone()
        return tail[0]
    else:
        return -1

def auto_download(dir,stockid,host_dir):
#    now = datetime.now()-timedelta(days=1)
    now = datetime.now()

#    date_before = date.today() - timedelta(days=20)
    Today = now.strftime("%Y-%m-%d")
#    print(Today)
    A=""
    _cur_OHCL_date=read_OHLC_From_sqlite(dir,stockid)
    if _cur_OHCL_date == -1:
        # if nodata then start by dDAY
        A=goodinfo.get_data_last_row(stockid=stockid,last_row=dDAY)
        # _start=date(2020,8,1).isoformat()
        # _start=Today-timedelta(days=dDAY)
        
    else:
        splitElements = _cur_OHCL_date.split('-')
        _cur_OHCL_date=date(int(splitElements[0]),int(splitElements[1]),int(splitElements[2]))+timedelta(days=1)
        cur_OHCL_date_Plus1Day = _cur_OHCL_date.isoformat()
        _start=cur_OHCL_date_Plus1Day
    
    # if already get OHCL than pass
    if A is not "":
        G=A
    else:
#    time.sleep(random.randint(0,1))#避免被判定為機器人 
        G=goodinfo.get_OHLC_goodinfo_date(stockid=stockid,start=_start,end=Today)
#     print(G)
    count_row = G.shape[0]  # gives number of row count
    if count_row>0:
        head=G["Date"].iloc[0] 
        tail=G["Date"].iloc[-1]# Access index of last element in data frame
        print('need upate from '+str(head)+" to "+str(tail))
        goodinfo.save2sqlite(dir=dir,stockid=stockid,data=G)
#    else:
#        print('OHLC Already upated')
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    db_handler.create_function("REGEXP", 2, regexp)
    # 比較 OHLC and SRC table data 
    query='select Date from OHLC WHERE OHLC.Date > (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name DESC limit 1)'
    df=pd.read_sql(query, db_handler)
    _date=None
    if  df.shape[0]>0:
        _date=df
    else:
        # check if 每筆SRC table 是否存在?
        query='SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name'
        rf=pd.read_sql(query, db_handler)
        # rf.shape[0]==0: 表示沒有資料，後續下載Date直接參考OHLC
        if  rf.shape[0]==0:
            query='select Date from OHLC order by date ASC'
            _date=pd.read_sql(query, db_handler)
            
        # else:
        #     return
    db_handler.close()
    try:
        if _date is not None:
            download_SRC_V2(dir=dir,stockid=stockid,df=_date,host_dir=host_dir)
    except Exception as E:
        print('Error : ', E)
        #remove SRCtable and OHLC accordig to dDAY
        # print(G)
        # select Date from OHLC order by date desc LIMIT dDAY sql below
        # SELECT * from (select Date from OHLC order by date desc LIMIT 100) ORDER by date asc LIMIT 1
#    else:

    D_dat=get_D_dat_with_d(dir=dir,stockid=stockid,d=dDAY)
    # DELETE FROM OHLC WHERE date < "2020-11-02"
    # query='SELECT Date from OHLC WHERE date < "'+str(D_dat)+'"'
    # df=pd.read_sql(query, db_handler)
    # for row in df.itertuples():
        # item=getattr(row, 'Date') 
    
    Remove_OHLC_SRC_(dir=dir,stockid=stockid,day_before=D_dat)
    
    VACUUM(dir=dir,stockid=stockid)
    
def item_SRC(df,name):
    G1=df.query('券商 == "'+name+'"')
    buy_vol=G1['買進股數'].sum()
    sell_vol=G1['賣出股數'].sum()
    buyNetVol=int(buy_vol-sell_vol)
    As=0
    Dv=0
    for i ,value in G1.iterrows():
    #     print(i,value['買進股數'],value['價格'])
        As+=int(value['買進股數']) * float(value['價格'])
        Dv+=int(value['賣出股數']) * float(value['價格'])
    #     print(As)

    buyNetAmt=As-Dv
    buyNetAmt
    SellAvg=0
    buyAvg=0
    if buy_vol != 0:
        buyAvg=As/buy_vol
    
    if sell_vol != 0:
        SellAvg=Dv/sell_vol
    buyAvg = float("{:.2f}".format(buyAvg))
    SellAvg = float("{:.2f}".format(SellAvg))
    
    return buyNetAmt,buyNetVol,buyAvg,SellAvg    
def Remove_OHLC_SRC_(dir,stockid,day_before):
    d_str=str(day_before)
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    db_handler.create_function("REGEXP", 2, regexp)
    # DA='2018-02-01'

    
    # cursor.execute(ST)
    cursor = db_handler.cursor()
    dropTableStatement='DELETE FROM OHLC WHERE Date < "'+d_str+'"'
    cursor.execute(dropTableStatement)
    
    query='SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" EXCEPT select Date from OHLC'
    # query='select Date from OHLC WHERE OHLC.Date > (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name DESC limit 1)'
    df=pd.read_sql(query, db_handler)
    for row in df.itertuples():
        item=getattr(row, 'name')
#        item1=getattr(row, 'Date')
#        item2=getattr(row, 'Date')
#        print(item)
        dropTableStatement = 'drop table if exists "'+item+'"'
        cursor.execute(dropTableStatement)

    
    
    db_handler.commit()
    db_handler.close()
    
    
def VACUUM(dir,stockid):
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    db_handler.execute("VACUUM")
    db_handler.commit()
    db_handler.close
    
    
def Reduce_SRC_data(dir,stockid,SRC_tablename):
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    db_handler.create_function("REGEXP", 2, regexp)
    
    
    cursor = db_handler.cursor()
    D_dat=get_D_dat_with_d(dir=dir,stockid=stockid,d=dDAY)
    # # remove old data
    dropTableStatement='DELETE FROM "'+str(SRC_tablename) +'"WHERE Date < "'+D_dat+'"'
    cursor.execute(dropTableStatement)
    
    db_handler.commit()
    db_handler.close()
    
def get_sqlite_local_file(last_folder,Nodir=True):
    # 歷遍資料夾下的的檔案名稱，符合後紀錄於local_files
    local_files=list()

    for root, dirs, files in os.walk(last_folder, topdown=True):
        for name in files:
            modify_time=modification_date(last_folder+'/'+name)
            name=name.replace("'","")
            if 'sqlite' in name and len(name) == 11:
                name=name.replace(".sqlite","")
                if Nodir is False:
                    data= (last_folder+'/'+name,modify_time)
                else:
                    data= (name,modify_time)
                local_files.append(data)
#                 file_time.append(modify_time)
    return local_files 

def modification_date(filename):
    t = os.path.getmtime(filename)
    R=datetime.fromtimestamp(t)
    return R.strftime("%Y-%m-%d")    

def Stream_SRC(D_Handel,name,date):
    try:
        folder_id=D_Handel.search_folder(name='Stock db')
        folder_id=D_Handel.search_folder(name=date,folder_id=folder_id)
        file_id=D_Handel.search_file(name=name,folder_id=folder_id)
    #     print(file_id)
        if file_id==None:
            return False,file_id
        html=D_Handel.downloadFile_SRC(file_id=file_id,local_filepath="..",IO=True)
        # BytesIO use tell() to check  current stream position.
        html.seek(0)
    #     print(html.tell())
        f1 = gzip.GzipFile(fileobj=html)
        RT=pd.read_csv(f1)
    except Exception as E:
        print('Error : ', E)
        return False,file_id
#     print(RT)
#     byte_str1 = f1.read()
#     text_obj1 = byte_str1.decode('UTF-8')  # Or use the encoding you expect
#     print(text_obj1)
    return True,RT


def host_file(host_dir):
   
    try :
        f1 = gzip.GzipFile(filename=host_dir)
        RT=pd.read_csv(f1)
        
    except Exception as E:
            print('Error : ', E)
            return False,file_id
    return True,RT 
    
    
    
def download_SRC(dir,stockid,start,end,host_dir):
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    Today = date.today().isoformat()  
    # _range=end-start
    # if _range.days is 0:
    #     _range=timedelta(days=1)
    # for i in range(1,_range.days+1):
    _start=start
    end_date=_start
    if _start > Today:#To avoid pass Today
        print(str(_start)+'>'+str(Today))
        return
    print(str(_start))
    c = db_handler.cursor()
                
    #get the count of tables with the name
    c.execute('SELECT count(name) FROM sqlite_master WHERE type="table" AND name="'+
    _start+'"')

    #if the count is 1 (table exists)
    if c.fetchone()[0]==1 : 
        print('Table exists.'+str(_start))
        return
    else :
        # print('Table does not exist.')
        
        # check if data exist local host
        # if _start in host_dir:
            
        key = _start
        d = [ x for x in host_dir if key in x ]
        if len(d)==1:
            print(host_dir[key])
            # print(d)
            host_dir=os.path.join(host_dir[key], str(stockid)+'.gz')
            _da,S =host_file(host_dir=host_dir)
        else:    
            # download from google drive
            _da,S=Stream_SRC(D_Handel,name=str(stockid)+'.gz',date=_start)
        
#        _da,_dataframe=get_stock__buyAndselldetail_2(stockid,_start)
        #older above data format
#        _da=get_stock__buyAndselldetail(stockid,_start, end_date)
        if _da != False:
            TT=cal_twse_SRC_(S)
            _dataframe=transform_to_(TT)
            jsontoSqlite(db_handler,_dataframe,_start,version=2)

    db_handler.close()
    
def download_SRC_V2(dir,stockid,host_dir,df=None):
    
    _date=df['Date']
    for i in _date:
        # start=datetime.strptime(i, '%Y-%m-%d')
#         print(i)
        download_SRC(dir,stockid,i,i,host_dir=host_dir)
        
def caculate_SRC_sqlit_version_with_size(dir,stockid,period=20,c_size=15,force=False):
    try:
        sqlite_name=dir+str(stockid)+".sqlite"
        db_handler = sqlite3.connect(sqlite_name)
        db_handler.create_function("REGEXP", 2, regexp)
        cursor = db_handler.cursor()
        df = pd.read_sql_query('SELECT OHLC.Date, OHLC.Volume FROM (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC) as SRC_list INNER JOIN OHLC ON SRC_list.name = OHLC.Date ORDER BY OHLC.Date ASC;', db_handler)
        SRC_table_name='SRC'
        
            
        if c_size != 15:
            SRC_table_name=SRC_table_name+str(c_size)
            
        if period != 20:
            SRC_table_name=SRC_table_name+'_period_'+str(period)
        if force:
            dropTableStatement = 'drop table if exists '+SRC_table_name
            cursor.execute(dropTableStatement)
            cursor.execute('CREATE TABLE IF NOT EXISTS "'+SRC_table_name+
            '"(Date Text PRIMARY KEY NOT NULL,\
            "M20" REAL)')
        else :
           
            # 檢查SRC 表是否存在            
            cursor.execute('SELECT name FROM  sqlite_master WHERE type ="table" And name="'+SRC_table_name+'"')
            _SRC = cursor.fetchall()
            if len(_SRC)==0:
                cursor.execute('CREATE TABLE IF NOT EXISTS "'+SRC_table_name+
                '"(Date Text PRIMARY KEY NOT NULL,\
                "M20" REAL)')
                    
            # 已更新過則 當 SRC 與OHLC 最後一筆資料的日期相同時
            cursor.execute('SELECT * FROM  (SELECT Date FROM  OHLC ORDER BY Date desc limit 1) As B EXCEPT SELECT * from             (SELECT Date FROM  "'+SRC_table_name+'"  ORDER BY Date desc limit 1 ) AS  A ')
            _SRC = cursor.fetchall()
            if len(_SRC)==0:
                print(SRC_table_name+"table Ready")
                return SRC_table_name
            else:
                cursor.execute('select * from  OHLC where date >= (select date from (select '+SRC_table_name+'.* from "'+SRC_table_name+'" order by '+SRC_table_name+'.Date DESC\
                                          limit 19) as B order by B.Date asc limit 1)')
                _SRC = cursor.fetchall()
                if len(_SRC) != 0:
                    df =pd.read_sql_query('select * from  OHLC where date >= (select date from (select '+SRC_table_name+'.* from "'+SRC_table_name+'" order by '+SRC_table_name+'.Date DESC\
                                          limit 19) as B order by B.Date asc limit 1)', db_handler)
                else:
                    df =pd.read_sql_query('select OHLC.Date,OHLC.Volume FROM (SELECT OHLC.Date  FROM (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC)  as SRC_list INNER JOIN OHLC ON SRC_list.name = OHLC.Date                 EXCEPT SELECT Date FROM  "'+SRC_table_name+'"  ORDER BY Date asc)                as K INNER JOIN OHLC ON K.Date=OHLC.Date ORDER BY OHLC.Date ASC', db_handler)
               
            # while "'+SRC_table_name+'"　table exist,find disparities 
        sdf=df.loc[:,['Date']]
#                 print(sdf)
                
        # cursor.execute('SELECT OHLC.Date, OHLC.Volume FROM (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC) as SRC_list INNER JOIN OHLC ON SRC_list.name = OHLC.Date;')
        # SRC_length=len(table_list)
        #  print(SRC_length)
        
        table_list=sdf.values.tolist()
        SRC_data_index=len(table_list)-1
        
        # rows = cursor.fetchall()
        rows_length=df.shape[0]-1
        # print(rows_length)
        # cursor.execute('select  Volume  from OHLC \
        #     ORDER BY Date ASC;')
        # Volume_rows = cursor.fetchall()
        start_date=""
        # if SRC_data_index > rows_length:
        #     print("error  table_list_length > rows_length")
        #     start_date= rows[ rows_length ]
        #     return
        # elif SRC_data_index < rows_length:

        #     print("error  table_list_length < rows_length")
        #     start_date=table_list[ SRC_data_index ]
        #     return
        # else:
        #     print("data size match")
        #     start_date=table_list[ SRC_data_index ]
        
        _start_index=len(table_list)
        # print(_start_index)
        # Create SRC table
        # cursor.execute('CREATE TABLE IF NOT EXISTS "SRC"\
        #     (Date TEXT,M20 REAL,\
        #     M10 REAL,M5 REAL)')
        # period=period-1
        # df={}
        date=[]
        
        # Times 執行測試
        Times=len(table_list)-period+1
        src_value=[]
        for i in range(0,Times):
            start_index=_start_index-period-i
            end_index=_start_index-i
            # A=Volume_rows[start_index:end_index+1]
#             sdf=df.loc[:,['Date']]
            Volume_list=df.loc[start_index:end_index,['Volume']]
            period_vol=0
            # print(Volume_list)
            # num = int(input('How many numbers: '))
            period_vol=(Volume_list['Volume']/1000).sum()

            # for _Volume in Volume_list['Volume']:
            #     period_vol=period_vol+(_Volume/1000)

            # print(period_vol)
            # print("period_vol "+str(period_vol))
            start=df.at[start_index,'Date']
            current_date=df.at[end_index-1,'Date']
            result=get_stock_SRC_(dir=dir,stockid=stockid,start=start,current_date=current_date,period_vol=period_vol,date_size=c_size,date_source="sqlite")
            
            # print(current_date +" "+str(result))
            # data = (str(current_date), float(result))
            date.append(str(current_date))
            src_value.append(result)
            
        _dict = {'Date': date,'M20': src_value}
        df = pd.DataFrame.from_dict(_dict)
#         cursor.execute('CREATE TABLE IF NOT EXISTS "SRC'+
#         '"(Date Text PRIMARY KEY NOT NULL,\
#         "M20" REAL)')
        cursor.executemany('insert or ignore into "'+SRC_table_name+
        '" ("Date","M20")\
        values(?,?)', df.values.tolist())
        db_handler.commit()
#         df.to_sql("SRC", db_handler, if_exists="append",index=False)
    except Exception as E:
        print('Error : ', E)
        
        
    # D_dat=get_D_dat_with_d(dir=dir,stockid=stockid,d=dDAY)
    # # remove old data
    # dropTableStatement='DELETE FROM "'+str(SRC_table_name) +'"WHERE Date < "'+D_dat+'"'
    # cursor.execute(dropTableStatement)
    
    db_handler.close()
    
    return SRC_table_name


def output2csv(dir,stockid):
    sqlite_name=dir+str(stockid)+".sqlite"
    conn = sqlite3.connect(sqlite_name, isolation_level=None,
                           detect_types=sqlite3.PARSE_COLNAMES)
    db_df = pd.read_sql_query("SELECT SRC.Date  ,SRC.M20 ,SRC30.M20 AS SRC30 ,SRC60.M20 SRC60,OHLC.Close ,OHLC.Volume\
            FROM SRC , SRC30 ,SRC60,OHLC WHERE SRC.Date = SRC60.Date and SRC.Date= SRC30.Date and SRC.Date=OHLC.Date order by SRC.Date", conn)

# db_df.rename(columns={"A": "a", "B": "c"},inplace=True)
    
    db_df.to_csv(dir+str(stockid)+'.csv', index=False)
    conn.close()
    
# 直接與sqlite中進行SRC計算(前提須有每日成交量)
def caculate_SRC_sqlit_version(dir,stockid,period=20,force=False):
    A=caculate_SRC_sqlit_version_with_size(dir=dir,stockid=stockid,period=period,force=force)
    return A

def price_change(dir,stockid,df,period=20):
    # change=(data['Close'][target]-data['Close'][index])/data['Close'][index]
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    cursor = db_handler.cursor()
    # J=df.index.values

    _period=str(period)
    df[_period+'MAX/MIN']=''
    df['Rise']=0.0
    df['MAX_Date']=''
    df['Min_Date']=''
 
    df.loc[:,'indexCompare']=df.index.values

    df.loc[:,'indexCompare']=df['indexCompare']-df['indexCompare'].shift(1)
 
    drop_list=[]
    for i , row in  df.iterrows():
        if(pd.isnull(row['indexCompare'])):
            continue
       # 透過index - index shift 為一的特性，append連續資料，除了連續資料的第一個
        if int(row['indexCompare']) == 1:
            drop_list.append(i)
        
    # drop 連續的資料
    df=df.drop(drop_list)
    del df['indexCompare']
    del df['green_red_Condiction']
    # print(df)

    for index, row in df.iterrows():
        # print(i)
        cursor.execute('select "Date" , "Close" from OHLC             where Date<="'+str(row['Date'])+'"order by Date desc             limit '+_period)
        _dfA = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])
        # print(_dfA)
        
        Max_items=_dfA[_dfA['Close']==_dfA['Close'].max()]
        Min_items=_dfA[_dfA['Close']==_dfA['Close'].min()]
        # print(Max_items.iloc[0]['Close']) 
        df.at[index,_period+'MAX/MIN']=str(Max_items.iloc[0]['Close'])+'/'+str(Min_items.iloc[0]['Close'])
        # df.at[_period+'MAX/MIN'][index]=str(Max_items.iloc[0]['Close'])+'/'+str(Min_items.iloc[0]['Close'])
        df.at[index,'MAX_Date']=Max_items.iloc[0]['Date']
        df.at[index,'Rise']=(int(Max_items.iloc[0]['Close'])-Min_items.iloc[0]['Close'])/Min_items.iloc[0]['Close']
        df.at[index,'Min_Date']=Min_items.iloc[0]['Date']
        
        # df['Rise'][index]=(int(Max_items.iloc[0]['Close'])-Min_items.iloc[0]['Close'])/Min_items.iloc[0]['Close']
        # df['Min_Date'][index]=Min_items.iloc[0]['Date']
    # print(Max_items)
    # print(df)
    db_handler.close()
    return df



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
       
    else:
        url_prefix="https://www.above.tw/json/StocksChips_BrkBuyNet?"
        url_suffix="&show=Brk;BuyNetAmt;BuyNetVol"
        # asc / desc  差異點在url 的結尾 (:a :d )
        stockid=str(stockid)
        url_asc=url_prefix+time_interval+"&sym="+stockid+"&psize="        +_date_size+"&p=1&s=BuyNetVol:a"+url_suffix

        url_desc=url_prefix+time_interval+"&sym="+stockid+"&psize="        +_date_size+"&p=1&s=BuyNetVol:d"+url_suffix

        re_asc = requests.get(url_asc)
        time.sleep(random.randint(0,1))#避免被判定為機器人 
        re_desc=requests.get(url_desc)
        buy_top15_data_size=get_total(re_desc,2)
        sell_top_data_size=get_total(re_asc,2)
    # 籌碼集中度=(區間買超前15的買張合計-區間賣超前15名的賣張合計) / 區間成交量
    
    return (buy_top15_data_size-abs(sell_top_data_size))/period_vol
        
def get_total(data,select_index):
    json_array = json.loads(data.text)
    # print(json_array['cheaders'])
    temp=0
    for item in json_array['data']:
#print(item[0],item[1],item[3])
        temp+=int(item[select_index])/1000
        
    return temp
