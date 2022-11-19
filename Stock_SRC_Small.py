import sys
import requests
import json
import time
import random
import sqlite3
from datetime import date, timedelta, datetime
import re
import pandas as pd
import search_tool
import os
import read_file_analyze as ka
import download_SRC as SRCtool
# download_SRC(stockid=4934,start=date(2019,1,1))

# https://github.com/thomasnield/oreilly_intermediate_sql_for_data/issues/5



# 需先使用 Top15 計算每日買賣前15的總量
# 採用前5天進行平均與當天比較分點進出
def TOP15_MA5_Ratio(stockid):
    sqlite_name=str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    # cursor = db_handler.cursor()
    df = pd.read_sql_query('SELECT * from "Top15_buy_sell_5_moving average"', db_handler)
    # Verify that result of SQL query is stored in the dataframe
    print(df.head())
    df['Top15BuyVol_5MA']=df['Top15BuyVol'].rolling(window=5,min_periods=5).mean()
    df['Top15BuyVol_5MA']=df['Top15BuyVol_5MA'].shift(1)
    df['Ratio_BuyVol']=df['Top15BuyVol']/df['Top15BuyVol_5MA']
    df['Top15SellVol_5MA']=df['Top15SellVol'].rolling(window=5,min_periods=5).mean()
    df['Top15SellVol_5MA']=df['Top15SellVol_5MA'].shift(1)
    df['Ratio_SellVol']=df['Top15SellVol']/df['Top15SellVol_5MA']
    # df.drop(['index'], axis=1)
    df.to_sql("Top15_buy_sell_5_moving average", db_handler, if_exists="replace",index=False)
    db_handler.close()
# 計算每日買賣前15的總量
def Top15(stockid):
    sqlite_name=str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    db_handler.create_function("REGEXP", 2, regexp)
    cursor = db_handler.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS "Top15_buy_sell_5_moving average"        (Date_Time TEXT,Top15BuyVol INTEGER,        Top15BuyVol_5MA INTEGER,Top15SellVol INTEGER,        Top15SellVol_5MA INTEGER)')
    cursor.execute('SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$"          ORDER BY name ASC;')
    # cursor.fetchone()
    # remaining_rows = cursor.fetchall()
    
    rows = cursor.fetchall()
    for row in rows:
    # while row is not None:
        print(row[0])
        cursor.execute('select SUM(BuyNetVol) from (SELECT  BuyNetVol FROM              "'+row[0]+'"             order by id asc limit 15)')
        Top15_buy=cursor.fetchone()
        # print(Top15_buy[0])
        cursor.execute('select SUM(BuyNetVol) from (SELECT  BuyNetVol FROM              "'+row[0]+'"             order by id desc limit 15)')
        Top15_sell=cursor.fetchone()
        # print(Top15_sell[0])
        data = (str(row[0]), int(Top15_buy[0]),int(0),int(Top15_sell[0]),int(0))
        cursor.execute('insert into "Top15_buy_sell_5_moving average"             (Date_Time,Top15BuyVol,Top15BuyVol_5MA,Top15SellVol,Top15SellVol_5MA)        values(?,?,?,?,?)', data)
    db_handler.commit()
    db_handler.close()





def jsontosqlite3():
    url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/SFP.json?api_key=sCJNpBm_3CDYKxgRbMFS'
    content = requests.get(url).content
    dataset = json.loads(content)
    dataframe=list()
    for row in dataset['datatable']['data']:
        date = datetime.strptime(row[1], '%Y-%m-%d')
        date1 = datetime.strptime(row[9],'%Y-%m-%d')
        data = (str(row[0]),date, float(row[2]), float(row[3]), float(row[4]), float(row[5]), float(row[6]), float(row[7]), float(row[8]),date1)
        dataframe.append(data)
    try:
        db = sqlite3.connect('Employee')
        cursor = db.cursor()
        cursor.execute('''create table stocks (ticker varchar(50),date datetime,open float,high float, low float,
        close float,volume float,dividends float,closeunadj float,lastupdated datetime)''')

    except Exception as E:
        print('Error :', E)
    else:
        print('table created')

    try:
        cursor.executemany('insert into stocks values(?,?,?,?,?,?,?,?,?,?)', dataframe)
    except Exception as E:
        print('Error : ', E)
    else:
        db.commit()
        print('data inserted')
    
# 累加分點劵商的買賣差



def get_stock__buyAndselldetail(_stockid,start, current_date,_date_size=3000):
    time_interval="sd="+start+"&ed="+current_date
    stockid=str(_stockid)
    date_size=str(_date_size)
    
    url_prefix="https://www.above.tw/json/StocksChips_BrkBuyNet?"
    url_suffix="&show=Brk;BuyNetAmt;BuyNetVol;BuyAvg;SellAvg"
    # asc / desc  差異點在url 的結尾 (:a :d )
    url_asc=url_prefix+time_interval+"&sym="+stockid+"&psize="    +date_size+"&p=1&s=BuyNetVol:d"+url_suffix
    re_asc = requests.get(url_asc)
    time.sleep(random.randint(0,1))#避免被判定為機器人 
    size=json.loads(re_asc.text)['rows']
    # print(size)
    if size>_date_size:
        return -1
    elif size == 0:
        return -1
    
    return re_asc.text






def get_stock__buyAndselldetail_2(stockid,date):
    column_names = ["Brk", "BuyNetAmt", "BuyNetVol","BuyAvg","SellAvg"]
    Result = pd.DataFrame(columns = column_names)
    url_prefix='https://www.above.tw/json/TProStkChips/BrkTrade?'
    url_prefix+="day="+str(date)+"&sym="+str(stockid)
#     test='https://www.above.tw/json/TProStkChips/BrkTrade?day=2020-08-19&sym=1301'
#    print(url_prefix)
    return_V=1
    try:
        re_asc = requests.get(url_prefix)
        json_ = json.loads(re_asc.text)
        dfa = pd.DataFrame(json_['data'])
        head_=json_['headers']
        Result=list()
    #     print(head_)
        dfa.columns =head_
        buyer_name = dfa[["Brk"]].copy()
        buyer_name = buyer_name.drop_duplicates()
        buyer_name.reset_index()
        for i ,value in buyer_name.iterrows():
            buyNetAmt,buyNetVol,buyAvg,SellAvg=item_SRC_2(df=dfa,name=value['Brk'])
            data = (str(value['Brk']), float(buyNetAmt),int(buyNetVol), float(buyAvg), float(SellAvg))
            Result.append(data)
#            Result=Result.append({"Brk":value['Brk']
#                     , "BuyNetAmt":buyNetAmt
#                     , "BuyNetVol":buyNetVol
#                     ,"BuyAvg":buyAvg
#                     ,"SellAvg":SellAvg},ignore_index=True)
    
    
    except Exception as E:
        print('Error :', E)
        return_V=-1
    return return_V,Result
def item_SRC_2(df,name):
#     df.head()
    key=(str(name))
#     buyNetAmt,buyNetVol,buyAvg,SellAvg=item_SRC(df=df,name=value['券商'])
#     print(key)
    G1=df.query('Brk == "'+key+'"')
   
    buy_vol=G1['BuyVol'].sum()
    sell_vol=G1['SellVol'].sum()
    buyNetVol=int(buy_vol-sell_vol)
    As=0
    Dv=0
    for i ,value in G1.iterrows():
    #     print(i,value['買進股數'],value['價格'])
        As+=int(value['BuyVol']) * float(value['Price'])
        Dv+=int(value['SellVol']) * float(value['Price'])
    buyNetAmt=As-Dv
    buyNetAmt
    buyAvg=0
    SellAvg=0
    if buy_vol!=0:
        buyAvg=As/buy_vol
        buyAvg = float("{:.2f}".format(buyAvg))
#         print(buy_vol,sell_vol)
    if sell_vol!=0:
        SellAvg=Dv/sell_vol
        SellAvg = float("{:.2f}".format(SellAvg))
    return buyNetAmt,buyNetVol,buyAvg,SellAvg


def check_file_exist(dir,stockid):
    if os.path.isfile(str(dir)+str(stockid)+".sqlite") is True:
        return True
    else:
        return False
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
    # for item in rows:
    #     if 

# def Data_alignment(dir=dir,stockid=item):
#     # SELECT *  from (SELECT  Date  from OHLC minus ) EXCEPT  SELECT * FROM  (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC) 
#     sqlite_name=dir+str(stockid)+".sqlite"
#     db_handler = sqlite3.connect(sqlite_name)
#     cursor = db_handler.cursor()
#     cursor.execute('SELECT *  from (SELECT  Date  from OHLC minus ) EXCEPT  SELECT * FROM  (SELECT name FROM  sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC)' )
#     cursor.fetchall()
def create_sqlite_with_comment():
    conn = sqlite3.connect('users.sqlite')
    cur = conn.cursor()
    # add comment in sql schema
    cur.execute('CREATE TABLE IF NOT EXISTS User  ( uid INTEGER,  flags INTEGER "-- Another field comment" )')
    # read comment sql schema

    for row in cur.execute("select sql from sqlite_master where sql not NULL").fetchall():
        print(row)
    conn.close()
    



        
def update_trace_tabel(dir,stockid):
    # 檢查檔案是否存在
    Rawdata=dir+str(stockid)+".sqlite"
    
    filename="TK"+str(stockid)+".sqlite"
    filepath=dir+filename
    if os.path.isfile(filepath) is not True:
        return
    try:
        Rawdata_handler = sqlite3.connect(Rawdata)
#        print(filename)
        sqlite_name=dir+filename
        db_handler = sqlite3.connect(sqlite_name)
        db_handler.create_function("REGEXP", 2, regexp)
        df = pd.read_sql_query('SELECT name FROM sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC',db_handler)
        for i , trace in  df['name'].iteritems(): 
    #         print(i)
    #        print("Trace table name="+trace)
    #         into trace table
            trace_table = pd.read_sql_query('SELECT * from "'+str(trace)+'" ORDER BY Date ASC;',db_handler)
    #         print(trace_table.head())
            count_row = trace_table.shape[0]
            tail_len=20-count_row
            if tail_len == 0:
    #            print("Trace table name="+trace+"size = 20")
                continue
            last=trace_table.at[count_row-1,'Date']
            print("trace_table"+last)
    #         reference to OHLC table
            request_len=count_row
            request='SELECT Date ,Close,Volume FROM  OHLC order by Date asc'
            OHLC_table = pd.read_sql_query(request,Rawdata_handler)
            OHLC_count_row = OHLC_table.shape[0]
    #         print(OHLC_table.head())
            Index_label = OHLC_table[OHLC_table['Date']==last].index.values
            if (OHLC_count_row-1) ==Index_label:
    #           print("Trace table name="+trace+" data is updated")
                return
            df=OHLC_table.loc[Index_label[0]-27:Index_label[0]+tail_len,:]
    #         print(A.head())
            A=ka.bollinger_bands(df,bis=2)
            SRC = pd.read_sql_query('SELECT * FROM  SRC order by Date asc', Rawdata_handler)

            SRC.loc[:,'5dt_M20']=(SRC['M20']-SRC['M20'].shift(5))/5
            SRC.loc[:,'M20_shift10']=SRC['M20'].shift(10)
            SRC.loc[:,'10dt_M20']=(SRC['M20']-SRC['M20_shift10'])/10

            result = pd.merge(SRC, A, how='inner', on='Date')
            target_label = result[result['Date']==last].index.values  
            sql_df=result.loc[result['Date'] > last,['Date','M20','5dt_M20','bolling Bandwith 10MA']]
    #         sql_df.to_csv(str(last)+'.csv', mode='w', header=True,index=False)
            print(sql_df.head())

            cursor = db_handler.cursor()
            cursor.executemany('insert or ignore into "'+
            str(trace)+
            '" ("Date","M20","5dt_M20","bolling Bandwith 10MA")\
            values(?,?,?,?)', sql_df.values.tolist())
            db_handler.commit()
    except Exception as E:
        print('Error :', E)
    Rawdata_handler.close()
    db_handler.close()

def save_trace_table(cnx,df,timestamp):
    try:
        # db_handler = sqlite3.connect(sqlite_name)
        cursor = cnx.cursor()
    #         cursor.execute('CREATE TABLE IF NOT EXISTS "'+
    #         str(stockid)+
    #         '"(Date Text PRIMARY KEY NOT NULL,\
    #         "M20" REAL, "5dt_M20" REAL, "bolling Bandwith 10MA" REAL)')
        
#         df.loc[:,'indexCompare']=df.index.values
        df = df.rename_axis('indexCompare').reset_index()
#         df.loc[:,'indexCompare'] = df.reset_index(level)
        df.loc[:,'indexCompare']=df['indexCompare']-df['indexCompare'].shift(1)
        
        temp_list=[]
        for i , row in  df.iterrows():
    #             print(row['indexCompare'])
            if(pd.isnull(row['indexCompare']) or int(row['indexCompare']) != 1):
                if(len(temp_list)>0):
    #                     print(str(temp_list[0])+","+str(temp_list[-1]))
                    sql_df=df.loc[temp_list[0]:temp_list[-1],['Date','M20','5dt_M20','bolling Bandwith 10MA']]
    #                     print(sql_df.head())
                    Y=sql_df.loc[temp_list[0],['Date']]
    #                     print(str(Y.values[0]))
                    cursor.execute('CREATE TABLE IF NOT EXISTS "'+
                    str(Y.values[0])+
                    '"(Date Text PRIMARY KEY NOT NULL,\
                    "M20" REAL, "5dt_M20" REAL, "bolling Bandwith 10MA" REAL)')
                    cursor.executemany('insert or ignore into "'+
                    str(Y.values[0])+
                    '" ("Date","M20","5dt_M20","bolling Bandwith 10MA")\
                    values(?,?,?,?)', sql_df.values.tolist())
                    cnx.commit()
                    temp_list.clear()
                    temp_list.append(i)
                else:
                    temp_list.append(i)
                
            # 透過index - index shift 為一的特性，append連續資料，除了連續資料的第一個
            elif int(row['indexCompare']) == 1:
                    temp_list.append(i)
        if(len(temp_list)>0):
            sql_df=df.loc[temp_list[0]:temp_list[-1],['Date','M20','5dt_M20','bolling Bandwith 10MA']]
            print(sql_df.head())
            Y=sql_df.loc[temp_list[0],['Date']]
            print(str(Y.values[0]))
            cursor.execute('CREATE TABLE IF NOT EXISTS "'+
            str(Y.values[0])+
            '"(Date Text PRIMARY KEY NOT NULL,\
            "M20" REAL, "5dt_M20" REAL, "bolling Bandwith 10MA" REAL)')
            cursor.executemany('insert or ignore into "'+
            str(Y.values[0])+
            '" ("Date","M20","5dt_M20","bolling Bandwith 10MA")\
            values(?,?,?,?)', sql_df.values.tolist())
            temp_list.clear()
            # 需執行commit
            
        
        # Execute the DROP Table SQL statement
        
        dropTableStatement = 'drop table if exists "TimeStamp";'

        cursor.execute(dropTableStatement)
#         update /insert lasttimestamp
        cursor.execute('CREATE TABLE "TimeStamp" (Date datetime);')
        # print("last timestamp="+timestamp.values[0])
        cursor.execute('insert into "TimeStamp"  ("Date") values("'+timestamp+'");')
        
    except Exception as E:
        print('Error :', E)
    




def filter_V1(dir,stockid):
    sqlite_name=dir+str(stockid)+".sqlite"
    db_handler = sqlite3.connect(sqlite_name)
    TKsqlite_name=dir+"TK"+str(stockid)+".sqlite"
    #連結TK sqlite資料庫
    #     read timestamp
    cnx = sqlite3.connect(TKsqlite_name)
    c = cnx.cursor()
    sub_query=''
    c.execute('SELECT count(name) FROM sqlite_master WHERE type="table" AND name="TimeStamp"')
    if c.fetchone()[0]==1 : 
        _last = pd.read_sql_query('SELECT Date  FROM  TimeStamp ;', cnx)
        _last_size=_last.shape[0]
#        print(_last.iloc[0,0])
        
        if _last_size==1:
            Q=_last.iloc[0,0]
            sub_query='where Date >="'+str(Q)+'"'
            
    # print(sub_query)
    # cursor = db_handler.cursor()
    # cursor.execute('SELECT Date ,Close FROM  OHLC order by Date desc')
    # cursor.fetchall()
    df = pd.read_sql_query('SELECT Date ,Close,Volume FROM  OHLC '+sub_query+' order by Date asc', db_handler)
    A=ka.bollinger_bands(df,bis=2)
    #     A.to_csv(dir+str(stockid)+'Bolling.csv', mode='a', header=True,index=False)
    SRC = pd.read_sql_query('SELECT * FROM  SRC '+sub_query+' order by Date asc', db_handler)
#     A.to_csv('Atest.csv', mode='w', header=True,index=False)
    SRC.loc[:,'5dt_M20']=(SRC['M20']-SRC['M20'].shift(5))/5
    SRC.loc[:,'M20_shift10']=SRC['M20'].shift(10)
    SRC.loc[:,'10dt_M20']=(SRC['M20']-SRC['M20_shift10'])/10
    
    
    
#    write  timestamp
    last_SRC_timestamp=SRC.iloc[-29,:]

#     print("last timestamp="+last_SRC_timestamp['Date'])

    result = pd.merge(SRC, A, how='inner', on='Date')
    # 篩選條件
    # SRC介於0~5%
    # SRC 5日斜率>=0.01
    # bolling 通道<=0.12
    cond1=result['M20']<0.05
    cond2=result['M20']>=0.0
    cond3=result['5dt_M20']>=0.01
    cond4=result['bolling Bandwith 10MA']<=0.2
    A=result.loc[cond2 & cond1 & cond3 & cond4]

    db_handler.close()

    
#     result.to_csv('test.csv', mode='w', header=True,index=False)
    save_trace_table(cnx,A,last_SRC_timestamp['Date'])
    cnx.commit()
    cnx.close()



        




    
        

        
def print_trace_tabel_recently(dir,stockid):
    now = datetime.now()
    Today = now.strftime("%Y-%m-%d")
#     print(Today)
    
    current_date = (now-timedelta(days=120)).isoformat()
    price_db_name=dir+str(stockid)+".sqlite"
    filename="TK"+str(stockid)+".sqlite"
    filepath=dir+filename
    if os.path.isfile(filepath) is not True:
        return
    price_db_handler = sqlite3.connect(price_db_name)
#    =sqlite3.connect(price_db_name)
    db_handler = sqlite3.connect(filepath)
    db_handler.create_function("REGEXP", 2, regexp)
    df = pd.read_sql_query('SELECT name FROM sqlite_master WHERE type ="table" And name REGEXP "^[0-9\-]+$" ORDER BY name ASC',db_handler)
    
#     cond0=df['Date']<= start
    cond1=df['name']>= current_date
    A=df.loc[cond1]
    
    rows_length=A.shape[0]
    if rows_length>0:
        print(str(stockid))
        print('Date ',' Price ')
        for i, v  in A.iterrows(): 
            Pr=pd.read_sql_query('select Close from OHLC  where date == "'+v['name']+'"',price_db_handler)
            print(v['name'],str(Pr['Close'][0]))
#         print(A)
    db_handler.close()
    price_db_handler.close()




    

#    VACUUM(stockid)

target = r'./older'
# local_dir={}
def local_folder(dir_target="./"):
# 歷遍當前目錄下的資料夾名稱 符合:2020-01-01此格式
    directory_list = list()
#     print(dir_target)
    for root, dirs, files in os.walk(dir_target):

        # if  target!=None and target in dirs:
        for name in dirs:
            
            combine_name=str(root)+str(name)
            if dir_target!="./":
                combine_name=str(name)
            # print(root)
            if root != dir_target:
                continue
            
            x = re.search(r"\d{4}-\d{2}-\d{2}", combine_name)
            if x is not None:
                directory_list.append(os.path.join(root, name))

    return directory_list



def main():
    input=sys.argv[1:]
    sample='2020'
    
    dirp='small_test'
    dir=dirp+'/'
    _start=date(2016,1,1)
    _end=date(2019,12,31)
    start = _start.isoformat()
    end=_end.isoformat()
    print_src=False
    stockid=2546
  
 
 # host_directory_list=> Search local host YYYY-MM-DD/xx.gz directory  avoid downlaoding from google drive

    
    DD=local_folder()
    DD+=local_folder(target)
    host_directory_list= dict()
    for item in DD:
        matchObj = re.search(r"\d{4}-\d{2}-\d{2}", item)
        # print(item)
        if matchObj:
            # print ("matchObj.group() : ", matchObj.group())
            host_directory_list[matchObj.group()] = item
    

    today = date.today()

    d1 = today.strftime("%Y-%m-%d")
    A=SRCtool.get_sqlite_local_file(dirp)
    # file_data = (str(row[0]),str(row[1]))
    file_df = pd.DataFrame(A, columns=['name','date'])
    
    file_df.sort_values(by="date",ascending=True,inplace=True)
    data = pd.read_csv(dir+"B.csv", delimiter=',')
    data['stockid'] = data['stockid'].astype("str")
    
    for row in data.itertuples():
        item=getattr(row, 'stockid')
        G1=file_df.query('name == "'+str(item)+'"')
#        print(len(G1.index))
        if len(G1.index) ==0:
#            print(item)
            file_df=file_df.append({'name': item,'date':d1}, ignore_index=True)
    if '-p' in input:
	    print_src=True
    
    print(data.dtypes)
    lst =search_tool.get_stockid('股票','上市認購(售)權證')
    isin_df = pd.DataFrame(lst,columns =['stockid'])
#    print(isin_df.head())
    print(isin_df.dtypes)
    #for col in isin_df.columns:
    #    print(col)
    isin_df['num_stockid']=isin_df['stockid'].str.extract(r"^([^\s]*)(?=\s)")
    #print(isin_df)  
    file_df['name'] = file_df['name'].astype("str")
    isin_df['num_stockid']=isin_df['num_stockid'].astype("str")
    inner_df = file_df.merge(isin_df,  left_on='name', right_on='num_stockid', how='inner', indicator=True)
#    Linner_df = data.merge(isin_df,  left_on='stockid', right_on='num_stockid', how='left', indicator=True)
    print(inner_df.shape[0])
#    print(file_df.shape[0])
    for row in inner_df.itertuples():

        item=getattr(row, 'name')
        print(item)
#    for index,_item in file_df.iterrows():
#        item=int(_item[0])
        if print_src:
            print_trace_tabel_recently(dir=dir,stockid=item)
        else:
            print(item)
            try:
                SRCtool.auto_download(dir=dir,stockid=item,host_dir=host_directory_list)
                
                if(check_file_exist(dir=dir,stockid=item)):
                    SRC_tablename=SRCtool.caculate_SRC_sqlit_version(dir=dir,stockid=item,period=20,force=False)
                    SRCtool.Reduce_SRC_data(dir=dir,stockid=item,SRC_tablename=SRC_tablename)
                    SRC_tablename=SRCtool.caculate_SRC_sqlit_version_with_size(dir=dir,stockid=item,period=10,force=False)
                    SRCtool.Reduce_SRC_data(dir=dir,stockid=item,SRC_tablename=SRC_tablename)
    #                caculate_SRC_sqlit_version_with_size(dir=dir,stockid=item,period=20,c_size=30,force=False)
    #                output2csv(dir=dir,stockid=item)
    #                filter_V1(dir=dir,stockid=item)
    #                update_trace_tabel(dir=dir,stockid=item)
    #                print_trace_tabel_recently(dir=dir,stockid=item)
    #            break
                # Cleaning_data OHLC ,SRC , 
                # select Date from OHLC WHERE Date >= '2020-01-01' order by date ASC
                # dropTableStatement = 'drop table if exists '+SRC_table_name
                # cursor.execute(dropTableStatement)
                
        
    #             R=SRC_green_red_check(dir=dir,stockid=item)
            #     # R['stockid']=item
            #     R.insert (0, "stockid", item)
            #     if index==0:
            #         tmp=price_change(dir=dir,stockid=item,df=R)
                    
            #         tmp.to_csv('my_csv.csv', mode='a', header=True,index=False)
            #     else:
            #         tmp=price_change(dir=dir,stockid=item,df=R)
            #         # A = pd.concat([A,tmp],sort=False)
            #         tmp.to_csv('my_csv.csv', mode='a', header=False,index=False)
        
        # print(A)
    
        # current_date = (_start+timedelta(days=10)).isoformat()
        # Top15(stockid)
        # TOP15_MA5_Ratio(stockid=stockid)
        # get_stock_SRC_(stockid=stockid,
        # start=str(start), 
        # current_date=str(current_date),period_vol=30,date_source='sqlite')

            except Exception as E:
                print('Error : ', E)






if __name__ == '__main__':
    main()


# In[ ]:




