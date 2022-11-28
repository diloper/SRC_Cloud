# -*- coding: utf-8 -*-
"""random_test.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/12MF5hMHlrJ3PUvo1gECkO7ZzBGrUrMNa
"""

# %load random_test.py
#!/usr/bin/env python

# %load random_test.py

import re , os , sqlite3
import pandas as pd
# import read_file_analyze as ka
from datetime import date, timedelta, datetime

RunningInCOLAB = 'google.colab' in str(get_ipython()) if hasattr(__builtins__,'__IPYTHON__') else False
print(RunningInCOLAB)
if RunningInCOLAB:
  # Load the Drive helper and mount
  from google.colab import drive
  # This will prompt for authorization.
  dir='./small_test/'
  drive.mount('/content/drive')

  os.chdir('/content/drive/MyDrive/RD_程式開發/SRC_deploy') 
  os.listdir() #確認目錄內容


def regexp(expr, item):
    reg = re.compile(expr)
    return reg.search(item) is not None

def find_SRC_negative_v2(dir,stockid,boundary=300):
#     now = datetime.now()
#     Today = now.strftime("%Y-%m-%d")
#     print(Today)
#     current_date = (now-timedelta(days=120)).isoformat()
    price_db_name=dir+str(stockid)+".sqlite"
    if os.path.isfile(price_db_name) is not True:
        return
    price_db_handler = sqlite3.connect(price_db_name)
    df = pd.read_sql_query('SELECT * from (SELECT OHLC.Date,OHLC.Close, SRC.M20 from SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date desc LIMIT "'+str(boundary)+'") order by date ASC',price_db_handler)
    df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    price_db_handler.close()

    df['SRC M20 5dt']=(df['M20']-df['M20'].shift(5))/5
#   抓取SRC 5dt連續性
    df['SRC 5dt_G_R']=df['SRC M20 5dt']/abs(df['SRC M20 5dt'])
    df['5dt_G_R_sum']=df['SRC 5dt_G_R'].rolling(window=10,min_periods=10).sum()
#     df.loc[df['M20'] <0&, 'name_match'] = 'Match'
    date_before = date.today() - timedelta(days=boundary)
#     print(date_before)
#    df.loc[(df['M20']<0) & (df['M20']>=-0.1)& (df['5dt_G_R_sum']>8) & (df['Date'] > str(date_before)),'RRR']=1
# 可調整 SRC 條件
    A=df.loc[(df['M20']<0) & (df['M20']>=-0.1)& (df['5dt_G_R_sum']>8) & (df['Date'] > str(date_before)) ]
#    A=df.loc[df['RRR']==1]
    
    
#     if A.shape[0]>0:
#         print(A.head())
        
#         print(A.head())
#         A.to_csv(dir+str(stockid)+'SRC_dt.csv', index=False)
    return A

def find_SRC_negative(dir,stockid,condition=True):
#     now = datetime.now()
#     Today = now.strftime("%Y-%m-%d")
#     print(Today)
#     current_date = (now-timedelta(days=120)).isoformat()
    price_db_name=dir+str(stockid)+".sqlite"
    if os.path.isfile(price_db_name) is not True:
        return
    price_db_handler = sqlite3.connect(price_db_name)
    df = pd.read_sql_query('SELECT OHLC.Date,OHLC.Close, SRC.M20 from SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date ASC',price_db_handler)
    df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    price_db_handler.close()
    count_list=[]
    date_st_list=[]
    prc_st_list=[]
    prc_st_20MA_list=[]
    date_ed_list=[]
    prc_ed_list=[]
    prc_ed_20MA_list=[]
    diff_20MA_list=[]
    stockid_list=[]
    one_M_price_list=[]
#         
    df['G_R_Cond']=df['M20']/abs(df['M20'])#定義正為1 負為-1
    df['G_R_Cond'] = df['G_R_Cond'].fillna(0)
    df['SRC M20 5dt']=(df['M20']-df['M20'].shift(5))/5
#   抓取SRC 5dt連續性
    df['SRC 5dt_G_R']=df['SRC M20 5dt']/abs(df['SRC M20 5dt'])
    df['5dt_G_R_sum']=df['SRC 5dt_G_R'].rolling(window=10,min_periods=10).sum()
#     df.loc[df['M20'] <0&, 'name_match'] = 'Match'
    date_before = date.today() - timedelta(days=20)
#     print(date_before)
    df.loc[(df['M20']<0) & (df['5dt_G_R_sum']>8) & (df['Date'] > str(date_before))& (df['M20']>=-0.1),'RRR']=1
    A=df.loc[df['RRR']==1]
    if A.shape[0]>0:
#         print(A.head())
        A.to_csv(dir+str(stockid)+'SRC_dt.csv', index=False)
    count_row = df.shape[0]
    compare1=2
    compare2=-1
    if condition==False:
        compare1=-2
        compare2=1
    a=1
    while(a<count_row):
#     for index in range(0,count_row-1):
#         a=index+1
        index=a-1
        B=int(df.loc[index,['G_R_Cond']].values[0])
        C=int(df.loc[a,['G_R_Cond']].values[0])
#         print('loop pre',a)
#        B-C=> 1-(-1)=2表示SRC開始由正轉負，反之 由負轉正
        
        re=B-C
        if  re==compare1 :
    #         print(index,a)
            
            count=1
            while(1):
                a=a+1
                if a < count_row and int(df.loc[a,['G_R_Cond']].values[0])  == compare2:
                    count=count+1
                else:
                    if a >= count_row:
                        a=count_row-1
#                     print(a)
                    break
            if count>20: # SRC<0 連續20次
                
                date_st=df.loc[index,['Date']].values[0]
                prc_st_20MA=df.loc[index,['20 Day MA']].values[0]
                prc_st=df.loc[index,['Close']].values[0]
#                 date_ed=date_ed=df.loc[a,['Date']].values[0]
                prc_ed_20MA=df.loc[a,['20 Day MA']].values[0]
                prc_ed=df.loc[a,['Close']].values[0]
                diff_20MA=(prc_ed_20MA-prc_st_20MA)/prc_ed_20MA
                date_ed=df.loc[a,['Date']].values[0]
                
                
                Next_M=a+30
                if a+30>= count_row:# 取得{D+30  or 最新的}price
                    Next_M=count_row-1
#                 one_M_price=df.loc[Next_M,['20 Day MA']].values[0]
                if diff_20MA < 0: # 20MA均線呈現下降
                    count_list.append(count)
                    date_st_list.append(date_st)
                    prc_st_list.append(prc_st)
                    prc_st_20MA_list.append("{:.2f}".format(prc_st_20MA))
                    
                    date_ed_list.append(date_ed)
                    prc_ed_list.append(prc_ed)
                    prc_ed_20MA_list.append("{:.2f}".format(prc_ed_20MA))
                    
                    diff_20MA_list.append("{:.4f}".format(diff_20MA))
                    stockid_list.append(str(stockid))
                    
                    one_M_price=df.loc[Next_M,['20 Day MA']].values[0]
                    one_M_price_list.append(one_M_price)
                    
                    
#                     A=df.loc[index:a,['Close diff 20 Day MA']]
#                     print(A,len(A))
#                     print(count,","
#                       ,date_st,",","{:.2f}".format(prc_st_20MA)
#                       ,",",date_ed,",","{:.2f}".format(prc_ed_20MA)
#                       ,",","{:.4f}".format(diff_20MA)
#                       ,",","{:.2f}".format(one_M_price),",",str(stockid))
                        
#                   Using lists in dictionary to create dataframe

        a=a+1
#     df['Close diff 20 Day MA']=(df['20 Day MA']-df['Close'])/df['Close']
#     df.to_csv('Close diff 20 Day MA.csv', index=False)
    dicty = {'count_list':count_list
             ,'date_st_list': date_st_list,'prc_st_list': prc_st_list,'prc_st_20MA_list': prc_st_20MA_list
             ,'date_ed_list': date_ed_list,'prc_ed_list': prc_ed_list,'prc_ed_20MA_list': prc_ed_20MA_list
             ,'diff_20MA_list': diff_20MA_list,'one_M_price_list':one_M_price_list,'stockid_list':stockid_list,} 
    rf = pd.DataFrame(dicty) 
    return rf



def find_SRC_back_positive(df,str_index,boundary=90,continue_times=20,find_positive=True):
    # str_index=307
    end_index=str_index-boundary+2

    if end_index <= 0:
#         print(str_index)
#         print(end_index)
#         print(df.shape[0])
        return None,None
    compare1=-2
    compare2=1
    if find_positive is not True:
        compare1=2
        compare2=-1
#第一層while是要找出斜率  開始由正轉負，反之 由負轉正
    while(str_index > end_index):
        str_index=str_index-1
        index=str_index-1
        B=int(df.loc[str_index,['G_R_Cond']].values[0])
        C=int(df.loc[index,['G_R_Cond']].values[0])
    # print('loop pre',str_index)
    # B-C=> 1-(-1)=2表示SRC開始由正轉負，反之 由負轉正 修改"find_positive"
        diff=B-C
        if  diff==compare1 :
    #         print(index,str_index)
    #         print(df.loc[str_index,['Date']].values[0])
            count=1
#第二層while檢測SRC斜率的連續性，如果要容許不連續要再做修改
            while(str_index >= end_index):
                str_index=str_index-1
                if str_index > end_index and int(df.loc[str_index,['G_R_Cond']].values[0])  == compare2:
                    count=count+1
                else:
#                     if str_index >= end_index:
    #                     str_index=count_row-1
#                         print(str_index,count)
    #                     print(count)
                    break
            if count > continue_times:
#                 print(index)
#                 print(df.loc[index,['Date']].values[0])
                Date=df.loc[index,['Date']].values[0]
#                 print(Date)
                return Date,index
    return None,None
#                 print(df.loc[str_index+count,['Date']].values[0])
    #         else:
    #             print(count)


# In[68]:
def query_(dir,stockid,boundary,date):
    price_db_name=dir+str(stockid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    price_db_handler = sqlite3.connect(price_db_name)
    tat='SELECT * FROM(SELECT OHLC.Date,OHLC.Close,SRC.M20 from  SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date DESC )WHERE Date <="'+ str(date) +'"order by Date desc  LIMIT "'+str(boundary)+'"'
    # df = pd.read_sql_query('SELECT OHLC.Date,OHLC.Close, SRC.M20 from SRC INNER JOIN OHLC ON SRC.Date = OHLC.Date order by SRC.Date ASC',price_db_handler)
    # df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    df = pd.read_sql_query(tat,price_db_handler)
    price_db_handler.close()
    
    df=df.iloc[::-1]
    df.reset_index(drop=True,inplace=True)
    df['G_R_Cond']=df['M20']/abs(df['M20'])#定義正為1 負為-1
    df['G_R_Cond'] = df['G_R_Cond'].fillna(0)
    # priec M20 slope 
    df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    df['20 Day MA']=df['20 Day MA'].shift(1)
    df.loc[:,'20 Day MA_5dt']=((df['20 Day MA']-df['20 Day MA'].shift(5))/df['20 Day MA'].shift(5)).round(4)
    return df
# 分析SRC 於負的區間的時間與 股價變化
# Step 1透過find_SRC_negative 先找出 SRC 斜率 上升 且即將由負轉正
# Step 2 針對各別時間點，再往前搜尋最後一次SRC由正轉負的

def find_SRC_negative_area(dir,stockid,boundary=600):
    df=find_SRC_negative_v2(dir=dir,stockid=stockid,boundary=boundary)
    # df
#     df.loc[:,'indexCompare']=df.index.values

#     df.loc[:,'indexCompare']=df['indexCompare']-df['indexCompare'].shift(1)

#     drop_list=[]
#     for i , row in  df.iterrows():
#         if(pd.isnull(row['indexCompare'])):
#             continue
#        # 透過index - index shift 為一的特性，append連續資料，除了連續資料的第一個e.g. 5=鄰近的五天 
#         if int(row['indexCompare']) <5:
#             drop_list.append(i)

#     # drop 連續的資料
#     dfr=df.drop(drop_list)
#     del dfr['indexCompare

#     print(df)
    dfr=continue_remove(df,5)
#     print('-----')
#     print(dfr)

    # SRC 負值開始可回朔天數須大於90天，需要夠長的區間進行分析

    dfr.loc[:,'indexCompare']=dfr.index.values


     # print(df.tail())
#     df.to_csv('stockid'+str(stockid)+'.csv', index=False)
#     df[]
    target=dfr[['indexCompare', 'Date']]
#     print(target)
    dfG = pd.DataFrame(columns=['str_date','str_index',
                            'end_date','end_index','during'])
    
    boundary=90
    
    
    for row in target.itertuples():
        end_index=getattr(row, 'indexCompare')
        dateA=getattr(row, 'Date')
        df=query_(dir=dir,stockid=stockid,boundary=boundary,date=dateA)
        
        index_A=df.loc[df['Date']==dateA]
        # print(index_A.index.values)
        end_index=index_A.index.values[0]
        Date,str_ind=find_SRC_back_positive(df=df,str_index=end_index,boundary=boundary)
#         print(Date)
#         分析區間內的股價變化
        if Date is not None:       
#             print(str_ind,getattr(row, 'Date'))
#             print(str_ind,end_index)
            dfG = dfG.append({'str_date': Date,'str_index':str_ind,'end_date':getattr(row, 'Date'),'end_index':end_index,'during':end_index-str_ind}, ignore_index=True)
#     print(dfG)
    return dfG,df
    

def continue_remove(df,diff=3):
    df.loc[:,'indexCompare']=df.index.values

    df.loc[:,'indexCompare']=df['indexCompare']-df['indexCompare'].shift(1)

    drop_list=[]
    for i , row in  df.iterrows():
        if(pd.isnull(row['indexCompare'])):
            continue
       # 透過index - index shift 為一的特性，remove  相差小於diff的資料。除了連續資料的第一個 
        if int(row['indexCompare']) <diff:
            drop_list.append(i)

    # drop 連續的資料
    dfr=df.drop(drop_list)
    del dfr['indexCompare']
    return dfr



def show_period_MA20_status(dfG,df):
    for row in dfG.itertuples():
        str_index=getattr(row, 'str_index')
        end_index=getattr(row, 'end_index')
    #         print(getattr(row, 'str_date'))
        A=df.iloc[str_index:end_index]

    #  找出斜率|x|<=0.002 的發生點
    #         PR=A[A['20 Day MA_5dt']<=0.002 & A['20 Day MA_5dt']>=-0.002]
        cond1=A['20 Day MA_5dt']<=0.002
        cond2=A['20 Day MA_5dt']>=-0.002
    #         PR=A[A['20 Day MA_5dt']<=0.002]

        PR=A.loc[cond2 & cond1 ]
        head=A.iloc[0:1]
        tail=A.tail(1)
    #     data = []
    #     data.insert(0, head)
    # 檢查是否已存在，插入頭尾兩筆資料
        PR=pd.concat([head, PR], ignore_index=False)
        PR=pd.concat([PR,tail], ignore_index=False)
        # dropping duplicate values 
        PR.drop_duplicates(keep='first',inplace=True) 
        PR=continue_remove(PR)
        print(PR)

        # 依照區間進行分析曲線 升 降

        arr=PR[:].index.values
        # arr
        for i in range(len(arr)-1):
            print(arr[i],arr[i+1])
            str_index=arr[i]
            end_index=arr[i+1]
            tmp=df.iloc[str_index:end_index+1]
#             print(tmp)
        #     統計 20 Day MA_5dt 正負的次數?
            cond1=tmp['20 Day MA_5dt']>0.0
            positive=tmp.loc[ cond1 ]

#             print('正:',positive.shape[0])
#             print('負:',tmp.shape[0]-positive.shape[0])    
#         print('----------------------')
# print(dfG)












def modification_date(filename):
    t = os.path.getmtime(filename)
    R=datetime.fromtimestamp(t)
    return R.strftime("%Y-%m-%d")    
def local_file(last_folder,Nodir=True):
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




def bollinger_bands(data,bis=2):
#     data=copy.deepcopy(df)
    data['std']=data['Close'].rolling(window=20,min_periods=20).std()
    # data['std']=data['std'].shift(1)
#     data['5 Day MA Vol']=data['Volume'].rolling(window=5,min_periods=5).mean()
#     data['5 Day MA Vol']=data['5 Day MA Vol'].shift(1)
#     temp=[]
#     temp=data['Volume']/data['5 Day MA Vol']
#     data['Vol > 5 Day MA Vol']=temp.where(temp>=3)
#     data['5 Day MA Vol'].loc[0] = data['5 Day MA Vol'1].loc[1]
#     data['5 Day MA Vol'].drop([len(data['5 Day MA Vol'])])

    data['20 Day MA']=data['Close'].rolling(window=20,min_periods=20).mean()
    # data['20 Day MA']=data['20 Day MA'].shift(1)
    data['Upper Band']=data['20 Day MA']+bis*data['std']
    data['Lower Band']=data['20 Day MA']-bis*data['std']
    #布林帶寬
    data['bolling Bandwith']=(data['Upper Band']-data['Lower Band'])/data['20 Day MA']
#     data['bolling Bandwith 10MA']=data['bolling Bandwith'].rolling(window=10,min_periods=10).mean()
#     data['bolling Bandwith 5MA']=data['bolling Bandwith'].rolling(window=5,min_periods=5).mean()
    #10 布林帶寬變化
#     data['10 bolling Band std']=data['bolling Bandwith'].rolling(window=10,min_periods=10).std()
    #保留小數點後第二位
    data=data.round({'bolling Bandwith':2,'10 bolling Band std':2})
    
    #data["bolling Bandwith < 5%"]=data['bolling Bandwith'].where(data['bolling Bandwith']<0.05)

    return data


# In[ ]:


def forecast_Evaluate(dir,stockid,end_date,forecast_day=60):
    # search stock for future x day find max/min price 
    price_db_name=dir+str(stockid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    price_db_handler = sqlite3.connect(price_db_name)
    df = pd.read_sql_query('SELECT Date,Close from OHLC where Date >="'+str(end_date)+'" order by Date ASC LIMIT "'+str(forecast_day)+'"',price_db_handler)
    # df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    if df.shape[0]<60:
        forecast_day=df.shape[0]
    # end_date
    # retrive max/min price  close data  forecast_day
    # df
    column = df['Close']
    max_index = column.idxmax()

    min_index=column.idxmin()
    # compare end date close price with max min
    Hdiff=(column[max_index]-column[0])/column[0]
    Ldiff=(column[min_index]-column[0])/column[0]
    result=False
    G=0.11
    H=0.2
    # 跌幅<G 漲幅>H
    diff=Ldiff
    if abs(Ldiff)<G and Hdiff>H:
#     if Hdiff>H:
        result=df.at[max_index,'Date']
        diff=Hdiff
    else:
        result=df.at[min_index,'Date']
    return result,diff,column[0]

def break_bolling_low(dir,socketid,str_date,end_date):
#     forecast_day=120
#     socketid=2301
#     str_date='2020-01-10'
#     end_date='2020-04-01'
    # search stock for future x day find max/min price 
    price_db_name=dir+str(socketid)+".sqlite"
    # if os.path.isfile(price_db_name) is not True:
    #     return
    price_db_handler = sqlite3.connect(price_db_name)
    dfb = pd.read_sql_query('SELECT Date,Close from OHLC where Date <"'+str(str_date)+'" order by Date DESC LIMIT 19',price_db_handler)
    dfb.sort_values(by=['Date'], inplace=True)
    df1 = pd.read_sql_query('SELECT Date,Close from OHLC where  Date<="'+ str(end_date) +'"AND Date >="'+str(str_date)+'" order by Date ASC',price_db_handler)
    price_db_handler.close()
    
    df=pd.concat([dfb, df1])
    
    # df['20 Day MA']=df['Close'].rolling(window=20,min_periods=20).mean()
    bollinger_bands(df,bis=2)
#     print(df)
#     df.to_csv('out.csv', index=False) 
    # df.drop(['B', 'C'], axis=1)
    # df['']=
    F=df['bolling Bandwith'].tail(1).values[0]
    df.loc[df['Lower Band'] > df['Close'] , 'SING'] = 1 
    df=df.iloc[19:]
    df.reset_index(drop=True,inplace=True)
    # price_db_handler.close()
    if df.shape[0]<60:
        forecast_day=df.shape[0]
    # end_date
    # retrive max/min price  close data  forecast_day
    # df
#     column = df['Close']
#     max_index = column.idxmax()
#     min_index=column.idxmin()
#     # compare end date close price with max min
#     Hdiff=(column[max_index]-column[0])/column[0]
#     Ldiff=(column[min_index]-column[0])/column[0]
#     result=False
#     G=0.11
#     H=0.2
#     # 跌幅<G 漲幅>H
#     if abs(Ldiff)<G and Hdiff>H:
#         result=df.at[max_index,'Date']
#     print(socketid)
    #SING =1 Close price < Bolling Lower Band 
    condA=df['SING']==1
    # df
    E=df.loc[condA]
#     跌破布林通道底部占比
    P=0
    M20_direction=0
    if E.shape[0]>0:
        P=E.shape[0]/df.shape[0]
        A=E.at[E.head(1).index[0],'20 Day MA']
        B=E.at[E.tail(1).index[0],'20 Day MA']
        M20_direction = 1 if A>B else -1
    return P,E.shape[0],M20_direction,F


def SRC_notify(oldfilename,df):
    new_df=df.loc[: ,['socketid','end_date']]

    # 有資料才比較
    new_df_A=new_df
    if new_df.shape[0] > 0 :
        # check if data exist
        if os.path.isfile(oldfilename) :
            old_df=pd.read_csv(oldfilename)
            counter=old_df.shape[0] 
            for row in old_df.itertuples():
                end_date=getattr(row, 'end_date')
                socketid=getattr(row, 'socketid')
                

                condition = new_df_A['socketid'] == str(socketid)
                Q=new_df_A.loc[condition]

                index_A=Q.loc[Q['end_date']==end_date]
                if index_A.shape[0]==1:
                    new_df_A=new_df_A.drop(index_A.index.tolist())
                    counter=counter-1 
#            當counter=0 則表示old_df行數全比較完畢        
            if len(new_df_A) == 0 and counter== 0:
                print('the same')
                return False
        print('save file')
        df.to_csv(oldfilename, encoding='utf-8',index=False)
        return True





def find_SRC_by_condition(dir):
    #  排序dir下的檔案
    #print("F")
    A=local_file(dir)
    file_df = pd.DataFrame(A, columns=['name','date'])
    file_df.sort_values(by="name",ascending=True,inplace=True)
    stockid=2105


    # data = pd.read_csv(dir+"B.csv", delimiter=',')
    df1 = pd.DataFrame(columns=['count_list'
                                ,'date_st_list','prc_st_list','prc_st_20MA_list'
                                ,'date_ed_list','prc_ed_list','prc_ed_20MA_list'
                                ,'diff_20MA_list','one_M_price_list','stockid_list'])

    # pbar = ProgressBar()

    Evaluate = pd.DataFrame(columns=['socketid','str_date','str_index',
                                'end_date','end_index','during','forecast_Evaluate','end_close','price_diff','bolling Bandwith','Percentage_of_touch_bolling_bottom','times','M20_direction'])

    # loop 所有檔案
    for row in  file_df.itertuples():

        stockid=getattr(row, 'name')
        #print(stockid)
        try:

            dfG,df=find_SRC_negative_area(dir,stockid,boundary=100)
        #     if dfG.shape[0]>0:
        #         break
        # 如果有資料則會彙整
            for row in dfG.itertuples():
                Evaluate = Evaluate.append({'socketid':stockid,'str_date': getattr(row, 'str_date'),'str_index':getattr(row, 'str_index')
                                        ,'end_date':getattr(row, 'end_date'),'end_index':getattr(row, 'end_index')
                                        ,'during':getattr(row, 'during')}, ignore_index=True)
        #         dfG.to_csv(dir+str(stockid)+'SRC_dt.csv', index=False)
        #     dfx=find_SRC_negative(dir=dir,stockid=stockid,condition=True)
        #         pbar.update(5)
        #     df1=pd.concat([df1, dfx])
        except Exception as E:
            print('Error :', E)
            print(row)
    print(Evaluate)

    for row in Evaluate.itertuples():
    #     print(row.Index)
        end_date=getattr(row, 'end_date')
        str_date=getattr(row, 'str_date')
        socketid=getattr(row, 'socketid')
        # 
        A,price_diff,end_close=forecast_Evaluate(dir,socketid,end_date)
        P,E,G,J=break_bolling_low(dir,socketid,str_date,end_date)
        Evaluate.at[row.Index,'forecast_Evaluate']=A
        Evaluate.at[row.Index,'price_diff']=price_diff
        Evaluate.at[row.Index,'bolling Bandwith']=J
        Evaluate.at[row.Index,'end_close']=end_close
        Evaluate.at[row.Index,'Percentage_of_touch_bolling_bottom']=P
        Evaluate.at[row.Index,'times']=E
        Evaluate.at[row.Index,'M20_direction']=G


    cond2=Evaluate['times']>=0
    # now = datetime.now()
    now =date.today() - timedelta(days=60)
    Today = now.strftime("%Y-%m-%d")
    print(Today)
    cond3=Evaluate['end_date']>Today
    cond1=Evaluate['bolling Bandwith']<2
    cond4=Evaluate['end_close'] > 30
    PR=Evaluate.loc[cond2 & cond1&cond4]
    # print(PR.shape[0]/Evaluate.shape[0])
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    # pd.set_option('display.max_colwidth', None)
    # pd.set_option('display.max_colwidth', None)
    # G=PR[PR['price_diff']>0.2]
    # print(G.shape[0]/PR.shape[0])
    PR.sort_values(by=['end_date'],inplace=True)
    return PR.loc[: ,['socketid','end_date','end_close']]


    


# 
def financing_target(stockid,gap=0.06):
  # stockid='2344'

  _sql='select financing.date,financing.fin_cost,financing.fin_maintenance_rate,OHLC.Close from financing join OHLC on OHLC.date=financing.date '
  # _sql='select * from OHLC'
  G=readfromsql_v2(dir,str(stockid),sql=_sql)
  if G is None:
    return None
  #F_10 days moveing average
  G['F_10']=G['fin_maintenance_rate'].rolling(window=10,min_periods=10).mean()
  #
  G['F_cost_10']=G['fin_cost'].rolling(window=10,min_periods=10).mean()
  G['F_10_diff']=(G['F_10']-G['fin_maintenance_rate'])/G['fin_maintenance_rate']
  # today=
  G.loc[G['F_10_diff']>gap, 'SING'] = 1 
  target=G[G['SING']==1]

  # target['index1'] = target.index
  target = target.reset_index(level=0)
  target['index_shift']=target['index'].shift(1)
  target['index_diff']=target['index']-target['index_shift']
  target['index_diff']=target['index_diff'].shift(-1)
  # 計算是否連續發生(透過dataframe index)
  # target['index_diff'] = target['index_diff'].fillna(0)
  condA=target['index_diff'] == 0 #
  condB=target['index_diff'] > 1

  # 將最後一筆資料手動手入，並移除重複index，並確保區間forloop檢測點完整
  pagination=target[condB|condA]

  # pagination
  if pagination.shape[0] < 1:
    return

  pagination = pagination.append(target.head(1),ignore_index=True)
  pagination = pagination.append(target.tail(1),ignore_index=True)
  pagination.drop_duplicates(subset="index",keep='first', inplace=True)
  pagination.sort_values(by=['index'], inplace=True,ignore_index=True)
  # print(pagination)
  # 去除重複值，檢測點數量僅有一筆，則跳過
  if pagination.shape[0] == 1:
    return
  # range=pagination.copy()
  
  for i in range(len(pagination['index'])-1):
    # print(pagination['index'].at[i])
    condA=target['index']<int(pagination['index'].at[i+1])
    condB=target['index']>=int(pagination['index'].at[i])
    rangae=target[condA & condB]
    # print(rangae.tail(1)['Date'])
    # df.at[row_label, column_name] = 78
    # print(int(pagination['index'].at[i+1]))
    target.loc[target['index'] == int(pagination['index'].at[i+1]), 'm_time'] = rangae.shape[0]

  # pagination[pagination['index']==]
  
  # target
  if target[['index_diff']].iloc[1].values != 1.0:
    target.loc[0, 'm_time']=1
    # target[['m_time']].iloc[0]=1
    # print(target[['index_diff']].iloc[1])
  target=target[target['m_time']>0]
  return target
  # type(pagination['index'])
def filter_financing(duration=10,gap=0.06):
  today = date.today()
  # print("Today's date:", str(today))
  frome_date = today-timedelta(days=duration)
  # print('Date String', today.strftime('%Y-%m-%d'))
  today.strftime('%Y-%m-%d')
  frome_date.strftime('%Y-%m-%d')
  t = pd.DataFrame()
  A=local_file(dir)
  for stock_id in A:
    # print(stock_id[0])
    a=financing_target(str(stock_id[0]),gap)
    if a is None:
      continue
    a.drop(columns=['index_shift', 'SING','index','F_10','F_cost_10','F_10_diff','index_diff'],inplace=True)
    condA=a['Date'] >= frome_date.strftime('%Y-%m-%d')
    condB=a['Date'] <= today.strftime('%Y-%m-%d')
    condC=a['fin_maintenance_rate']>90
    condD=a['fin_maintenance_rate']<130
    result=a[condA & condB & condC &  condD]
    result=result.copy()
    result['stockid']=str(stock_id[0])
    # print(result)
    t=t.append(result, ignore_index=True)
  # result
  # t.sort_values(by=['Date'],inplace=True)
  t.sort_values(by=['fin_maintenance_rate'],inplace=True)
  return t

def readfromsql_v2(dir,stockid,dateformate=False,sql=None):

    price_db_name=dir+str(stockid)+".sqlite"
    if os.path.isfile(price_db_name) is not True:
        return
    db_handler = sqlite3.connect(price_db_name)
    cursor = db_handler.cursor()
    cursor.execute('SELECT name FROM sqlite_master WHERE type ="table" And name = "financing"')
    re = cursor.fetchall()
    if len(re)<1:
      return None
    df_mysql = pd.read_sql_query(sql, db_handler)

    db_handler.close()
    return df_mysql
def show_over_num_result(num=10):
  maxtime=10
  t = pd.DataFrame()
  gap=0.1
  while(1):

    result=filter_financing(duration=10,gap=gap)
    # result=result.copy()
    t=t.append(result, ignore_index=True)
    t.drop_duplicates(inplace=True)
    if t.shape[0] >=num:
      break
    else:
      gap=gap-0.01
    maxtime=maxtime-1
    if maxtime <1:
      break
  return t
import os
import filecmp
def diff_file(G,filename):
    diff_filename=filename+"_diff"
    diff_flag=None
    if os.path.exists(filename) is True:
        G.to_csv(diff_filename)
        diff_flag=filecmp.cmp(diff_filename,filename, shallow=False)
        if diff_flag is True:
            os.remove(diff_filename)
        else:
            G.to_csv(filename)
    else:
        G.to_csv(filename)
    return diff_flag

dir='./small_test/'
def main():
    oldfilename='SRCresult.csv'
    #A=find_SRC_by_condition(dir=dir)
    #B=SRC_notify(oldfilename=oldfilename,df=A)
#    C=SRC_notify(oldfilename=A,df=oldfilename)
    #print(A)
    a=show_over_num_result(num=30)
    print(a)
# In[94]:

if __name__ == '__main__':
    main()
