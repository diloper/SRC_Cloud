import pandas as pd
import copy
import numpy as np
# bis=標準差
def bollinger_bands(df,bis=2):
    data=copy.deepcopy(df)
    data['std']=data['Close'].rolling(window=20,min_periods=20).std()
    # data['std']=data['std'].shift(1)
    data['5 Day MA Vol']=data['Volume'].rolling(window=5,min_periods=5).mean()
    data['5 Day MA Vol']=data['5 Day MA Vol'].shift(1)
    temp=[]
    temp=data['Volume']/data['5 Day MA Vol']
    data['Vol > 5 Day MA Vol']=temp.where(temp>=3)
#     data['5 Day MA Vol'].loc[0] = data['5 Day MA Vol'1].loc[1]
#     data['5 Day MA Vol'].drop([len(data['5 Day MA Vol'])])

    data['20 Day MA']=data['Close'].rolling(window=20,min_periods=20).mean()
    # data['20 Day MA']=data['20 Day MA'].shift(1)
    data['Upper Band']=data['20 Day MA']+bis*data['std']
    data['Lower Band']=data['20 Day MA']-bis*data['std']
    #布林帶寬
    data['bolling Bandwith']=(data['Upper Band']-data['Lower Band'])/data['20 Day MA']
    data['bolling Bandwith 10MA']=data['bolling Bandwith'].rolling(window=10,min_periods=10).mean()
    data['bolling Bandwith 5MA']=data['bolling Bandwith'].rolling(window=5,min_periods=5).mean()
    #10 布林帶寬變化
    data['10 bolling Band std']=data['bolling Bandwith'].rolling(window=10,min_periods=10).std()
    #保留小數點後第二位
    data=data.round({'bolling Bandwith':2,'10 bolling Band std':2})
    
    #data["bolling Bandwith < 5%"]=data['bolling Bandwith'].where(data['bolling Bandwith']<0.05)

    # 5days 10days 波動計算 待優化算法
#     data["lnA"]=np.log(data['Close'])
#     data["lnA"]=data["lnA"][1:]
#     data["lnB"]=np.log(data['Close'])
#     data["lnB"]=data['lnB'].shift(1)
#     data["lnC"]=data["lnA"]-data["lnB"]
#     data = data.drop('lnA', 1)
#     data = data.drop('lnB', 1)
#     data["5 Day fluctuation"]=data["lnC"].rolling(window=5,min_periods=5).std()
#     data["20 Day fluctuation"]=data["lnC"].rolling(window=20,min_periods=20).std()
#     temp=data["5 Day fluctuation"]/data["20 Day fluctuation"]
#     data['fluctuation 5 Day  > 20 Day']=temp.where(temp>=1)
#     data = data.drop('lnC', 1)
    return data
def go_analyze_Trading_Price_over(stockid,data,Search_range,over=3):
        column_index=data.columns.get_loc("Close")
        get_last_data=len(data['Close'])
        if stockid=="6217.TW":
                print("")
        last_1=data.iloc[ get_last_data-Search_range-1 ,column_index]
        last_2=data.iloc[ get_last_data-1 ,column_index]
        if float((last_2-last_1)/last_1)>0.1:
                return 1
        else:
                return -1

def VOL_over_MA(stockid,data,Search_range,over=3):
         # 計算該天成交量是否大於平均5日3倍 (vol_geater>3)
       
        data['5 Day MA Vol']=data['Volume'].rolling(window=5,min_periods=5).mean()
        data['5 Day MA Vol']=data['5 Day MA Vol'].shift(1)
        temp=[]
        temp=data['Volume']/data['5 Day MA Vol']
        data['Vol > 5 Day MA Vol']=temp.where(temp>=over)
        get_last_data=len(data['Close'])
        
        column_index=data.columns.get_loc("Vol > 5 Day MA Vol")
        last=data.iloc[ get_last_data-Search_range:get_last_data ,column_index]
       
        for A in last:                      
                        if not pd.isnull(A):
                                return 1
        return -1
        # 3338 3/7~3/11 買入回檔的條件建立

def Ten_MA_match(stockid,item,Search_range):
        item['5 Day MA']=item['Close'].rolling(window=5,min_periods=5).mean()
        item['10 Day MA']=item['Close'].rolling(window=10,min_periods=10).mean()
        item['20 Day MA']=item['Close'].rolling(window=20,min_periods=20).mean()
        item['Close-10 Day MA']=(item['Close']-item['10 Day MA'])/item['10 Day MA']
        
        #60 Day MA 斜率
        item['60 Day MA']=item['Close'].rolling(window=60,min_periods=60).mean()
        item['Close-60 Day MA']=(item['Close']-item['60 Day MA'])/item['60 Day MA']
        item["60 Day MA older"]=item['60 Day MA'].shift(1)
        item['60 Day MA slope']=(item['60 Day MA']-item["60 Day MA older"])/item['60 Day MA']
        item['Close-10 Day MA']=(item['Close']-item['10 Day MA'])/item['10 Day MA']
        item['Close-10 Day MA']=(item['Close']-item['10 Day MA'])/item['10 Day MA']
        item = item.drop('60 Day MA older', 1)
        # 加入index 排序
        AH=[]
        AH= range(0, len(item) )
        item.insert(0, 'index', AH) 
        # 篩選條件
        cond1=item['Close-10 Day MA']<-0.001
        cond2=item['Close-10 Day MA']>-0.02
        cond3=item['Close-60 Day MA']<=0
        cond4=item['Close-60 Day MA']>-0.009
        A=item.loc[cond2 & cond1 & cond3 & cond4]
        result=[]
        get_last_data=len(item['Close'])
        watch=get_last_data-0#觀察天數
        watch2=get_last_data-5
        if len(A)<1:
                return 0
        for values in A['index']:
                # last2=item.iloc[ values ,column_index2]
                check2=item['Close'][values]
                check3=item['20 Day MA'][values]
                check4=item['60 Day MA'][values]
                date=item['Date'][values]
                # if values>watch and check2 > 12 and check3>check4:
                if values<watch and watch2<values and check2 > 10 and check2 < 30 and check3<check4 :
                        result.append(date)
        if len(result) > 0 :
                return result
        else:
                return 0

def fallenMA_match(stockid,item,Search_range):
        item['5 Day MA']=item['Close'].rolling(window=5,min_periods=5).mean()
        item['20 Day MA']=item['Close'].rolling(window=20,min_periods=20).mean()
        item['60 Day MA']=item['Close'].rolling(window=60,min_periods=60).mean()

        item['Close-60 Day MA']=(item['Close']-item['60 Day MA'])/item['60 Day MA']
        #漲幅
        item["60 Day MA older"]=item['60 Day MA'].shift(1)
        item['60 Day MA slope']=(item['60 Day MA']-item["60 Day MA older"])/item['60 Day MA']
        item = item.drop('60 Day MA older', 1)
         # 加入index 排序
        AH=[]
        AH= range(0, len(item) )
        item.insert(0, 'index', AH) 
        #5 、20 MA 交叉
        item['20 Day MA-5 Day MA']=(item['20 Day MA']-item['5 Day MA'])/item['5 Day MA']
        cond1=item['Close-60 Day MA']>0.001
        cond2=item['Close-60 Day MA']<0.02
        cond3=item['60 Day MA slope']>0.0005
        A=item.loc[(item['20 Day MA-5 Day MA'] > -0.01) & (item['20 Day MA-5 Day MA'] <0.01) & cond2 &cond1 & cond3]
        # column_index2=item.columns.get_loc("60 Day MA slope")
        result=[]
        get_last_data=len(item['Close'])
        watch=get_last_data-0#觀察天數
        watch2=get_last_data-20
        if len(A)<1:
                        return 0
        for values in A['index']:
                # last2=item.iloc[ values ,column_index2]
                check2=item['Close'][values]
                check3=item['Close-60 Day MA'][values-1]
                check4=item['Close-60 Day MA'][values]
                date=item['Date'][values]
                # if values>watch and check2 > 12 and check3>check4:
                if values<watch and watch2<values and check2 > 12 and check2 < 35 and check3>check4:
                        result.append(date)
        if len(result) > 5 :
                return result
        else:
                return 0
        # get_last_data=len(item['Close'])
        # watch=get_last_data-20
        # column_index=item.columns.get_loc("20 Day MA-5 Day MA")
        # last=item.iloc[ get_last_data-Search_range:get_last_data ,column_index]
        #漲幅
        # item["60 Day MA older"]=item['60 Day MA'].shift(1)
        # item['60 Day MA slope']=(item['60 Day MA']-item["60 Day MA older"])/item['60 Day MA']

def MA_match(stockid,item,Search_range):
        item['5 Day MA']=item['Close'].rolling(window=5,min_periods=5).mean()
        item['60 Day MA']=item['Close'].rolling(window=60,min_periods=60).mean()
        O=[]
        O=(item['60 Day MA']-item['5 Day MA'])/item['5 Day MA']
        #60 、20 MA 交叉
        fileer=0.13>O 
        fileer2=0.10<O
        item['60 Day MA-5 Day MA']=O.where(fileer2 & fileer)
        get_last_data=len(item['Close'])
        watch=get_last_data-10
        column_index=item.columns.get_loc("60 Day MA-5 Day MA")
        last=item.iloc[ get_last_data-Search_range:get_last_data ,column_index]
        #漲幅
        item["60 Day MA older"]=item['60 Day MA'].shift(1)
        item['60 Day MA slope']=(item['60 Day MA']-item["60 Day MA older"])/item['60 Day MA']
        item = item.drop('60 Day MA older', 1)
        #Close Value meet 60 MA
        item["60-Close"]=(item['60 Day MA']-item['Close'])/item['Close']
        # 加入index 排序
        AH=[]
        AH= range(0, len(item) )
        item.insert(0, 'index', AH)
        # item.insert(0, column='index', AH)
        A=item.loc[(item['60-Close'] > -0.01) & (item['60-Close'] <= 0.01)]
        column_index2=item.columns.get_loc("60 Day MA slope")
        #5天的成交量
      
        item['5 Day MA Vol']=item['Volume'].rolling(window=5,min_periods=5).mean()
        # if stockid=='1102.TW':
        #         print('')
        if len(A)<1:
                return 0
        for values in A['index']:
                # print(values)
                continues_counter=0
                last2=item.iloc[ values-Search_range:values ,column_index2]
                for slop in last2:
                        if slop >-0.001 or  pd.isnull(slop):
                                break
                        else :
                               continues_counter=continues_counter+1
        if(continues_counter==Search_range):
                check1=item['5 Day MA Vol'][values]
                check2=item['Close'][values]
                # if stockid=='00715L.TW':
                #         print('')
                # watch 當成交量500張 成交價>10 發生在近期5天內
                # if check1>500*1000 and check2>10 and watch < values:
                if check1>200*1000 and check2>10 and watch < values:
                        return item['Date'][values]
                else:
                        return 0
                # print(item['Date'][values])
                
        else:
                # print(stockid)
                return 0
                # print(k[1])
                # print(k['60-Close'])
        # for A in last: 
        #         if not pd.isnull(A):
        #                 # print(stockid)
        #                 for A in range(1,Search_range+1,1):
        #                         Q=float(last2[[get_last_data-A]])
        #                         if Q>0.0:
        #                                 return Q
        #                         else:
        #                                 return -1

        # return -1
        
def go_analyze_Trading_Price(target_val,target_val2,stockid,item,not_top_price):
        item['20 Day MA']=item['Close'].rolling(window=20,min_periods=20).mean()
        
        get_last_data=len(item['20 Day MA'])
        column_index=item.columns.get_loc("20 Day MA")
        last=item.iloc[ get_last_data-1 ,column_index]
        last2=item.iloc[ get_last_data-19 ,column_index]
        
        if (float(last)/float(last2))> 1.02:#出現高點
                return -1
        # last3=item.iloc[ get_last_data-41,column_index]
        # if last is not nan:
                # x = np.array([last,last2, last3])
        
        if last>target_val2 or target_val>last:
                return -1
        else:
                return 1

# 連續 Search_range天  成交量>target_val
def go_analyze_Trading_Volume(target_val,stockid,item,Search_range):
        get_last_data=len(item['Volume'])
        if(get_last_data<Search_range):
                return -1
        continues_counter=0
        column_index=item.columns.get_loc("Volume")
        last=item.iloc[ get_last_data-Search_range-1:get_last_data ,column_index]
        for A in range(1,Search_range+1,1):
                Q=int(last[[get_last_data-A]]) 
                if Q > target_val*1000: #1000股=一張
                        continues_counter=continues_counter+1
                else:
                        continues_counter=continues_counter-1
        #       print(last[[get_last_data-A]])
        if(continues_counter==Search_range):
                return 1
        else:
                # print(stockid)
                return 0
def go_analyze_bollinger(stock,data,Search_range,vol_ratio):
        item=bollinger_bands(data)
        get_last_data=len(item['Close'])
        # 最後五筆 布林通道 連續 5%
        # iloc[:,0:2]
        if(get_last_data<Search_range):
                return -1
        continues_counter=0
        column_index=item.columns.get_loc("bolling Bandwith")
        last=item.iloc[ get_last_data-Search_range-1:get_last_data ,column_index]
        for A in range(1,Search_range+1,1):
                Q=float(last[[get_last_data-A]]) 
                if Q < 0.05:
                        continues_counter=continues_counter+1
                else:
                        continues_counter=continues_counter-1
        #       print(last[[get_last_data-A]])
        if(continues_counter==Search_range):
                return 1
        else:
                return 0
        
# 設定mode 1=突破布林  2=突破後回檔至20MA 但 股價未低於突破布林
def go_analyze_bolling_over(data,Search_range,vol_ratio_a,vol_ratio_b,mode=2):
        item=bollinger_bands(data,1.2)
        item['Close Change']=(data['Close']-data['Close'].shift(1))/data['Close']
        # 計算是否有站上布林通道Upper  (bollinger_over>1)
        item['Close Over Upper Band'] =(item['Close'] -item['Lower Band'])/(item['Upper Band']-item['Lower Band'])
        item['Open Under Upper Band'] =(item['Open'] -item['Lower Band'])/(item['Upper Band']-item['Lower Band'])
        item['Over 5MAVOL'] =item['Volume']/item['5 Day MA Vol']

        item['Upper Band slope'] = (item['Upper Band']- item['Upper Band'].shift(1))/item['Upper Band'] 
        item['bolling Band 10MA'] = item['bolling Bandwith'].rolling(window=10,min_periods=10).mean()
        get_last_data=len(item['Close'])

        #bolling 觀察天數區間
        # bolling_watch=get_last_data-25
        bolling_watch2=get_last_data-Search_range
        #MA20 觀察天數區間
        if mode==2:
                item['20 Day MA']=item['Close'].rolling(window=20,min_periods=20).mean()
                item['Close-20 Day MA']=(item['Close']-item['20 Day MA'])/item['20 Day MA']
                MA20_watch=get_last_data-1
                MA20_watch2=get_last_data-3

        # 加入index 排序
        AH=[]
        AH= range(0, len(item) )
        item.insert(0, 'index', AH) 
        # 篩選條件 
        # 突破boling 通道
        cond0=item['Open Under Upper Band']<1    
        cond1=item['Close Over Upper Band']>0.8
        cond2=item['Close Over Upper Band']<=1.1
        # 布林Upper Band 斜率
        # cond4=item['Upper Band slope'] >=0.01
        # 布林帶寬 
        cond5=item['bolling Band 10MA']<=0.07

        # 觀察範圍
        cond3=item['index']>bolling_watch2
       
        #上漲
        # cond6=item['Close Change']<0

        # 成交量 >  vol_ratio
        cond7=item['Over 5MAVOL']>vol_ratio_a
        cond8=item['Over 5MAVOL']<vol_ratio_b
        # 使用條件進行過濾
        A=item.loc[cond0 &cond1 & cond2 & cond3  & cond5 &cond7 & cond8]

        # 股價回檔至20MA 曲線上
        # cond3=item['Close-20 Day MA']< 0.001
        # cond4=item['Close-20 Day MA'] > -0.001
        # B=item.loc[ cond3 & cond4]


       
        val=""
        if len(A)<1:
                return val
        
        if mode==3 :
                print(len(A))
                # condp=item['index']>bolling_watch2
                # G=A.loc(condp)
                return A['Date'].values.tolist()
        Q=A.tail(1) # 只專注最後一次突破布林通道那一筆
        for values in Q['index']:
               
                date=item['Date'][values]
                bolling_price=item['Close'][values]
                if mode==2:        
                        close_position=item['Close-20 Day MA'][MA20_watch]
                        close_position_price=item['Close'][MA20_watch]
                        # close_position (收盤價 與 20MA 的距離) 
                        # bolling_price (突破bolling 的收盤價) 
                        if  close_position < 0.01 and bolling_price < close_position_price :
                                 val.append(date)                               
                # if values>watch and check2 > 12 and check3>check4:
                else :
                        C=0
                        N=90
                        # 連續N天有通道壓縮
                        # decreasing-for-loops e.g.(6,0,-1)
                        for i in range (values-1,values-N,-1):
                                V_bolling=item['bolling Band 10MA'][i]
                                # bolling_price=item['Close'][i]
                                # print(bolling_price)
                                if V_bolling<=0.1:
                                        C=C+1
                                else:
                                        break       
                        
                        val=date+","+str(C)
                        # val.append(date+",C"+str(C))


        return val




def go_analyze(stock,data,Search_range,vol_ratio):
        item=bollinger_bands(data)
        # 計算是否有站上布林通道Upper  (bollinger_over>1)
        bollinger_over=[]
        bollinger_over =(item['Close'] -item['Lower Band'])/(item['Upper Band']-item['Lower Band'])
        item['Close Over Upper Band'] = bollinger_over.where(bollinger_over>1)

        # 計算該天成交量是否大於平均5日3倍 (vol_geater>3)
        vol_geater=[]
        vol_geater=item['Volume']/item['5 Day MA Vol']
        item['Over 5MAVOL'] = vol_geater.where(vol_geater>vol_ratio)
        # 計算  大於平均5日3倍 且 有站上布林通道Upper 
        buy=[]
        buy=item['Over 5MAVOL']*item['Close Over Upper Band']
        item['BUY']=buy.where(buy>1)#(數學運算>1)

        # item.to_csv("A.csv")#存檔
        
        val=[]
        index=0
        get_last_data=len(item['Close'])
        last=item.iloc[ get_last_data-Search_range-1:get_last_data ,-1 ]
        date=item.iloc[ get_last_data-Search_range-1:get_last_data ,0 ]
        for A in last:                      
                if not pd.isnull(A):
                        #  val+=str(index)+" "
                        #     print(date['1'])
                        buy_date=date[[get_last_data-Search_range-1+index]]
                        
                        val.append(buy_date)
                        # print (date[[get_last_data-20+index]])
                index=index+1
                        #     print (data.get_loc(index))
        
        # if len(val) > 1:
                # val+" buy"

        return val

# 3231.TW
def main():
        print("python main function")
        dir="./TW/"
        target=dir+str(3413)+".TW.csv"
        data = pd.read_csv(target) 

        # Search_range=85
        # vol_ratio=2
        mode=3
        result=go_analyze_bolling_over(data,Search_range=120,vol_ratio_a=1,vol_ratio_b=5,mode=mode)
        for i in range(len(result)):
                print(result[i])
        
        # MA_match(target,data,Search_range=Search_range)
        # G=go_analyze_bollinger(data,Search_range=Search_range,vol_ratio=vol_ratio)

if __name__ == '__main__':
        main()