import io,re,os
import pandas as pd
import upload_file
def local_files_v1(dir_target="./",reg=r"\d{4}\d{2}\d{2}"):
# 歷遍當前目錄下的資料夾名稱 符合:2020-01-01此格式
    directory_list = list()
#     print(dir_target)
    for root, dirs, files in os.walk(dir_target):

        # if  target!=None and target in dirs:
        for name in files:
            
            combine_name=str(root)+str(name)
            if dir_target!="./":
                combine_name=str(name)
            # print(root)
            if root != dir_target:
                continue
            x = re.search(reg, combine_name)
            if x is not None:
                directory_list.append(os.path.join(root, name))

    return directory_list
def local_folder(dir_target="./",format=None):
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

def get_stockid(head,tail,df_flag=False):
#     Plan A: requests sometimes fail
#     my_headers = {'cookie': create_cookies()
#     ,'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'}
    
#     url = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
#     res = requests.get(url,headers =my_headers)
#     plan B :download isin from google drive
    D_Handel=upload_file.Google_Driver_API()
    folder_id=D_Handel.search_folder(name='Stock db')
    file_id=D_Handel.search_file(name='isin.html',folder_id=folder_id)
    html=D_Handel.downloadFile(file_id=file_id,local_filepath="isin.html",IO=True)
    del D_Handel
    text_obj = html.decode('UTF-8')

    df = pd.read_html(text_obj)[0]

    # 設定column名稱
    df.columns = df.iloc[0]
    # 刪除第一行
    # df = df.iloc[2:]
    # 先移除row，再移除column，超過三個NaN則移除
    df.dropna(axis='columns',inplace=True)
    df.dropna(axis='index',inplace=True)
    # df.head()
    dfx=df.iloc[:,0:1]    
    dfx.reset_index(inplace=True)
    del dfx['index']
    # 股票
#     G=dfx.query('有價證券代號及名稱 == "上市認購(售)權證"')
#    print(dfx.head())
    G1=dfx.query('有價證券代號及名稱 == "'+head+'"')
    G2=dfx.query('有價證券代號及名稱 == "'+tail+'"')
#     index=G.index.values[0]
#     o=dfx.iloc[0:index,:]
    index1=G1.index.values[0]
    index2=G2.index.values[0]
    k=dfx.iloc[index1+1:index2,0]

    return k.tolist()
