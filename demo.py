#!/usr/bin/env python
# coding: utf-8

import sys

# import bz2
import pandas as pd
import io,re,os 
import requests, shutil, time, cv2
from bs4 import BeautifulSoup
import numpy as np
from keras.models import load_model
from preprocessBatch import preprocessing
from utilities import one_hot_decoding
import upload_file 
# from datetime import date as GG
import json
from datetime import date, timedelta, datetime

os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
allowedChars = 'ACDEFGHJKLNPQRTUVXYZ2346789';
CAPTCHA_IMG = "captcha.jpg"
PROCESSED_IMG = "preprocessing.jpg"
FOLDER = "captcha/"
CSV_FILE = "auto-train.csv"

print('model loading...')
model = load_model("twse_cnn_model.hdf5")
print('loading completed')


# In[2]:


def crawl_SRC(stockid):
    bsr_session = requests.Session()
    resp = bsr_session.get("https://bsr.twse.com.tw/bshtm/bsMenu.aspx", verify=False)
    soup = BeautifulSoup(resp.text, 'html.parser')
    img_tags = soup.select("#Panel_bshtm img")
#     print("img_tags len",len(img_tags))
    if len(img_tags)==0:
        return False,False
    src = img_tags[0].get('src')
    
    resp = bsr_session.get("https://bsr.twse.com.tw/bshtm/" + src, stream=True, verify=False)
    if resp.status_code == 200:
        with open(CAPTCHA_IMG, 'wb') as f:
            resp.raw.decode_content = True
            shutil.copyfileobj(resp.raw, f)
            
    preprocessing(CAPTCHA_IMG, PROCESSED_IMG)
    train_data = np.stack([np.array(cv2.imread(PROCESSED_IMG))/255.0])
    prediction = model.predict(train_data)
    
    predict_captcha = one_hot_decoding(prediction, allowedChars)

    payload = {}
    acceptable_input = ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION', 'RadioButton_Normal',
                        'TextBox_Stkno', 'CaptchaControl1', 'btnOK']

    inputs = soup.select("input")
    for elem in inputs:
        if elem.get("name") in acceptable_input:
            if elem.get("value") != None:
                payload[elem.get("name")] = elem.get("value")
            else:
                payload[elem.get("name")] = ""
                
    payload['TextBox_Stkno'] = str(stockid)
    payload['CaptchaControl1'] = predict_captcha
    re_flag=False
    resp = bsr_session.post("https://bsr.twse.com.tw/bshtm/bsMenu.aspx", data=payload, verify=False)
    if '驗證碼錯誤!' in resp.text:
        print('驗證碼錯誤, predict_captcha: ' + predict_captcha)
        return 0,False
    elif '驗證碼已逾期!' in resp.text:
        print('驗證碼已逾期, predict_captcha: ' + predict_captcha)
        return 0,False
    elif '查無資料' in resp.text:
        print(str(stockid),'查無資料')
        return True,False
    elif 'HyperLink_DownloadCSV' in resp.text:
        # add to label file",
#         i += 1
#         print("success, add " + str(i) + ".jpg with captcha: " + predict_captcha)
#         shutil.copyfile(CAPTCHA_IMG, FOLDER + str(i) + ".jpg")
#         with open(CSV_FILE,'a') as fd:
#             fd.write(predict_captcha + "\n")
#         print("completed")

        re_flag=True
        A=bsr_session.get("https://bsr.twse.com.tw/bshtm/bsContent.aspx?v=t", verify=False)
        soup = BeautifulSoup(A.text, 'html.parser')
       
        sample_txt = "<td class=\"column_value\" colspan=\"3\" id=\"receive_date\">  2020/06/30</td>"
        filtera = r'\d{2}(?:\d{2})?\/\d{1,2}\/\d{1,2}'
        x = re.search(filtera, sample_txt)
        date = soup.find(id="receive_date")
        x = re.search(r"(\d{4})\/(\d{1,2})\/(\d{1,2})",  str(date))
        if x is not None:
            check_counter=3       
            for i in x.groups():     
                check_counter=check_counter-1
            if check_counter == 0:
                date=x.group(0).replace("/","-")
        else :
            print("date format error",date)
        return bsr_session.get("https://bsr.twse.com.tw/bshtm/bsContent.aspx", verify=False), date
    else:
        print("error")
        return False,False

def SRC_data_preprocessing(stockid,resp,date):


    if type(date)==bool and date == False:
        return False
    dicy=str(date)+"/"+str(stockid)
    if os.path.isfile(dicy+'.gz'):
#         print('file exist')
        return True
    if type(resp)==bool and resp==False:
        return False
    elif type(resp)==int and resp==0:
        return True
    
    if resp.status_code != requests.codes.ok and resp!=False:
        return False
#     print(resp.text)
    
    pattern_start = resp.text.find("序號")
    pattern_end = len(resp.text)
    tr_data = resp.text[pattern_start:pattern_end]
    tr_data =tr_data.replace(",,",",")
    pd_data=pd.read_csv(io.StringIO(tr_data))
    #         print(pd_data.head())
    df1 = pd_data.iloc[:,:5]
    df2 = pd_data.iloc[:,5:]
#     print(df1.head())
#     print("-------------")
    #     modify df2 column to same as df1
    for col in df1.columns: 
        old_col=col+".1"
        df2.rename(columns={old_col:col},inplace=True)


    total_df=pd.concat([df1, df2])
    
    total_df.sort_values(by=['序號'], inplace=True)
#     print(total_df.head())
    if not os.path.exists(str(date)):
        os.makedirs(str(date))
    compression_opts = dict(method='zip',
                        archive_name='out.csv')
    
    total_df.to_csv(dicy+'.gz', index = False, header=True,
          compression='gzip')

    return True

def create_cookies():
    fixed_value=';SCREEN_SIZE=WIDTH=1093&HEIGHT=615'
    today=date.today()
    Q=today.strftime("%Y%m%d%H%M%S")  # 格式化日期
    # print(Q)
    A=requests.get("https://api.ipify.org/?format=json", verify=False)
    # A.text
    # my_ip = load(A.text)['ip']
    # jsDumps = json.dumps(A.text)  
    jsLoads = json.loads(A.text)
    public_ip=jsLoads['ip']
    # public_ip
    cookies=Q+'_'+public_ip+fixed_value
    cookies
    return cookies

def get_stockid(head,tail):
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


def local_file(last_folder):
    # 歷遍資料夾下的的檔案名稱，符合後紀錄於local_files
    local_files=list()
    for root, dirs, files in os.walk(last_folder, topdown=True):
        for name in files:
            name=name.replace("'","")
            name=name.replace(".gz","")
            local_files.append(name)
    return local_files
        



import progressbar
def main():
    today = date.today()

    d1 = today.strftime("%Y-%m-%d")
    counter=1
    while(1):
        time.sleep(1)
        counter=counter+1
        print(counter)
        resp,date_y=crawl_SRC('2412')
        if date_y is not False:
            break
        if counter>10:
            break
    print("retry conuter=", counter)
    #resp,date_y=crawl_SRC('2412')
    # directory_list=local_folder(str(date_y))
    # last_folder=directory_list.pop()
    # local_files=local_file(last_folder=date_y)
    print(date_y)
    local_files=local_file(date_y)
    stockid_list=get_stockid('股票','上市認購(售)權證')

    folder_id=None
    date_y=''
    filenametype='.gz'
    folderlist=[]
    folderlist.append('Stock db')

    for _item in progressbar.progressbar(stockid_list):

        x = re.search(r"^([^\s]*)(?=\s)", str(_item))
        

        if x is not None:
            fail_c=0
            try:
                y=local_files.remove(x.group(0))
                if y==None:
                    continue
            except ValueError:
                pass
            try:
                while(1):

                    time.sleep(1)
                    # download data
                    resp,date_y=crawl_SRC(x.group(0))
                    A=SRC_data_preprocessing(x.group(0),resp=resp,date=date_y)
                    if resp is True and date_y is False:
                        break
                    if A is True:
                        break
                    else:
                        fail_c=fail_c+1
                        print (x.group(0)," fail "+str(fail_c))
                        if fail_c>=5:
                            print (x.group(0)," fail")
                            break
            except Exception as  e:
                print(e)
        
                

def notify_service(action):
    filen='endSRC'
    # using file to notify bash script service condition
    if action==1 :
        if os.path.isfile(filen):
            os.remove(filen)
        # clean file 
        # titail="twse start "

    elif action==2 :
        open(filen,mode='a').close()     










def getList():
    url = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    res = requests.get(url, verify = False)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    table = soup.find("table", {"class" : "h4"})
    c = 0
    for row in table.find_all("tr"):
        data = []
        for col in row.find_all('td'):
            col.attrs = {}
            data.append(col.text.strip().replace('\u3000', ''))
        
        if len(data) == 1:
            pass # title 股票, 上市認購(售)權證, ...
        else:
            print(data)



import line_bot_test
def Line_agent(condition,message):
    Line_agent=line_bot_test.M_line_Bot_API()
    current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if condition==1 :
        titail="twse start "
    elif condition==2 :
        titail="twse end "
    elif condition==3 :
        titail="start="+message+"end="+current_time
    else :
        titail=message
    Line_agent.sendMessage(titail)



# In[2]:



def local_files_v1(dir_target="./"):
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
            x = re.search(r"\d{4}\d{2}\d{2}", combine_name)
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

target = r'./older'



# upload xxxx.gz to google driver，指定Stock db為根目錄
 

def upload(D_Handel,folder_id,foldername,file_prex,remove=True):
    file_id=D_Handel.search_file(name=file_prex+".gz",folder_id=folder_id)
    filepath=foldername+"/"+file_prex+".gz"
#     print(target+"/"+filepath[2:])
    if file_id is None:
        D_Handel.uploadFile(filename=file_prex+".gz",filepath=filepath
                            ,mimetype="application/zip"
                            ,folder_id=folder_id)
    else:
        if remove==True:
#             os.remove(filepath)
#             print(target+"/"+foldername[2:])
            if os.path.isdir(target+"/"+foldername[2:]) is False:
                print("目錄不存在。")
                os.makedirs(target+"/"+foldername[2:])
                
            shutil.move(filepath,target+"/"+filepath[2:])
        

def upload_agent():
    directory_list=local_folder()
#     print(directory_list)
    directory_list.sort(reverse = True)
#     last_folder=directory_list.pop()
# local_files=local_file(last_folder=last_folder) 
    D_Handel=upload_file.Google_Driver_API()
    root_folder_id=D_Handel.search_folder(name='Stock db')

    for index,foldername in progressbar.progressbar(enumerate(directory_list)):
        # print("index "+str(index))
        r_foldername=foldername.replace('./','')
        folder_id=D_Handel.search_folder(name=r_foldername,folder_id=root_folder_id)
#     無，則建立資料夾
        if folder_id is None:
            folder_id=D_Handel.search_folder(name='Stock db')
            folder_id=D_Handel.createFolder(name=r_foldername,folder_id=folder_id)  
        
        local_files=local_file(last_folder=foldername)
#         print(str(index))
    
        for filename in progressbar.progressbar(local_files):
            
            try:
                if index == 0:
                    # index==0 表示為目前最新的資料夾  不用改變目錄 或 刪除
                    r_flag=False
                else:
                    r_flag=True
#                     print(foldername+"/"+filename)
                upload(D_Handel=D_Handel,folder_id=folder_id,foldername=foldername
                                                          ,file_prex=filename,remove=r_flag)
            except Exception as  e:
                print(e)
#   local_folder name  ORDER by date DESC
def delet_folder(remain_num,check_Empty=True,dir_target='./',file=False):
    if file is True:
        directory_list=local_files_v1(dir_target=dir_target)
    else:
        directory_list=local_folder(dir_target=dir_target)
#     print(dir_target)
    directory_list.sort(reverse = True)
#     directory_list.pop(0)
#    print(directory_list)
    del directory_list[0:remain_num]
#     print(directory_list)
    for path in directory_list:
        if os.path.exists(path) and file == True:
            os.remove(path)
            continue

        if check_Empty == True: 
            directory= os.listdir(path) 
#         len(directory)=0 表示資料夾內部為空的
            if len(directory) == 0: 
                # print("Empty directory") 
                os.rmdir(path)
        else:
            shutil.rmtree(path, ignore_errors=True)
import Stock_SRC_Small as Stock_Small
import financing_colab as financing
oldfilename='SRCresult.csv'
import random_test as my_SRC
import logging
log_dir="log/"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)
    print(log_dir)
today = date.today().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=log_dir+today+'.log', # write to this file
    filemode='a', # open in append mode
    format='%(name)s - %(levelname)s - %(message)s'
    )
logging.debug("Debug logging test...")

logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)

if __name__ == '__main__':
    # directory_list=local_folder('2020-07-23')
    # local_files=local_file('2020-07-23')
#     notify_service(1)
    # argvL=str(sys.argv)
    try:
        print ('Argument List:', str(sys.argv))
    
        delet_folder(remain_num=240,dir_target='./financing/',check_Empty=False,file=True)
        start_t=current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        financing.main()
        main()
        upload_agent()
        Stock_Small.main()
        delet_folder(remain_num=1)
        delet_folder(remain_num=100,dir_target='./older/',check_Empty=False)
        delet_folder(remain_num=240,dir_target='./financing/',check_Empty=False,file=True)
        A=my_SRC.find_SRC_by_condition(dir='./small_test/')
        #print(A)
        notify=my_SRC.SRC_notify(oldfilename=oldfilename,df=A)
        #print(notify)
    
        Line_agent(condition=3,message=start_t+str(sys.argv))
        if notify is True:
            Line_agent(condition=4,message=str(A))
    except:
        logging.error("Catch an exception.", exc_info=True)
        Line_agent(condition=3,message="find logging.error")
#     notify_service(2)
#     stockid_list=get_stockid('股票','上市認購(售)權證')

