#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError
import configparser
import os

# LINE 聊天機器人的基本資料

# total_section = config.sections()
# for sSection in total_section:
#     print ("Section = ", sSection)
#     for item in config.items(sSection):
#         print ("key = %s, valule = %s" % (item[0], item[1]))


# In[ ]:


class M_line_Bot_API:
#     self.drive_service
    def __init__(self):
        config = configparser.ConfigParser()
        A=config.read('line_bot.ini')
        access_token=config.get('line-bot', 'channel_access_token')
        self.to=config.get('line-bot', 'user_id')
        self.line_bot_api = LineBotApi(access_token)
        
    def sendMessage(self,text):
        try:
            self.line_bot_api.push_message(self.to, TextSendMessage(text=text))
        except LineBotApiError as e:
            print(e)


# In[ ]:




# line_bot_api = LineBotApi('<channel access token>')


# In[ ]:


def wirteINI():
    config = configparser.ConfigParser()
    config['DEFAULT'] = {'ServerAliveInterval': '45',
                          'Compression': 'yes',
                          'CompressionLevel': '9'}
    config['bitbucket.org'] = {}
    config['bitbucket.org']['User'] = 'hg'
    config['topsecret.server.com'] = {}
    topsecret = config['topsecret.server.com']
    topsecret['Port'] = '50022'     # mutates the parser
    topsecret['ForwardX11'] = 'no'  # same here
    config['DEFAULT']['ForwardX11'] = 'yes'
    with open('AAAAAA.ini', 'w') as configfile:
        config.write(configfile)
        
def loadINI():
#     curpath = os.path.dirname(os.path.realpath(__file__))
    cfgpath = os.path.join(os.getcwd(), 'AAAAAA.ini')
    # 創建對象
    conf = configparser.ConfigParser()
    # 讀取INI
    conf.read(cfgpath, encoding='utf-8')
    # 取得所有sections
    sections = conf.sections()
    # 取得某section之所有items，返回格式為list
    for a in sections:
        items = conf.items(a)
    return ([sections, items])

def testAPI():
    wirteINI()
    loadINI()


# In[ ]:

from datetime import date, timedelta, datetime
import sys
if __name__ == '__main__':
    param_1= sys.argv[1] 
    print ('Params=', param_1)
    A=M_line_Bot_API()
    current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    A.sendMessage(param_1+" "+current_time)
