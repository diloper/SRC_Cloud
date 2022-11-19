#!/usr/bin/env python
# coding: utf-8

# In[18]:


# %load upload_file.py
#!/usr/bin/env python

# In[14]:


# %load upload_file.py
from __future__ import print_function
import pickle
import os.path
import io
from apiclient import errors
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

from urllib.error import HTTPError
# If modifying these scopes, delete the file token.pickle.
#SCOPES = ['https://www.googleapis.com/auth/drive']
SCOPES = ['https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets.readonly']
#SCOPES = ['https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive.resource']
#SAMPLE_SPREADSHEET_ID = '1_NwoXo-M2x250v3Ky7ZPG_pgKk0XOLZP2q8C-xvw6Tc'
#SAMPLE_RANGE_NAME = 'financing!A2:D'

class Google_Driver_API:
#     self.drive_service
    def __init__(self):
        self.drive_service=None
        self.login_Auth()
        
    def login_Auth(self):
        """Shows basic usage of the Drive v3 API."""
        self.creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        # create credentials with google  tutorial
        self.drive_service= build('drive', 'v3', credentials=creds)
        self.sheet_service= build('sheets', 'v4', credentials=creds)
     
    def getSheetvalue(self,SAMPLE_SPREADSHEET_ID='1_NwoXo-M2x250v3Ky7ZPG_pgKk0XOLZP2q8C-xvw6Tc',SAMPLE_RANGE_NAME='financing!A2:D'):
        try:
            sheet = self.sheet_service.spreadsheets()
            result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range=SAMPLE_RANGE_NAME).execute()
            values = result.get('values', [])
    
            if not values:
                print('No data found.')
                return None
    
#            print('Name, Major:')
#            for row in values:
#                # Print columns A and E, which correspond to indices 0 and 4.
#    #            print('%s, %s' % (row[0], row[4]))
#                print('%s' % (row))
              
        except HTTPError as err:
            print(err)
        return values
        
    def listFiles(self,size,auto_nextPage=False):
        page_token = None
        while True:
            response = self.drive_service.files().list(pageSize=size,q="mimeType='image/jpeg'",
                                                  spaces='drive',
                                                  fields='nextPageToken, files(id, name)',
                                                  pageToken=page_token).execute()
            for file in response.get('files', []):
                # Process change
                print ('Found file: %s (%s)' % (file.get('name'), file.get('id')))
            page_token = response.get('nextPageToken', None)
            if page_token is None or auto_nextPage is False:
                break
#         results = self.drive_service.files().list(pageSize=size,fields="nextPageToken, files(id, name)").execute()
#         items = results.get('files', [])
#         if not items:
#             print('No files found.')
#         else:
#             print('Files:')
#             for item in items:
#                 print('{0} ({1})'.format(item['name'], item['id']))

    def uploadFile(self,filename,filepath,mimetype,folder_id):
    #     'parents':[{"id":"0B6Nxxxxxxxx"}]
        file_metadata = {'name':filename}
        # print("uploadFile file",filename)
        if folder_id is not None:
             file_metadata.update( {'parents' : [folder_id]} )
        media = MediaFileUpload(filepath,
                                mimetype=mimetype)
        file = self.drive_service.files().create(body=file_metadata,
                                            media_body=media,
                                            fields='id').execute()
#         print('File ID: %s' % file.get('id'))

    def downloadFile_SRC(self,file_id,local_filepath,IO=False):
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
#            print("Download %d%%." % int(status.progress() * 100))
        if IO == True:
            return fh
        with io.open(local_filepath,'wb') as f:
            fh.seek(0)
            f.write(fh.read())
    


    def downloadFile(self,file_id,local_filepath,IO=False):
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))
        if IO == True:
            fh.seek(0)
            return fh.read()
        with io.open(local_filepath,'wb') as f:
            fh.seek(0)
            f.write(fh.read())
            
    def createFolder(self,name,folder_id):
        
        file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
        }
        # print("uploadFile file",filename)
        if folder_id is not None:
             file_metadata.update( {'parents' : [folder_id]} )
        
        # print("create folder",pre_item)
        file = self.drive_service.files().create(body=file_metadata,
                                            fields='id').execute()
        print ('create Folder ID: %s' % file.get('id'))
        return file.get('id')

    def searchFile(self,size,query):
        results = self.drive_service.files().list(
        pageSize=size,fields="nextPageToken, files(id, name, kind, mimeType)",q=query).execute()
        items = results.get('files', [])
        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(item)
                print('{0} ({1})'.format(item['name'], item['id']))

    def search_JPEG_File(self,service):
        page_token = None
        while True:
            response = service.files().list(q="mimeType='image/jpeg'",
                                                  spaces='drive',
                                                  fields='nextPageToken, files(id, name)',
                                                  pageToken=page_token).execute()
            for file in response.get('files', []):
                # Process change
                print ('Found file: %s (%s)' % (file.get('name'), file.get('id')))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
                
    def search_folder(self,name,folder_id=None):
        qurry = {
        'name': "name='"+name+"'"
        }
        Q=qurry.get('name')+" and mimeType = 'application/vnd.google-apps.folder'   and trashed = false"
        if folder_id is not None:
            Q=Q+" and parents in '{}'".format(folder_id)
        
        
        response = self.drive_service.files().list(q=Q,spaces='drive').execute()
        for file in response.get('files', []):
            # Process change
            if(file.get('name')==name):
                
                return file.get('id')
        return None
		
    def search_folders(self,folder_id):



        Q="mimeType = 'application/vnd.google-apps.folder'\
        and trashed = false and parents in '{}'".format(folder_id)
        
        try:

            files = []
            page_token = None
            while True:        
                response = self.drive_service.files().list(q=Q,spaces='drive',
                          fields='nextPageToken, '
                              'files(id, name)',
                          pageToken=page_token).execute()

                files.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break    
        except errors.HttpError as error:
             print ('An error occurred: %s' % error)
             files = None

        return files

    def list_folder_files(self,folder_id):
      try:

        files = []
        page_token = None
        while True:
          # pylint: disable=maybe-no-member
          response = self.drive_service.files().list(q="parents in '{}'".format(folder_id)+" and trashed = false",
                          spaces='drive',
                          fields='nextPageToken, '
                              'files(id, name)',
                          pageToken=page_token).execute()
          # for file in response.get('files', []):
            # Process change
            # print(F'Found file: {file.get("name")}, {file.get("id")}')
          files.extend(response.get('files', []))
          page_token = response.get('nextPageToken', None)
          if page_token is None:
            break

      except errors.HttpError as error:
        print ('An error occurred: %s' % error)
        files = None

      return files

		
		
    def search_file(self,name,folder_id=None):
        qurry = {
        'name': "name='"+name+"'"
        }
        
        
        Q=qurry.get('name')+" and mimeType != 'application/vnd.google-apps.folder'        and trashed = false"
        if folder_id is not None:
            Q=Q+" and parents in '{}'".format(folder_id)
            
        # q="mimeType='application/vnd.google-apps.spreadsheet' 
        # and parents in '{}'".format(folder_id)
        response = self.drive_service.files().list(q=Q
                                        ,spaces='drive',corpora='user',
                                       fields="files(id, name, kind, mimeType)").execute()
        re=response.get('files', [])
#         print(re)
        for file in re:
            # Process change
            if(file.get('name')==name):
#                 print(file.get('id'))
#                 print ('Found file: %s (%s)' % (file.get('name'), file.get('id')))

                return file.get('id')

        return None

    def Uploadfile_Agent(self,filename,filenametype,local_file_path,pre_folder=None):

        # if pre_folder!=None:
        #     for t in pre_folder:
        #         print("folder name",t)
        for s in (filename,filenametype,local_file_path):
    #         print(s)
            if type(s) == str and len(s) < 1:
                print('formate error',s,'type(s)==string and len(s)>0')
                return False

        s_filename=filename+filenametype
        oldfolder_id=''
            # create folder if not exist
        for pre_item in pre_folder:
            folder_id=self.search_folder(name=pre_item)

            print("folder name",pre_item)
            if folder_id is None:
                if oldfolder_id != '':
                    oldfolder_id=self.createFolder(name=pre_item,folder_id=oldfolder_id)
                else:    
                    oldfolder_id=self.createFolder(name=pre_item,folder_id=folder_id)
            else :
                oldfolder_id=folder_id #紀錄上一層資料夾的ID
        try:
            # if oldfolder_id != '':
            #     folder_id=oldfolder_id
#             file_id=self.search_file(name=filename,)
#             if file_id is None:
            if oldfolder_id != '':
                file_id=self.search_file(name=filename,folder_id=oldfolder_id)
                if file_id is None:
                    self.uploadFile(filename=filename
                                ,filepath=local_file_path
                                ,mimetype="application/zip"
                                ,folder_id=oldfolder_id)
            else:
                file_id=self.search_file(name=filename,folder_id=folder_id)
                if file_id is None:
                    self.uploadFile(filename=filename
                                ,filepath=local_file_path
                                ,mimetype="application/zip"
                                ,folder_id=folder_id)
        except Exception as  e:
            print(e)
            
            
            
    def get_stockid(self,head,tail):
        # D_Handel=Google_Driver_API()
        folder_id=self.search_folder(name='Stock db')
        file_id=self.search_file(name='isin.html',folder_id=folder_id)
        html=self.downloadFile(file_id=file_id,local_filepath="isin.html",IO=True)
        # del D_Handel
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
    
def main():

# ----------------demo above function ------------------- 
# Need to create folder with  filepath='./2020-07-02/' 
# and filename='059762.gz' in local directory
# ,before start run


    D_Handel=Google_Driver_API()
    A=D_Handel.getSheetvalue()
#    print(A)
    print(type(A))
#    D_Handel.get_stockid('股票','上市認購(售)權證')
#    filename='test.gz'
#    filepath='./2020-07-02/'
#    foldername='python create'
#    folderlist=[]
#    folderlist.append('test1')
#    folderlist.append('test2')
#    folderlist.append('test3')
#    local_file_path=filepath+filename
#    filenametype='.gz'
    


    # D_Handel.Uploadfile_Agent(pre_folder=folderlist
    #                  ,filename=filename
    #                  ,filenametype=filenametype
    #                  ,local_file_path=local_file_path)
#     folder_id=D_Handel.search_folder(name=foldername)
#     if folder_id is None:
#         folder_id=D_Handel.search_folder(name='Stock db')
#         folder_id=D_Handel.createFolder(name=foldername,folder_id=folder_id)
#     else:
#         D_Handel.uploadFile(filename=filename,filepath=filepath+filename,mimetype="application/zip",folder_id=folder_id)
#         file_id=D_Handel.search_file(name=filename)
#         if file_id is not None:
#             D_Handel.downloadFile(file_id=file_id,local_filepath='downlaod from Drive'+filename)
    
#     file_id=D_Handel.search_file(name=filename)
          
            
if __name__ == '__main__':
    main()


# In[ ]:





# In[ ]:





# In[ ]:



