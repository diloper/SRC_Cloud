import io,re,os 
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
