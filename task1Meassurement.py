import os
import time
import requests
import json
import pandas as pd

def create_dummy_file(file_path, size):
    with open(file_path, 'wb') as f:
        f.write(b'\0' * size)

def send_get_request(fileId):
    response = requests.get(f'http://localhost:5555/file_by_id/{fileId}')
    return json.loads(response.text), response.status_code

def send_post_request(file_path, strategy):
    url = 'http://localhost:5555/files'
    files = {'file': open(file_path, 'rb')}
    data = {'strategy': strategy}
    response = requests.post(url, files=files, data=data)
    return json.loads(response.text), response.status_code

def test_upload_file(file, strategy):
    dummyPath = 'dummy_files/'
    timeList = []
    for _ in range(100):
        start_time = time.time()
        response, status = send_post_request(dummyPath + file, strategy)
        execution_time = time.time() - start_time
        timeList.append(execution_time)    
    
    return response["fileId"], timeList

def test_download_file(fileId):
    response = requests.get(f'http://localhost:5555/filename_by_id/{fileId}')
    jsonRes = json.loads(response.text)
    timeList = []
    fragmentsReceied = []
    for _ in range(100):
        response, status = send_get_request(fileId)
        timeList.append(response["downloadTime"])
        fragmentsReceied.append(response["numberOfFragments"])

    return timeList, fragmentsReceied

if __name__ == '__main__':
    dummyPath = 'dummy_files/'
    os.makedirs(os.path.join(dummyPath), exist_ok=True)
    kb100 = create_dummy_file(os.path.join(dummyPath, 'kb102400.txt'), 102400)
    mb1 = create_dummy_file(os.path.join(dummyPath, 'mb1.txt'), 1000000)
    mb10 = create_dummy_file(os.path.join(dummyPath, 'mb10.txt'), 10000000)
    mb100 = create_dummy_file(os.path.join(dummyPath, 'mb100.txt'), 100000000)

    ## Buddy - Upload
    buddykb102400Id, smallFileList = test_upload_file('kb102400.txt', 'buddy')
    buddymb1Id, mediumFileList = test_upload_file('mb1.txt', 'buddy')
    buddymb10Id, largeFileList = test_upload_file('mb10.txt', 'buddy')
    buddymb100Id, extraLargeFileList = test_upload_file('mb100.txt', 'buddy')

    # ## Buddy - Download
    smallDownloadList, smallDownloadFragments = test_download_file(buddykb102400Id)
    mediumDownloadList, mediumDownloadFragments = test_download_file(buddymb1Id)
    largeDownloadList, largeDownloadFragments = test_download_file(buddymb10Id)
    xlDownloadList, xlDownloadFragments = test_download_file(buddymb100Id)

    df = pd.DataFrame({'1': smallFileList, '2' : mediumFileList, '3' : largeFileList, '4' : extraLargeFileList, 
                       '5':'', '':smallDownloadList, '6':mediumDownloadList, '7':largeDownloadList, '8':xlDownloadList, 
                       '9':'', '10':smallDownloadFragments, '11':mediumDownloadFragments, '12':largeDownloadFragments, '13':xlDownloadFragments})

    # Append the DataFrame to an Excel file
    with pd.ExcelWriter('task1meassurements.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
        df.to_excel(writer, sheet_name='buddy', index=False, header=False)
    
    # # ## Min Copysets - Upload
    minCopysetskb102400Id, smallFileList = test_upload_file('kb102400.txt', 'min_copysets')
    minCopysetsmb1Id, mediumFileList = test_upload_file('mb1.txt', 'min_copysets')
    minCopysetsmb10Id, largeFileList  = test_upload_file('mb10.txt', 'min_copysets')
    minCopysetsmb100Id, extraLargeFileList = test_upload_file('mb100.txt', 'min_copysets')

    # ## Min Copysets - Download
    smallDownloadList, smallDownloadFragments = test_download_file(minCopysetskb102400Id)
    mediumDownloadList, mediumDownloadFragments = test_download_file(minCopysetsmb1Id)
    largeDownloadList, largeDownloadFragments = test_download_file(minCopysetsmb10Id)
    xlDownloadList, xlDownloadFragments = test_download_file(minCopysetsmb100Id)
        
    df = pd.DataFrame({'1': smallFileList, '2' : mediumFileList, '3' : largeFileList, '4' : extraLargeFileList, 
                       '5':'', '':smallDownloadList, '6':mediumDownloadList, '7':largeDownloadList, '8':xlDownloadList, 
                       '9':'', '10':smallDownloadFragments, '11':mediumDownloadFragments, '12':largeDownloadFragments, '13':xlDownloadFragments})

    # Append the DataFrame to an Excel file
    with pd.ExcelWriter('task1meassurements.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
        df.to_excel(writer, sheet_name='min_copysets', index=False, header=False)

    # # ## Random - Upload
    randomkb102400Id, smallFileList = test_upload_file('kb102400.txt', 'random')
    randommb1Id, mediumFileList = test_upload_file('mb1.txt', 'random')
    randommb10Id, largeFileList = test_upload_file('mb10.txt', 'random')
    randommb100Id, extraLargeFileList = test_upload_file('mb100.txt', 'random')

    # # ## Random - Download
    smallDownloadList, smallDownloadFragments = test_download_file(randomkb102400Id)
    mediumDownloadList, mediumDownloadFragments = test_download_file(randommb1Id)
    largeDownloadList, largeDownloadFragments = test_download_file(randommb10Id)
    xlDownloadList, xlDownloadFragments = test_download_file(randommb100Id)
    
    df = pd.DataFrame({'1': smallFileList, '2' : mediumFileList, '3' : largeFileList, '4' : extraLargeFileList, 
                       '5':'', '':smallDownloadList, '6':mediumDownloadList, '7':largeDownloadList, '8':xlDownloadList, 
                       '9':'', '10':smallDownloadFragments, '11':mediumDownloadFragments, '12':largeDownloadFragments, '13':xlDownloadFragments})

    # Append the DataFrame to an Excel file
    with pd.ExcelWriter('task1meassurements.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
        df.to_excel(writer, sheet_name='random', index=False, header=False)
