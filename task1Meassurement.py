import os
import shutil
import time
import numpy as np
import requests
import json
# import pandas as pd
import matplotlib.pyplot as plt
import subprocess
import atexit

def cleanup():
    # Code to be executed when the program stops
    print("Program stopped. Cleaning up...")
    try:
        subprocess.call(['sh', 'stop_leadnode.sh'])
        subprocess.call(['sh', 'stop_datanodes.sh'])
        # shutil.rmtree('data/')
    except:
        pass

# Register the cleanup function to be called when the program stops
atexit.register(cleanup)

def create_dummy_file(file_path, size):
    with open(file_path, 'wb') as f:
        f.write(b'\0' * size)

def send_get_request(fileId):
    response = requests.get(f'http://localhost:5555/file_by_id/{fileId}')
    return json.loads(response.text), response.status_code

def send_get_metrics():
    response = requests.get(f'http://localhost:5555/get_metrics')
    jsonRes = json.loads(response.text)
    return jsonRes['k'], jsonRes['N']

def send_post_request(file_path, strategy):
    url = 'http://localhost:5555/files'
    files = {'file': open(file_path, 'rb')}
    data = {'strategy': strategy}
    response = requests.post(url, files=files, data=data)
    return json.loads(response.text), response.status_code

def upload_file(file, strategy):
    dummyPath = 'dummy_files/'
    timeList = []
    for _ in range(100):
        start_time = time.time()
        response, status = send_post_request(dummyPath + file, strategy)
        if status != 201:
            print(response)
        execution_time = time.time() - start_time
        timeList.append(execution_time)    
    
    return response["fileId"], timeList

def download_file(fileId):
    timeList = []
    wrongFragmentsReceived = []
    for _ in range(100):
        response, status = send_get_request(fileId)
        if status != 200:
            print(response)
        timeList.append(response["downloadTime"])
        wrongFragmentsReceived.append(response["wrongFragmentsReceived"])

    return timeList, wrongFragmentsReceived

def test_upload_and_download(strategy, k, N):
    subprocess.call(['sh', 'start_leadnode.sh', str(k), str(N)])
    subprocess.call(['sh', 'start_datanodes.sh', str(N)])

    time.sleep(5)
    ## Upload
    buddykb100Id, smallFileList = upload_file('kb100.txt', strategy)
    # Calculate statistics
    smallFileList = np.array(smallFileList) * 1000
    median = np.median(smallFileList)
    average = np.average(smallFileList)
    std_dev = np.std(smallFileList)

    # Create histogram
    plt.hist(smallFileList, color='blue')
    plt.xlabel('Upload Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (100 kb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}')
    plt.savefig(os.path.join("histograms/", f'{strategy}Upload100kb_N{N}k{k}.png'))
    plt.clf()
    
    buddymb1Id, mediumFileList = upload_file('mb1.txt', strategy)
    mediumFileList = np.array(mediumFileList) * 1000
    median = np.median(mediumFileList)
    average = np.average(mediumFileList)
    std_dev = np.std(mediumFileList)

    plt.hist(mediumFileList, color='green')
    plt.xlabel('Upload Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (1 mb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Upload1mb_N{N}k{k}.png'))
    plt.clf()

    buddymb10Id, largeFileList = upload_file('mb10.txt', strategy)
    largeFileList = np.array(largeFileList) * 1000
    median = np.median(largeFileList)
    average = np.average(largeFileList)
    std_dev = np.std(largeFileList)

    plt.hist(largeFileList, color='purple')
    plt.xlabel('Upload Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (10 mb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Upload10mb_N{N}k{k}.png'))
    plt.clf()

    buddymb100Id, extraLargeFileList = upload_file('mb100.txt', strategy)
    extraLargeFileList = np.array(extraLargeFileList) * 1000
    median = np.median(extraLargeFileList)
    average = np.average(extraLargeFileList)
    std_dev = np.std(extraLargeFileList)

    plt.hist(extraLargeFileList, color='orange')
    plt.xlabel('Upload Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (100 mb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Upload100mb_N{N}k{k}.png'))
    plt.clf()

    # ## Download
    smallDownloadList, wrongFragmentsSmall = download_file(buddykb100Id)
    smallDownloadList = np.array(smallDownloadList) * 1000
    median = np.median(smallDownloadList)
    average = np.average(smallDownloadList)
    std_dev = np.std(smallDownloadList)

    plt.hist(smallDownloadList, color='blue')
    plt.xlabel('Download Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (100 kb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}, Max errors: {max(wrongFragmentsSmall)}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Download100kb_N{N}k{k}.png'))
    plt.clf()

    mediumDownloadList, wrongFragmentsMedium = download_file(buddymb1Id)
    mediumDownloadList = np.array(mediumDownloadList) * 1000
    median = np.median(mediumDownloadList)
    average = np.average(mediumDownloadList)
    std_dev = np.std(mediumDownloadList)

    plt.hist(mediumDownloadList, color='green')
    plt.xlabel('Download Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (1 mb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}, Max error: {max(wrongFragmentsMedium)}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Download1mb_N{N}k{k}.png'))
    plt.clf()

    largeDownloadList, wrongFragmentsLarge = download_file(buddymb10Id)
    largeDownloadList = np.array(largeDownloadList) * 1000
    median = np.median(largeDownloadList)
    average = np.average(largeDownloadList)
    std_dev = np.std(largeDownloadList)

    plt.hist(largeDownloadList, color='purple')
    plt.xlabel('Download Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (10 mb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}, Max error: {max(wrongFragmentsLarge)}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Download10mb_N{N}k{k}.png'))
    plt.clf()

    xlDownloadList, wrongFragmentsXL = download_file(buddymb100Id)
    xlDownloadList = np.array(xlDownloadList) * 1000
    median = np.median(xlDownloadList)
    average = np.average(xlDownloadList)
    std_dev = np.std(xlDownloadList)

    plt.hist(xlDownloadList, color='orange')
    plt.xlabel('Download Time (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Histogram of {strategy} approach (100 mb) N = {N}, k = {k}\nMedian: {median:.2f}, Average: {average:.2f}, Std Dev: {std_dev:.2f}, Max error: {max(wrongFragmentsXL)}')
    # plt.legend()
    plt.savefig(os.path.join("histograms/", f'{strategy}Download100mb_N{N}k{k}.png'))
    plt.clf()

    subprocess.call(['sh', 'stop_leadnode.sh'])
    subprocess.call(['sh', 'stop_datanodes.sh'])
    shutil.rmtree('data/')

if __name__ == '__main__':
    dummyPath = 'dummy_files/'
    os.makedirs(os.path.join(dummyPath), exist_ok=True)
    create_dummy_file(os.path.join(dummyPath, 'kb100.txt'), 100000)
    create_dummy_file(os.path.join(dummyPath, 'mb1.txt'), 1000000)
    create_dummy_file(os.path.join(dummyPath, 'mb10.txt'), 10000000)
    create_dummy_file(os.path.join(dummyPath, 'mb100.txt'), 100000000)

    test_upload_and_download('buddy', 3, 3)
    test_upload_and_download('buddy', 3, 6)
    test_upload_and_download('buddy', 3, 12)
    test_upload_and_download('buddy', 3, 24)

    test_upload_and_download('min_copysets', 3, 3)
    test_upload_and_download('min_copysets', 3, 6)
    test_upload_and_download('min_copysets', 3, 12)
    test_upload_and_download('min_copysets', 3, 24)

    test_upload_and_download('random', 3, 3)
    test_upload_and_download('random', 3, 6)
    test_upload_and_download('random', 3, 12)
    test_upload_and_download('random', 3, 24)

