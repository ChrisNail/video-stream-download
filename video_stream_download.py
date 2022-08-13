import glob
import json
import os
import subprocess
import threading
import time
import urllib as urlLib

def streamTsFile(url, filePath='./', fileName='stream.ts', start=1, digits=1, retry=False, log=True):
    """Download a stream of TS videos from a URL, appending them to a file until the end of the video is reached"""
    num = start
    byteSize = 0
    crawling = True
    buffer = []
    BYTES_TO_MB = (1024 * 1024)

    destination = filePath + fileName
    if fileName.find('.ts') == -1:
        destination += '.ts'
    
    # If not a retry, clear the file
    if not retry:
        file = open(destination, mode='w')
        file.close()

    # Log to stdout
    def __log(str, end='\n'):
        if log:
            print(str, end=end)

    # Grab a segment of the TS file and append it to the buffer
    def __getSegment(index, bufferNum=0):
        try:
            index_str = "{:0{digits}}".format(index, digits=digits)
            fileUrl = url.replace('[i]', index_str)
            request = urlLib.request.urlopen(fileUrl, timeout=10)
            fileContents = request.read()
            nonlocal byteSize
            byteSize += len(fileContents)
            buffer.append(fileContents)
            return True
        except urlLib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise e

    # Flush buffer to file
    def __flush():
        file = open(destination, mode='ab')
        nonlocal buffer
        for b in buffer:
            if b != None:
                file.write(b)

        file.close()
        buffer = []

    # Loop while grabbing segments until a 404 is found
    def __crawlStream():
        nonlocal crawling, num
        while crawling:
            crawling = __getSegment(num)
            __log("Downloading '{}' - Segment {}: {:.2f} MB".format(fileName, num, byteSize / BYTES_TO_MB), end='\r')
            if (len(buffer) >= 10):
                __flush()

            num += 1

        __flush()

    try:
        __crawlStream()
        __log("Finished downloading {} ({:.2f} MB)".format(destination, byteSize / BYTES_TO_MB))
    except:
        # If the file failed to download, remove it
        __log("Failed to download file: {} (Failed segment: {})".format(destination, num))
        remove = input('Save file for a retry (y/n)? ')
        if remove.find('y') == -1:
            os.remove(destination)

def streamFileList(listFileName, filePath='./', start=1, digits=1, log=True):
    """Download a list of files from a JSON array that contains objects with url and filename attributes"""
    def __log(str, end='\n'):
        if log:
            print(str, end=end)

    file_list = []
    try:
        file = open(listFileName)
        file_list = json.load(file)
    except Exception as e:
        __log('Could not open file ' + listFileName)
        raise e

    __log('Downloading {} files'.format(len(file_list),))
    for stream in file_list:
        streamTsFile(stream['url'], filePath, stream['filename'], start, digits, log)
    __log('')

def convertMp4(fileName):
    """Use ffmpeg to convert a TS file to an MP4"""
    mp4FileName = fileName.replace('.ts', '.mp4')
    subprocess.run(['ffmpeg', '-hide_banner', '-nostats', '-loglevel', 'quiet', '-i', fileName, '-c:v', 'libx264', '-c:a', 'aac', mp4FileName])

def batchConvertMp4(file_exp='./*.ts'):
    """Use ffmpeg to batch convert multple TS files to MP4 using threading"""
    file_list = glob.glob(file_exp)
    for fileName in file_list:
        thread = threading.Thread(target=convertMp4, args=(fileName,))
        thread.start()

    while threading.active_count() > 1:
        print('Files remaining: {}'.format(threading.active_count() - 1), end='\r')
        time.sleep(1)
