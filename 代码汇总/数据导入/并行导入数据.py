"""
Created on Sun May 24 18:14:42 2021

@author: Meijia Liu
"""

import pandas as pd
import pymssql
import os
import csv
import re
import multiprocessing
import time

start=time.perf_counter()
allFileNum = 0
level=1
#path = 'C:/Users/jljiang/Desktop/高频数据/200501/SH2'
path = 'C:/Users/user9/Desktop/202012'

conn = pymssql.connect(server="10.7.6.92", user="temp", password="XJTLU12345shixi", database="HFData")
cursor=conn.cursor()

def insert(level,path,f):
    global allFileNum
    fileList = []
    emptyfile = []
    if (os.path.isdir(path + '/' + f)):
        printPath((level + 1), path + '/' + f)  # recursion
    if (os.path.isfile(path + '/' + f)):
        datefile = str(path + '/' + f)
        if 'DL' or 'SQ' or 'IN' in datefile:  # no ZZ first
            # datefile = str(path + '/' + f)
            # datefile=datefile.replace('/','\\\\')
            print(datefile)

            csvfile = open(datefile, 'r')
            readcsv = csv.reader(csvfile)

            try:
                symbolname = ''.join(re.split(r'[^a-z]', f[0:-4]))
            except:
                symbolname = '-'
            filename = f[0:-4]
            market = datefile.split('/')[-2]

            # filter out empty file
            try:
                next(csvfile)
            except:
                emptyfile.append(datefile)
            else:
                for line in readcsv:
                    sql = 'insert into HFData_test([time],symbol,futures_market,[open],high,low,[close],volume,amount,open_interest,[filename]) WITH(TABLOCK) values(' \
                          + '\'' + '{0}' + '\'' + ',' + '\'' + '{1}' + '\'' + ',' + '\'' + '{2}' + '\'' + ',{3},{4},{5},{6},{7},{8},{9}' + ',' + '\'' \
                          + '{10}' + '\'' + ')'

                    sql = sql.format(line[0], symbolname, market, line[1], line[2], line[3], line[4], line[5],
                                     line[6], line[7], filename)
                    print(sql)
                    cursor.execute(sql)
                    conn.commit()
                    print('success')
        fileList.append(f)

    for fl in fileList:
        allFileNum = allFileNum + 1
    print(fileList)
    print(emptyfile)

def printPath(level, path):
    files = os.listdir(path)
    # process = []
    # for f in files:
    #     p = multiprocessing.Process(target=insert, args=[level, path, f])
    #     p.start()
    #     process.append(p)
    #
    #
    # for pro in process:
    #     pro.join()
    for f in files:
        insert(level,path,f)


if __name__ == '__main__':

    printPath(level, path)
    finish=time.perf_counter()
    print(f'process finished in {finish-start} seconds')
    print('总文件数 =', allFileNum)
    cursor.close()
    conn.close()

