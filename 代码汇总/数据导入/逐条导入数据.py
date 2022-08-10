import pandas as pd
import numpy as np
import pyodbc
import os
import csv
import re


def insert_tale(filepath, filename, cursor):
    for name in filename:
        path=os.path.join(filepath,name)
        result = path.replace('\\', '/')
        csvfile=open(result+'.csv','r')
        readCSV=csv.reader(csvfile)
        next(readCSV)
        for row in readCSV:
            sql='insert into [dbo]'+'.'+'[1907'+name+'] values ('+'\''+'{0}'+'\''+',{1},{2},{3},{4},{5},{6},{7})'
            sql=sql.format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7])
            print(sql)
            cursor.execute(sql)
        cursor.commit()

def filtercontract(contractname,path):
    '''
    :param contractname: a list of filename under a file
    :return: a list of contract name without format like 'DYa009' and 'DYaMI'
    '''
    filteredname=[]
    filterout=[]
    for name in contractname:
        check=re.match('\w\w\w\d\d\d\d',name)
        check2=re.match('\w\w\w\w\d\d\d\d',name)
        if check is not None or check2 is not None:
            filteredname.append(name)
        else:
            outpath=os.path.join(path,name)
            outpath=outpath.replace('\\','/')
            filterout.append(outpath)
    df=pd.DataFrame(filterout)
    print(df)
    return filteredname


# def creattable(filename, cursor):
#     for name in filename:
#         sql='create table '+'[dbo]'+'.'+'[1907'+name+']'+'(Time datetime not null,[open] int not null, high int not null,low int not null,[close] int not null,volume int not null,amount int not null,open_interest int not null)'
#         cursor.execute(sql)
#     cursor.commit()

if __name__ == '__main__':

    server = "10.7.6.92"
    user = "temp"
    password = "XJTLU12345shixi"
    database = "HFData"

conn=pyodbc.connect(DRIVER='{SQL Server}',SERVER=server,DATABASE=database,UID=user,PWD=password)
#conn = pymssql.(server, user, password, database, charset='GBK')
if conn:
    print('success')
else:
    print('failure')

cur = conn.cursor()  #create a cursor

filepath='C:/Users/public/lmj/DL-1min-temp'
#filename=['DYj2005','DYl2005', 'DLjd2005', 'DYpp2005', 'DYeg2005', 'DYc2005', 'DYy2005', 'DYi2005', 'DYm2005', 'DYp2005' ]

filename=os.listdir(filepath)
emptyfile=[]

for file in filename:
    newpath=os.path.join(filepath,file)
    newpath=newpath.replace('\\','/')
    contractname=os.listdir(newpath)
    filteredname=filtercontract(contractname,newpath)
    for contract in filteredname:
        wholepath=os.path.join(newpath,contract)
        wholepath=wholepath.replace('\\','/')
        print('wholepath:'+wholepath)

        symboltemp = ''.join(re.split(r'[^a-z]', contract[0:-4]))

        maturetemp='20'+contract[-8:-6]+'-'+contract[-6:-4]+'-01'

        csvfile = open(wholepath, 'r')
        readcsv = csv.reader(csvfile)
        try:
            next(readcsv)
        except:
            emptyfile.append(wholepath)
        else:
            for line in readcsv:
                sql = 'insert into DL_1min_18to20temp(time,maturity,symbol,[open],high,low,[close],volume,amount,open_interest) values(' \
                      + '\'' + '{0}' + '\'' + ',' + '\'' + '{1}' + '\'' + ',' + '\'' + '{2}' + '\'' + ',{3},{4},{5},{6},{7},{8},{9})'
                sql = sql.format(line[0], maturetemp, symboltemp, line[1], line[2], line[3], line[4], line[5], line[6],
                                 line[7])
                cur.execute(sql)
                print('success')

        cur.commit()





print('END')





#creattable(filename,cur)
#print('create table success')
#insert_tale(filepath,filename,cur)


# cur.execute(sql) #run sql command
# #cur.fetchone()
# row=cur.fetchall() #read selected results
# result=np.array(row)
# df=pd.DataFrame(result,columns=['code','total asset'])
# print(df)


#must close cursor and connection
cur.close()
conn.close()