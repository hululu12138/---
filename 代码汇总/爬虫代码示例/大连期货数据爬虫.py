'''
author: Meijia Liu
Create time: 2021-08-21 08:23:00
'''

import requests
from lxml import etree
import pandas as pd
import numpy as np
import csv
import codecs

def transfer(list):
    newLi=[]
    transfer = lambda x: ''.join(x).strip()
    for i in list:
        newLi.append(transfer(i))
    del(newLi[10])
    del(newLi[10])
    newLi=newLi[:-2]
    return newLi

def getColumns(url,headers):
    data = {
        'dayQuotes.variety': 'all',
        'dayQuotes.trade_type': '0',
        'year': '2004',
        'month': '0',
        'day': '02'
    }
    page_text = requests.post(url=url, data=data, headers=headers).text

    tree = etree.HTML(page_text)
    column = tree.xpath('//div[@class="tradeArea"]//div[@class="dataArea"]//tr[1]/th/text()')
    return column

if __name__ == '__main__':
    url='http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'
    }
    column=getColumns(url,headers)
    with open('期货数据.csv','w',encoding='utf-8') as fp:
        # writer=csv.DictWriter(fp,fieldnames=column)
        # writer.writeheader()
        writer=csv.writer(fp,delimiter=',')
        writer.writerow(column)


        csvfile=open('businessDay.csv','r')
        readcsv=csv.reader(csvfile)
        next(readcsv)
        count=0
        for line in readcsv:
            print(line)
            year=line[0].split('/')[0]
            month=line[0].split('/')[1]
            day=line[0].split('/')[2]
            data={
                'dayQuotes.variety': 'all',
                'dayQuotes.trade_type': '0',
                'year': fr'{year}',
                'month':fr'{int(month)-1}',
                'day':fr'{day}'
            }
            page_text=requests.post(url=url,data=data,headers=headers).text

            tree=etree.HTML(page_text)
            totalNum=len(tree.xpath('//div[@class="tradeArea"]//div[@class="dataArea"]//tr'))



            for trNum in range(2,totalNum+1):
                origin=tree.xpath(fr'//div[@class="tradeArea"]//div[@class="dataArea"]//tr[{trNum}]//text()')
                newData=transfer(origin)
                newData.append(str(line[0]))
                newData[1]=newData[1]+'d'
                # print(newData)
                if newData[-5]=='0':
                    continue
                else:
                    pass
                    # writer.writerow({'商品名称':newData[0],'交割月份':'\t'+newData[1],'开盘价':newData[2],
                    #                  '最高价':newData[3],'最低价':newData[4],'收盘价':newData[5],'前结算价':newData[6],
                    #                  '结算价':newData[7],'涨跌':newData[8],'涨跌1':newData[9],'成交量':newData[10],
                    #                  '持仓量':newData[11],'持仓量变化':newData[12],'成交额':newData[13],'time':newData[14]})
                    writer.writerow(newData)
                    print('success')
