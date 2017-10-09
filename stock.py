import requests
from datetime import datetime, date
from pymongo import MongoClient

import tushare as ts
import numpy as np
from pandas import DataFrame



def getZhangting():
    """
    从金融界获取每日涨停股票信息，并且存入数据库

    :returns: this is a description of what is returned
    :raises keyError: raises an exception
    """
    #to-do 首先判断最新交易日的数据有没有，没有，就全部要下载
    lastDate = getLastDate()
    lastDatetime = datetime(lastDate.year, lastDate.month, lastDate.day)
    url = 'http://home.flashdata2.jrj.com.cn/limitStatistic/ztForce/'+ \
        lastDate.strftime('%Y%m%d') +'.js'

    res = requests.get(url)
    data = res.text[16:].replace('\r\n', '')
    clean_data = data[data.find('Data')+8:-4]

    zt_lists = clean_data.split('],[')
    zts = []
    for item in zt_lists:
        zt = item.split(',')
        d = dict(date = lastDatetime, code = zt[0][1:-1], name = zt[1][1:-1], price = zt[2],
            zt_first_time = fn_toDatetime(zt[7][1:-1], lastDate),
            zt_last_time = fn_toDatetime(zt[8][1:-1], lastDate), zt_reason = '其他')
        zts.append(d)
    
    print(len(zts))
    fn_save_many('zhangting', zts)

        


def getDaily():
    """
    通过tushare获取收盘数据，存入数据库

    :param param1: this is a first param
    :param param2: this is a second param
    :returns: this is a description of what is returned
    :raises keyError: raises an exception
    """
    today_all = ts.get_today_all()
    today_all_list = []
    today = datetime.today()
    for i in today_all.index:
        row = today_all.iloc[i]
        d = dict(date=today, code=row['code'],name=row['name'], 
        changepercent=row['changepercent'], close=row['trade'], open=row['open'],
        high=row['high'], low=row['low'], pre_close=row['settlement'],
        turnover=row['turnoverratio'], amount=row['amount'])
        today_all_list.append(d)

    print(len(today_all_list))
    fn_save_many('daily', today_all_list)

    




def getLastDate():
    """
    辅助函数，把形如09:25:00的字符串转换为日期类型

    :param param1: 形如09:25:00的字符串
    :param param2: date类型
    :returns: datetime类型
    :raises keyError: raises an exception
    """
    #构造一个365长度的列表，2代表非交易日，0和1代表交易日，0代表数据还没下载，1代表数据已经下载
    #这个列表可以放在数据库里面，然后每次先去数据库里读取最后一个1的日期
    lastDate = date(2017,9,27)
    return lastDate


def fn_toDatetime(time_str, date=date.today()):
    """
    辅助函数，把形如09:25:00的字符串转换为日期类型

    :param param1: 形如09:25:00的字符串
    :param param2: date类型
    :returns: datetime类型
    :raises keyError: raises an exception
    """
    return datetime.strptime(date.isoformat()+time_str, '%Y-%m-%d%H:%M:%S')
    

def fn_save_many(collection, list):
    """
    辅助函数，把一个字典的列表存入mongodb数据库

    :param collection: 要存入的集合名称
    :param list: 要插入的数据列表，里面每一项都是一个dict
    :returns: datetime类型
    :raises keyError: raises an exception
    """
    client = MongoClient('mongodb://localhost:27017/')
    db = client['win']
    col = db[collection]
    col.insert_many(list)
    print('save many successed')
    client.close()

    
getDaily()