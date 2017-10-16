import requests
from datetime import datetime, date
from pymongo import MongoClient

import tushare as ts

# 获取股票实时现价用ts.get_realtime_quotes('000581')
# 从股票选为observe的时候开始记录daily的数据
# 获取dialy数据用ts.ts.get_hist_data('000001')有涨幅，但是没有成交额，
# ts.get_k_data('0000001')则反之

#-------------------    TO  DO    ----------------------#
# 下一步要思考数据获取的替代方案，每类数据都要找出一个替代方案，保证每天都能及时下载数据

#-------------------交易日有关 BEGIN----------------------#
def create_trade_days(year):
    """
    生成某年交易日，1为交易日，0为非交易日，2是交易日涨停数据已经下载。存入meta集合

    :raises keyError: raises an exception
    """
    length = 366 if ((year%4 == 0 and year%100 !=0)or(year%400==0)) else 365
    trade_days = [1]*length

    # 公休日设为0,必须手动
    trade_days[fn_n_from_date(year,1,1)-1]=0
    trade_days[fn_n_from_date(year,1,2)-1]=0
    trade_days[fn_n_from_date(year,1,27)-1]=0
    trade_days[fn_n_from_date(year,1,28)-1]=0
    trade_days[fn_n_from_date(year,1,29)-1]=0
    trade_days[fn_n_from_date(year,1,30)-1]=0
    trade_days[fn_n_from_date(year,1,31)-1]=0
    trade_days[fn_n_from_date(year,2,1)-1]=0
    trade_days[fn_n_from_date(year,2,2)-1]=0
    trade_days[fn_n_from_date(year,4,2)-1]=0
    trade_days[fn_n_from_date(year,4,3)-1]=0
    trade_days[fn_n_from_date(year,4,4)-1]=0
    trade_days[fn_n_from_date(year,4,29)-1]=0
    trade_days[fn_n_from_date(year,4,30)-1]=0
    trade_days[fn_n_from_date(year,5,1)-1]=0
    trade_days[fn_n_from_date(year,5,29)-1]=0
    trade_days[fn_n_from_date(year,5,30)-1]=0
    trade_days[fn_n_from_date(year,10,2)-1]=0
    trade_days[fn_n_from_date(year,10,3)-1]=0
    trade_days[fn_n_from_date(year,10,4)-1]=0
    trade_days[fn_n_from_date(year,10,5)-1]=0
    trade_days[fn_n_from_date(year,10,6)-1]=0

    d = {}
    j=1
    # 周末设为0
    for i in range(length):
        month, day = fn_date_from_n(year, i+1)
        trade_day = date(year,month, day)
        trade_day
        if trade_days[i] != 0 and trade_day.weekday() <5:
            d[trade_day.strftime("%Y%m%d")]=j
            d[j] = trade_day.strftime("%Y%m%d")
            j += 1

    return d      

def fn_date_from_n(year, n):
    """
    辅助函数，n是一年中第几天，返回月、日tuple

    :param year: int 年份
    :param n: int 第几天，元旦是第1天
    :returns: 返回一个(month, day)tuple
    :raises keyError: raises an exception
    """
    notleap = [31,59,90,120,151,181,212,243,273,304,334,365]
    leap = [31,60,91,121,152,182,213,244,274,305,335,366]
    months = leap if (year%4 == 0 and year%100 !=0)or(year%400==0) else notleap

    if n<=31:
        return (1,n)

    for month,past_days in enumerate(months):
        if n<= past_days:
            day = n - months[month-1]
            return (month+1, day)

def fn_n_from_date(year, month, day):
    """
    辅助函数，根据年月日返回天数

    :param collection: 要存入的集合名称
    :param doc: 要插入的文档,dict类型
    :raises keyError: raises an exception
    """
    notleap = [31,28,31,30,31,30,31,31,30,31,30,31]
    leap = [31,29,31,30,31,30,31,31,30,31,30,31]
    months = leap if (year%4 == 0 and year%100 !=0)or(year%400==0) else notleap

    if month ==1:
        return day
    
    for i in range(month-1):
        day += months[i]
        
    return day
    
def fn_pre_day(trade_day):
    """
    辅助函数，获取上一个交易日

    :param trade_day: datetime或者date类型都可以
    :returns: 返回一个datetime对象，便于pymongo的查找
    :raises keyError: raises an exception
    """
    n= TRADE_DAYS_2017[trade_day.strftime("%Y%m%d")]
    return datetime.strptime(TRADE_DAYS_2017[n-1],"%Y%m%d") 


TRADE_DAYS_2017 = create_trade_days(2017)
#-------------------交易日有关 END----------------------#


def get_zhangting(zt_date):
    """
    获取每日涨停股票信息，并且存入数据库
    需要金融界网站支持

    :param zt_date: (month, day)的元组
    :returns: this is a description of what is returned
    :raises keyError: raises an exception
    """
    year = 2017
    import_date = datetime(year, zt_date[0], zt_date[1])
    url = 'http://home.flashdata2.jrj.com.cn/limitStatistic/ztForce/'+ \
        import_date.strftime('%Y%m%d') +'.js'

    res = requests.get(url)
    data = res.text[16:].replace('\r\n', '')
    clean_data = data[data.find('Data')+8:-4]
    zt_lists = clean_data.split('],[')

    client = MongoClient('mongodb://localhost:27017/')
    db = client['win']
    everything = db['everything']

    #测试有没有导入过
    result = everything.find_one(
        {"zhangtings.date":import_date}
    )
    if type(result)==dict:
        print(import_date.isoformat()[:10],": 该日期已经下载过")
        client.close()
        return None

    pre_day = fn_pre_day(import_date)
    print("前一交易日为 ",pre_day.isoformat()[:10]) 

    insert_count = 0
    for item in zt_lists:
        zt = item.split(',')
        #去掉ST股
        if float(zt[3]) < 6:
            continue
        #获取上一个交易日的连板数
        result = everything.find_one(
            {"code":zt[0][1:-1],"zhangtings.date":pre_day},
            {"zhangtings.succession":1}
        )

        succession_num = (result["zhangtings"][0]["succession"] + 1) if result else 1

        d = dict(date = import_date, price = float(zt[2]),succession=succession_num,
        zt_first_time = fn_toDatetime(zt[7][1:-1], import_date),
        zt_last_time = fn_toDatetime(zt[8][1:-1], import_date), zt_reason = '其他')
        result = everything.update_one(
            {"code": zt[0][1:-1]},
            {
                "$push":{
                     "zhangtings":d
                }
            }
        )
        insert_count += result.modified_count

        #如果是新股票
        if result.matched_count == 0:
            zhangting =[]
            zhangting.append(d)
            d_new = dict(code = zt[0][1:-1], name = zt[1][1:-1], selected = 0,themes =[],
            observations=[], plans = [], shoulds =[], comments=[], zhangtings=zhangting)
            result = everything.insert_one(d_new)
            insert_count += 1
    
    print(import_date.isoformat()[:10],' save ', insert_count, ' items successfully')
    client.close()

def get_zhangtings(from_date, to_date=date.today().strftime("%Y%m%d")):
    """
    批量获取每日涨停股票信息，并且存入数据库。应对出差错过。
    不支持跨年
    需要金融界网站支持

    :param from_date: str "20170101"，方便去交易日里查询。起始时间，包含。
    :param to_date: str "20170101" 结束时间，包含。不加入交易日判断，因此请输入交易日，默认是当天
    :returns: 
    :raises keyError: raises an exception
    """
    # from_n = fn_n_from_date(int(from_date[:4]),int(from_date[4:6]),int(from_date[6:]))
    # to_n = fn_n_from_date(int(to_date[:4]),int(to_date[4:6]),int(to_date[6:]))
    from_n = TRADE_DAYS_2017[from_date]
    to_n = TRADE_DAYS_2017[to_date]

    year = int(from_date[:4])
    for i in range(from_n,to_n+1):
        zt_date = datetime.strptime(TRADE_DAYS_2017[i],"%Y%m%d")
        get_zhangting((zt_date.month, zt_date.day))

        
def get_all_stocks():
    """
    第一次全量导入everything集合，以后在导入今日涨停时增量导入
    需要tushare支持

    :returns: this is a description of what is returned
    :raises keyError: raises an exception
    """
    all_stocks = ts.get_stock_basics()
    all_stocks_list = []
    length = len(all_stocks)
    for i in range(length):
        code = all_stocks.index[i]
        name = all_stocks.iloc[i]['name']
        d = dict(code = code, name = name, selected = 0,themes =[],
        observations=[], plans = [], shoulds =[], comments=[], zhangtings=[])
        all_stocks_list.append(d)
    
    fn_save_many('everything', all_stocks_list)

def get_daily():
    """
    通过tushare获取收盘数据，存入数据库

    :param param1: this is a first param
    :param param2: this is a second param
    :returns: this is a description of what is returned
    :raises keyError: raises an exception
    """
    today_all = ts.get_today_all()
    today_all_list = []
    _today_date = datetime.today()
    today = datetime(_today_date.year, _today_date.month, _today_date.day)
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
    lastDate = date(2017,10,9)
    return lastDate


def fn_toDatetime(time_str, zt_date):
    """
    辅助函数，把形如09:25:00的字符串转换为datetime类型

    :param param1: 形如09:25:00的字符串
    :param param2: datetime类型
    :returns: datetime类型
    :raises keyError: raises an exception
    """
    return datetime.strptime(zt_date.isoformat()[:10]+time_str, '%Y-%m-%d%H:%M:%S')
    

def fn_save_many(collection, docs):
    """
    辅助函数，把一个字典的列表存入mongodb数据库

    :param collection: string 要存入的集合名称
    :param docs: list 要插入的数据，里面每一项都是一个dict
    :returns: None
    :raises keyError: raises an exception
    """
    client = MongoClient('mongodb://localhost:27017/')
    db = client['win']
    col = db[collection]
    result = col.insert_many(docs)
    assert len(docs)==len(result.inserted_ids)
    print('save ',len(docs),' successed')
    client.close()

def fn_save_one(collection, doc):
    """
    辅助函数，把一个文档存入mongodb数据库

    :param collection: 要存入的集合名称
    :param doc: 要插入的文档,dict类型
    :raises keyError: raises an exception
    """
    client = MongoClient('mongodb://localhost:27017/')
    db = client['win']
    col = db[collection]
    result = col.insert_one(doc)
    assert result.inserted_id != None
    print('save 1 successed')
    client.close()

def temp():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['win']
    meta = db['meta']

    n =fn_n_from_date(2017, 8, 11)-1
    print(n)
    meta.update_one(
        {"name":2017,"$atomic":"true"},
        {"$set":{
            "content."+str(n):2
        }}
    )

    client.close()

# get_zhangtings('20170928')


