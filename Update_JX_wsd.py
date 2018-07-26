# -*- coding: utf-8 -*-
"""
Created on 2018.07.02
@author: JayLee
功能：使用Wind接口函数wsd导入数据到JX数据库
实时更新：每周交易日收盘后17点钟0分导入当天的数据
需要设定的参数：起始数据下载时间、交易日收盘后数据下载时间
"""
from GlobalObject_JX_wsd import *
from WindPy import *
import time
w.start()
#===============================主函数入口===============================
#查询当前交易日及当前交易日的上一个交易日
time_start=time.time()#计时函数
time_now = str(DT.datetime.today())[:10]
if DT.datetime.today().weekday() in [5,6]:
    last_trade_date = w.tdaysoffset(0, time_now).Data[0][0]
else:
    last_trade_date = w.tdaysoffset(-1, time_now).Data[0][0]
last_trade_day = str(last_trade_date)[0:10]
# 获取当前股票池
stock_list = w.wset("SectorConstituent", u"date=" + time_now + ";sector=全部A股").Data[1]
# 获取上市日期
Ipo_date = pd.read_excel('Ipo_date.xlsx')
#取交易日数据
Cycle = "Days=Trading"
conn = pymssql.connect(host='sqlserver', user='jxlh', password='jingxi8868', database='JX', charset='utf8')
cur = conn.cursor()
sqlstr = f"select CompanyCode, max(Date) as Maxdate from JXDailyData group by CompanyCode"
cur.execute(sqlstr)
get_maxdate = dict(cur.fetchall())
conn.commit()
for i in range(len(stock_list)):
    #查询数据库中该股票的最新时间记录
    print(str(i) + "_" + stock_list[i])
    exist_flag = stock_list[i] in get_maxdate.keys()
    # 若当前数据库为空, 则从设定的起始日期开始下载数据
    if exist_flag==False:
        self_start_time = '2005-01-03'
        stock_Ipodate = str(Ipo_date.ix[0,stock_list[i]])[0:10]
        if stock_Ipodate > self_start_time:
            self_start_time = stock_Ipodate
        print("数据未下载，起始下载时间：", self_start_time)
        timelist = w.tdays(self_start_time, last_trade_day, Cycle).Times
        ImportStock_Daily(timelist, stock_list[i], cur, conn, Operator='jaylee')
    # 若当前数据库非空，则从最新记录日期开始下载数据
    else:
        print("数据已存在，进行数据更新")
        start_time = get_maxdate[stock_list[i]]
        timelist = w.tdays(start_time, last_trade_day, Cycle).Times
        # 若数据库中该股票的最新时间即为当前时间，则无需对该段时间进行数据下载
        if len(timelist)<=1:
            continue
        ImportStock_Daily(timelist[1:], stock_list[i], cur, conn, Operator='jaylee')
cur.close()
conn.close()
time_end=time.time()
print("数据下载到当前时间需花费：", time_end-time_start)

#循环判断当前时间是否满足数据下载时间要求
print("=========循环判断当前时间是否满足下载要求(当日为交易日且收盘后17点进行数据下载)=========")
while True:
    timeNow = DT.datetime.now()
    time_now = str(timeNow)[0:10]
    # 判断当日是否为交易日
    timelist_now = w.tdays(time_now, time_now).Times
    if len(timelist_now)>0 and timeNow.hour == 17 and timeNow.minute == 0:
        print("=========下载交易日" + str(timeNow)[0:16] + "数据=========")
        conn = pymssql.connect(host='sqlserver', user='jxlh', password='jingxi8868', database='JX', charset='utf8')
        cur = conn.cursor()
        cur_stock_list = w.wset("SectorConstituent", u"date=" + time_now + ";sector=全部A股").Data[1]
        ImportStock_Now(timeNow, cur_stock_list, cur, conn, Operator='jaylee')
        print("数据下载更新完成，休眠60s")
        TI.sleep(1 * 60)
    else:
        print(str(DT.datetime.now())[0:16]+" 不满足下载要求，休眠60s")
        TI.sleep(1 * 60)