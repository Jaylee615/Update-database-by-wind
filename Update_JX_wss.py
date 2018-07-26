# -*- coding: utf-8 -*-
"""
Created on 2018.07.02
@author: JayLee
功能：使用Wind接口函数wss导入数据到JX数据库
实时更新：每周交易日收盘后17点钟0分导入当天的数据
需要设定的参数：起始数据下载时间、交易日收盘后数据下载时间
"""

from GlobalObject_JX_wss import *
import time as TI
from WindPy import *
from EmQuantAPI import *
w.start()
c.start()

#===============================主函数入口===============================
time1=TI.time()#计时函数
conn = pymssql.connect(host='sqlserver', user='jxlh', password='jingxi8868', database='JX', charset='utf8')
cur = conn.cursor()
time_start = '2005-01-03'
time_now = str(DT.datetime.today())[:10]
if DT.datetime.today().weekday() in [5,6]:
    last_trade_date = w.tdaysoffset(0, time_now).Data[0][0]
else:
    last_trade_date = w.tdaysoffset(-1, time_now).Data[0][0]
last_trade_day = str(last_trade_date)[0:10]
Cycle = "Days=Trading" #取交易日数据
timelist = w.tdays(time_start, last_trade_day, Cycle).Times
for each_time in timelist:
    print("=========下载交易日" + str(each_time) + "数据=========")
    # 获取当前股票池
    stock_list = c.sector("001004", str(each_time)).Data[0::2]
    # stock_list = w.wset("SectorConstituent", u"date=" + str(each_time) + ";sector=全部A股").Data[1]
    sqlstr = f"select CompanyCode from JXDailyData where Date =" + "'" + str(each_time) + "'"
    cur.execute(sqlstr)
    get_curstocks = cur.fetchall()
    cur_stocks = [each[0] for each in get_curstocks]
    #判断数据库当前时间股票数据是否下载完整
    difference_stock = list(set(stock_list).difference(set(cur_stocks)))
    if len(difference_stock)==0:
        print("数据库当前时间股票数据下载完整")
        continue
    else:
        print("数据库当前时间股票数据未下载完整, 继续下载")
        ImportStock_Daily(each_time, difference_stock, cur, conn, Operator='jaylee')
cur.close()
conn.close()
time2=TI.time()
print("数据下载到当前时间需花费：", time2-time1)

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
        sqlstr = f"select CompanyCode from JXDailyData where Date =" + "'" + str(time_now) + "'"
        cur.execute(sqlstr)
        get_curstocks = cur.fetchall()
        cur_stocks = [each[0] for each in get_curstocks]
        stock_list = w.wset("SectorConstituent", u"date=" + time_now + ";sector=全部A股").Data[1]
        difference_stock = list(set(stock_list).difference(set(cur_stocks)))
        ImportStock_Now(timeNow, difference_stock, cur, conn, Operator='jaylee')
        print("数据下载更新完成，休眠60s")
        TI.sleep(1 * 60)
    else:
        print(str(DT.datetime.now())[0:16]+" 不满足下载要求，休眠60s")
        TI.sleep(1 * 60)