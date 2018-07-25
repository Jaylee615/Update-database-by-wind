# -*- coding: utf-8 -*-
"""
Created on 2018.07.02
@author: JayLee
"""
import uuid
import pymssql
import pandas as pd
import numpy as np
from WindPy import *
import datetime as DT
import time as TI
import win32serviceutil
import win32service
import win32event
import logging
import inspect
import traceback
w.start()
pd.set_option('expand_frame_repr', False)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 17)

# 获取某一日期所有股票数据
def Get_dailyData(loadDate, stockList):
    loadDataFeild = ['mkt_cap_ard', 'open', 'high', 'low', 'close', 'pct_chg', 'volume', 'amt', 'trade_status',
                     'pb_lyr', 'pe_ttm', 'pe_lyr']
    AllData = pd.DataFrame({loadDataFeild[i]: np.repeat(None, len(stockList)) for i in range(len(loadDataFeild))},
                           columns=loadDataFeild, index=stockList)
    for WDFeild in loadDataFeild:
        loadi = w.wsd(','.join(stockList), WDFeild, loadDate, loadDate, "unit=1;ruleType=3;Days=Alldays;PriceAdj=F")
        pddfi = pd.DataFrame(loadi.Data, index=[WDFeild], columns=loadi.Codes).transpose()
        AllData[WDFeild] = pddfi
    return AllData

# 获取某只股票某一时间段内数据
def Get_timelistData(timelist, stock):
    loadDataFeild = ['mkt_cap_ard', 'open', 'high', 'low', 'close', 'pct_chg', 'volume', 'amt', 'trade_status',
                     'pb_lyr', 'pe_ttm', 'pe_lyr']
    AllData = pd.DataFrame({loadDataFeild[i]: np.repeat(None, len(timelist)) for i in range(len(loadDataFeild))},
                           columns=loadDataFeild, index=timelist)
    for WDFeild in loadDataFeild:
        loadi = w.wsd(stock, WDFeild, timelist[0], timelist[-1], "unit=1;ruleType=3;Days=Alldays;PriceAdj=F")
        pddfi = pd.DataFrame(loadi.Data, index=[WDFeild], columns=loadi.Times).transpose()
        AllData[WDFeild] = pddfi
    return AllData

# 插入stocklist中某只股票记录到数据库
def Write_stock_toDB(AllData, stockCode, loadDate, cur, conn, upDateOperator):
    valueList = [AllData.reindex([stockCode]).iloc[0, :][k] for k in
                 range(len(AllData.reindex([stockCode]).iloc[0, :]))]
    stringValueList = [str(x) for x in valueList]
    insertString = ''
    for k in range(len(valueList)):
        if type(valueList[k]) == str:
            insertString = insertString + ',' + "'" + stringValueList[k] + "'"
        elif valueList[k] is None:
            insertString = insertString + ',' + 'NULL'
        else:
            if np.isnan(valueList[k]):
                insertString = insertString + ',' + 'NULL'
            else:
                insertString = insertString + ',' + stringValueList[k]
    GUID = str(uuid.uuid1())
    insertStringF = f"'{GUID}','{stockCode}'{insertString},'{loadDate}','{upDateOperator}'"
    tableField = 'JXDailyDataID,CompanyCode,Value,OpenPrice,HighPrice,LowPrice,ClosePrice,DGR,Volume,Amount,TradeStatus,PB,PETTM,PELYR,Date,UpdateOperator'
    sqlstr = f"INSERT INTO JXDailyData({tableField}) VALUES ({insertStringF})"
    cur.execute(sqlstr)
    conn.commit()

# 插入timelist中某个日期记录到数据库
def Write_daily_toDB(AllData, stockCode, loadDate, cur, conn, upDateOperator):
    valueList = [AllData.reindex([loadDate]).iloc[0, :][k] for k in
                 range(len(AllData.reindex([loadDate]).iloc[0, :]))]
    stringValueList = [str(x) for x in valueList]
    insertString = ''
    for k in range(len(valueList)):
        if type(valueList[k]) == str:
            insertString = insertString + ',' + "'" + stringValueList[k] + "'"
        elif valueList[k] is None:
            insertString = insertString + ',' + 'NULL'
        else:
            if np.isnan(valueList[k]):
                insertString = insertString + ',' + 'NULL'
            else:
                insertString = insertString + ',' + stringValueList[k]
    GUID = str(uuid.uuid1())
    insertStringF = f"'{GUID}','{stockCode}'{insertString},'{loadDate}','{upDateOperator}'"
    tableField = 'JXDailyDataID,CompanyCode,Value,OpenPrice,HighPrice,LowPrice,ClosePrice,DGR,Volume,Amount,TradeStatus,PB,PETTM,PELYR,Date,UpdateOperator'
    sqlstr = f"INSERT INTO JXDailyData({tableField}) VALUES ({insertStringF})"
    cur.execute(sqlstr)
    conn.commit()

#timelist时段内某只股票数据导入数据库
def ImportStock_Daily(timelist, stock, cur, conn, Operator='Operator'):
    AllData = Get_timelistData(timelist, stock)
    if AllData.isnull().values.all():
        print('下载数据为空，流量超出下载限制,停止下载')
        exit()
    else:
        print("数据下载完成，存入数据库中......")
        for i in range(len(timelist)):
            try:
                Write_daily_toDB(AllData, stock, timelist[i], cur, conn, Operator)
            except:
                print("该记录已存在于数据库")

#当前时间每只股票数据导入数据库(需判断当前时间是否为除权除息日)
def ImportStock_Now(loadDate, stockList, cur, conn, Operator='Operator'):
    AllData = Get_dailyData(loadDate, stockList)
    if AllData.isnull().values.all():
        print('下载数据为空，流量超出下载限制,停止下载')
        exit()
    else:
        print("=========判断stockList在loadData日是否除权除息=========")
        for i in range(len(stockList)):
            stockCode = stockList[i]
            #读取当前交易日的前一个交易日数据库中收盘价
            last_tradeday = w.tdaysoffset(-1,loadDate).Data[0][0]
            str_last_tradeday = str(last_tradeday)[0:10]
            sqlstr = f"select ClosePrice from JXDailyData where CompanyCode='" + stockCode + "' and Date='" + str_last_tradeday + "'"
            cur.execute(sqlstr)
            get_lastclose = cur.fetchall()
            # 若该股为第一天上市的新股，则无需进行除权除息判断并直接存入数据
            if len(get_lastclose)==0:
                print(stockCode+"为新股，无需进行除权除息判断")
                try:
                    temp_Data1 = Get_dailyData(loadDate, [stockCode])
                    Write_stock_toDB(temp_Data1, stockCode, loadDate, cur, conn, Operator)
                except:
                    print("该记录已存在于数据库，停止下载")
                continue
            DB_last_Close = float(get_lastclose[0][0])
            conn.commit()
            #读取当前交易日的前一个交易日Wind中收盘价
            Wind_last_Close = w.wsd(stockCode, "close", str_last_tradeday, str_last_tradeday, "PriceAdj=F").Data[0][0]
            #如果该股票在数据库中上一交易日收盘价与wind中相等,则loadDate为非除权日,只需下载当天loadDate数据
            if DB_last_Close == Wind_last_Close:
                try:
                    Write_stock_toDB(AllData, stockCode, loadDate, cur, conn, Operator)
                except:
                    print("该记录已存在于数据库，停止下载")
            #如果不相等,则loadDate为除权日,需要对loadDate之前所有数据进行更新
            else:
                print(stockCode+" 在"+str(loadDate)[0:10]+"日除权除息，并对其进行数据更新")
                sqlstr = f"select min(Date) from JXDailyData where CompanyCode='" + stockCode + "'"
                cur.execute(sqlstr)
                start_time = cur.fetchall()[0][0]
                sqlstr = f"delete from JXDailyData where CompanyCode=" + "'" + stockCode + "'"
                cur.execute(sqlstr)
                conn.commit()
                #起始时间到除权除息日时间
                div_exdate = w.tdays(start_time, loadDate).Times
                for each_time in div_exdate:
                    temp_Data2 = Get_dailyData(each_time, [stockCode])
                    Write_stock_toDB(temp_Data2, stockCode, each_time, cur, conn, Operator)
        cur.close()
        conn.close()