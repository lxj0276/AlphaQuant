__author__ = 'wangjp'

import os
import sys
import time
import datetime as dt

import numpy as np
import pandas as pd
import configparser as cp

import cx_Oracle
import mysql.connector

from HelpModules.Logger import Logger
from HelpModules.Calendar import Calendar
from DataReaderModule.CacheManager import CacheManager
from DataReaderModule.Constants import ALIAS_FIELDS as alf
from DataReaderModule.Constants import ALIAS_STATUS as als
from DataReaderModule.Constants import rootPath, DatabaseNames, QuickTableFieldsDict, NO_QUICK


class DataReader:
    """
    本地 mysql 数据库读取接口
    对于本地不存在的字段，可以从远程万德数据库读取
    对常用数据 提供数据缓存功能
    """
    calendar = None
    cacheManager = None
    dateCalibrator = None
    QuickFieldsTableDict = None


    def __init__(self, basePath=None, cacheLevel='LEVEL1', connectRemote=False):
        if basePath is None:
            basePath = os.path.join(rootPath,'DataReaderModule')
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(basePath,'configs','dataPath.ini'))
        self.h5Path = cfp.get('data', 'h5')
        cfp.read(os.path.join(basePath,'configs', 'loginInfo.ini'))
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.connectRemote = connectRemote
        if self.connectRemote:
            loginfoWind = dict(cfp.items('Wind'))
            self.connWind = cx_Oracle.connect(r'{user}/{password}@{host}/{database}'.format(**loginfoWind))
        # create logger
        self.logger = Logger(logPath=os.path.join(basePath,'log')).get_logger(loggerName=__name__, logName='database_reader')
        self.logger.info('')
        # construct catch manager
        if DataReader.cacheManager is None:
            DataReader.cacheManager = CacheManager(basePath=basePath, cacheLevel=cacheLevel)
            self.logger.info('CacheManager constructed')
        # construct calendar
        if DataReader.calendar is None:
            DataReader.calendar = Calendar()
            self.logger.info('Clendar constructed')
        # construct quickFieldsTableDict
        if DataReader.QuickFieldsTableDict is None:
            self.quickFieldsTableDict()

    def quickFieldsTableDict(self):
        outDict = {}
        for tb in QuickTableFieldsDict:
            for fld in QuickTableFieldsDict[tb]:
                outDict[fld] = tb
        DataReader.QuickFieldsTableDict = outDict
        self.logger.info('QuickFieldsTableDict constructed')

    def fields_check(self, fields, checkRemote=False):
        """
        根据给定的 fields 匹配其对应的数据表
        :param fields:
        :return:
        """
        fields = list(set(fields))  # 去除重复列
        fieldsByTable = {}
        for fld in fields:
            if fld in self.QuickFieldsTableDict:
                if fieldsByTable.get(self.QuickFieldsTableDict[fld]) is None:
                    fieldsByTable[self.QuickFieldsTableDict[fld]] = [fld]
                else:
                    fieldsByTable[self.QuickFieldsTableDict[fld]].append(fld)
            else:
                if checkRemote: # 检查远程数据库中对应字段
                    raise NotImplementedError
                else:
                    if fieldsByTable.get('NON_LOCAL') is None:
                        fieldsByTable['NON_LOCAL'] = [fld]
                    else:
                        fieldsByTable['NON_LOCAL'].append(fld)
        print(fieldsByTable)
        return fieldsByTable


    def get_data(self,
                 headDate=None,
                 tailDate=None,
                 dateList=None,
                 stkList=None,
                 fields=None,
                 selectType='CloseClose',
                 tableName=None,
                 dbName=None,
                 useCache=True,
                 fromMysql=False):
        """
        从数据库中读取信息 包括基础数据 以及 因子数据 含头 含尾
        :param headDate
        :param tailDate
        :param dateList  如果有 dateList 则dateList 优先级高于 head and tail date
        :param stkList
        :param fields
        :param selectType
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        if stkList is not None:
            stkListStr = ['"{}"'.format(stk) for stk in stkList]
        if dateList is None:
            headDate = self.calendar._tradeDates[0] if headDate is None else str(headDate)
            tailDate = self.calendar._tradeDates[-1] if tailDate is None else str(tailDate)
        else:
            dateList = self.calendar.tdaysoffset(num=0, currDates=dateList)     # 将 输入日期 校准到 交易日
        indexFields = [alf.DATE, alf.STKCD]
        # 检查需要提取的字段 是否存在 本地数据库没有的字段
        fieldsByTable = self.fields_check(fields=fields, checkRemote=self.connectRemote)
        if fieldsByTable.get('NON_LOCAL') is not None:
            self.logger.warning('Fields {0} is not stored in local database, will not be extracted'.
                                format(','.join(fieldsByTable['NON_LOCAL'])))
        # take selected data
        outData = pd.DataFrame([])
        if tableName is not None:   # 如果给定 tableName 则在指定的table取数据
            raise NotImplementedError
        else:
            for table in fieldsByTable:
                if table=='NON_LOCAL':  # 非本地已存储数据
                    raise NotImplementedError
                else:
                    if table in self.cacheManager._tableSaved:      # 该表 (部分或全部字段) 已经被缓存
                        cachedFields = [fld for fld in fieldsByTable[table] if fld in self.cacheManager._fieldsSaved[table]]
                        unCachedFields = [fld for fld in fieldsByTable[table] if fld not in self.cacheManager._fieldsSaved[table]]
                    else:
                        cachedFields = []
                        unCachedFields = fieldsByTable[table]
                if cachedFields:    # 可以从缓存读取的字段
                    start = time.time()
                    # 先将缓存中存储的 全部字段 数据取出
                    if dateList is not None:   # 避免通过 .loc[dateList, cachedFields] 的方式提取缓存数据时 遇到不存在的日期index
                        headDate = dateList[0]
                        tailDate = dateList[-1]
                        dateSlice = slice(headDate,tailDate)
                    else:
                        dateSlice = slice(None)
                        dateList = self.calendar.tdaysbetween(headDate=headDate, tailDate=tailDate)
                    cachedIdx = (dateSlice,slice(None)) if stkList is None else (dateSlice, stkList)
                    cachedData = self.cacheManager._tableSaved[table].loc[cachedIdx, cachedFields]    # 不存在的日期将不会被取出
                    # 检查 需要提取的 字段 缓存的日期情况
                    partialFields = []
                    for fld in cachedFields:
                        # 计算 该字段已缓存的 每天对应的股票数量
                        fldStkCnt = self.cacheManager._fieldStkCnt[table][fld]
                        fldStkCnt.columns = [alf.STKCNT]
                        cachedDates = fldStkCnt.index.values      #  该字段当前存储的日期，可能存在部分天里没有全部股票
                        extraDates = list(set(dateList) - set(cachedDates))    #  没有缓存的日期 该日期的全部股票都需要取
                        countDiff = self.cacheManager._stockCounts.loc[cachedDates,:] - fldStkCnt      # 缓存里有该天，但是不是全部股票的数据， 为了省时间也都取出来
                        partialDates = countDiff[countDiff[alf.STKCNT] > 0].index.values      # 已经缓存的 含有部分股票的天数
                        takeFields = [alf.DATE, alf.STKCD, fld]
                        addedFieldData = pd.DataFrame([], columns=[fld])    # 该字段所需提取的 缓存额外的 数据
                        if extraDates:
                            ####  需要提取全部股票的日期 #####
                            if not fromMysql:
                                extraDates = ['"{}"'.format(tdt) for tdt in extraDates]
                            keyDict = {'fields': ','.join(takeFields),
                                       'dbname': dbName,
                                       'table':  '_'.join([table,'quick']) if table not in NO_QUICK else table,
                                       'tdt': alf.DATE,
                                       'stk': alf.STKCD,
                                       'dates': ','.join(extraDates),
                                       'stkcds': ','.join(stkListStr) if stkList is not None else None,
                                       'stkcdLine': '' if stkList is None else 'AND ({0} IN ({1}))'.format(alf.STKCD, ','.join(stkListStr)),
                                       }
                            if fromMysql:
                                sqlLines = 'SELECT {fields} FROM {dbname}.{table} WHERE {tdt} IN ({dates}) {stkcdLine}'.format(**keyDict)
                                extraFieldData = pd.read_sql(sql=sqlLines,
                                                             con=self.connMysqlRead,
                                                             index_col=indexFields,
                                                             columns=takeFields)
                            else:
                                h5File = os.path.join(self.h5Path, '{}.h5'.format(table))
                                whereLines = ''.join(['{tdt} in ({dates})'.format(**keyDict),
                                                      '' if stkList is None else 'and {stk} in ({stkcds})'.format(**keyDict)])
                                extraFieldData = pd.read_hdf(path_or_buf=h5File,
                                                             key=table,
                                                             where=whereLines,
                                                             columns=takeFields,
                                                             mode='r')
                            addedFieldData = extraFieldData
                        if partialDates.shape[0]>0:
                            #### 需要提取部分股票的日期 ###
                            partialStocks = list(set([pair[1] for pair in self.cacheManager._fieldsIndex[table][fld] if
                                                      pair[0] in partialDates]))  # 已经缓存的那部分股票
                            needStksStr = ['"{}"'.format(stk) for stk in (set(stkList) - set(partialStocks))]
                            if not fromMysql:
                                partialDates = ['"{}"'.format(tdt) for tdt in partialDates]
                            keyDict = {'fields': ','.join(takeFields),
                                       'dbname': dbName,
                                       'table':  '_'.join([table,'quick']) if table not in NO_QUICK else table,
                                       'tdt': alf.DATE,
                                       'stk': alf.STKCD,
                                       'dates': ','.join(partialDates),
                                       'stkcds': ','.join(stkListStr) if stkList is not None else None,
                                       'stkcdLine': 'AND ({0} NOT IN ({1}))' if stkList is None else 'AND ({2} IN ({3}))'
                                           .format(alf.STKCD, ','.join(partialStocks),alf.STKCD,','.join(needStksStr)),
                                       }
                            if fromMysql:
                                sqlLines = 'SELECT {fields} FROM {dbname}.{table} WHERE {tdt} IN ({dates}) {stkcdLine}'.format(**keyDict)
                                partialStockData = pd.read_sql(sql=sqlLines, con=self.connMysqlRead, columns=fields, index_col=indexFields)
                            else:
                                h5File = os.path.join(self.h5Path, '{}.h5'.format(table))
                                whereLines = ''.join(['{tdt} in ({dates})'.format(**keyDict),
                                                      '' if stkList is None else 'and {stk} in ({stkcds})'.format(**keyDict)])
                                partialStockData = pd.read_hdf(path_or_buf=h5File,
                                                               key=table,
                                                               where=whereLines,
                                                               columns=takeFields,
                                                               mode='r')
                            addedFieldData = partialStockData if addedFieldData.empty else pd.concat([addedFieldData, partialStockData], axis=0, sort=False)
                        if not addedFieldData.empty:   # 缓存字段不足，有新数据需要写入
                            if not partialFields: # 第一次拼接, 直接把需要的 index 加满
                                cachedData = pd.concat([cachedData, addedFieldData], axis=0, sort=False)
                            else:       # 后续拼接，需要的index 都已有了，直接往里面写入就行
                                cachedData.loc[addedFieldData.index, fld] = addedFieldData
                            partialFields.append(fld)
                            if useCache:
                                self.cacheManager.checkinCache(tableName=table, tableData=addedFieldData)      # 将新取出的数据加入缓存
                    outData = cachedData if outData.empty else outData.join(other=cachedData, on=indexFields)
                    dataShape = cachedData.shape
                    self.logger.info('Fileds {} loaded from CACHE'.format(','.join(cachedFields)))
                    self.logger.info('Table {0} has {1} rows and {2} cols loaded from CACHE with {3} seconds'
                                     .format(table, dataShape[0], dataShape[1], time.time() - start))
                if unCachedFields:   # 需要从数据库读取的字段
                    start = time.time()
                    unCachedFields = indexFields + unCachedFields
                    if not fromMysql:
                        dateList = ['"{}"'.format(tdt) for tdt in dateList] if dateList is not None else None
                    keyDict = {'fields': ','.join(unCachedFields),
                               'dbname': dbName,
                               'table': '_'.join([table,'quick']) if table not in NO_QUICK else table,
                               'tdt': alf.DATE,
                               'stk': alf.STKCD,
                               'head': headDate,
                               'tail': tailDate,
                               'dates': '' if dateList is None else ','.join(dateList),
                               'stkcds': ','.join(stkListStr) if stkList is not None else None,
                               'stkcdLine': '' if stkList is None else 'AND ({0} IN ({1}))'.format(alf.STKCD, ','.join(stkListStr)),
                               'signhead': '>=' if selectType in ('CloseClose', 'CloseOpen') else '>',
                               'signtail': '<=' if selectType in ('OpenClose', 'CloseClose') else '<',
                               }
                    if fromMysql:
                        if dateList is not None:    # 通过 dateList 提取
                            sqlLines = 'SELECT {fields} FROM {dbname}.{table} WHERE ({tdt} IN ({dates})) {stkcdLine}'.format(**keyDict)
                        else:       #通过 headDate to tailDate 提取
                            sqlLines = 'SELECT {fields} FROM {dbname}.{table} WHERE {tdt}{signhead}{head} AND {tdt}{signtail}{tail} {stkcdLine}'.format(**keyDict)
                        newData = pd.read_sql(sql=sqlLines,
                                              con=self.connMysqlRead,
                                              columns=unCachedFields,
                                              index_col=indexFields)
                    else:
                        h5File = os.path.join(self.h5Path, '{}.h5'.format(table))
                        if dateList is not None:
                            whereLines = ''.join(['{tdt} in ({dates})'.format(**keyDict),
                                                  '' if stkList is None else 'and {stk} in ({stkcds})'.format(**keyDict)])
                        else:
                            whereLines = ''.join(['{tdt}{signhead}"{head}" and {tdt}{signtail}"{tail}"'.format(**keyDict),
                                                  '' if stkList is None else 'and {stk} in ({stkcds})'.format(**keyDict)])
                        newData = pd.read_hdf(path_or_buf=h5File,
                                              key=table,
                                              where=whereLines,
                                              columns=unCachedFields,
                                              mode='r')
                    if useCache:
                        self.cacheManager.checkinCache(tableName=table, tableData=newData)     # 数据库取出的数据加缓存
                    outData = newData if outData.empty else outData.join(other=newData, on=indexFields)
                    dataShape = newData.shape
                    self.logger.info('Fileds {} loaded from LOCAL DATABASE'.format(','.join(unCachedFields)))
                    self.logger.info('Table {0} has {1} rows and {2} cols loaded from LOCAL DATABASE with {3} seconds'
                                     .format(table, dataShape[0], dataShape[1], time.time() - start))
                self.logger.info('Table {0} : fields {1} loaded'.format(table, ','.join(fieldsByTable[table])))
        return outData

    def get_responses(self,
                      headDate=None,
                      tailDate=None,
                      stkList=None,
                      selectType='CloseClose',
                      retTypes=None,
                      fromMysql=True):
        """
        计算收益率 ： 只考虑收益率是否完整有意义，日期是否匹配
                    不考率交易行为, 交易行为性过滤交给 filterY
        主要过滤   ： 交易日期是否与指数日期匹配 : 主要是考虑股票数据空缺问题（因并购重组停牌等）
                     期间停牌天数是否占比过大 ： 可以通过roll.sum 来计算，但只有日期匹配才为正确值
        :param stkList:
        :param retNums: 收益率天数
        :param retTypes: 收益率类型 开收， 收收， 开开， 收开
        :return: 返回 以dateList, stkList 为 index 的dataframe，注意 收益率应为 dateList 中日期所对应的 未来收益
        """
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        if retTypes is None:
            retTypes = {'OC': [1], 'CC': [2,3], }
        # 提取所有的日期数
        allNums = []
        for rtype in retTypes:
            for num in retTypes[rtype]:
                if num not in allNums:
                    allNums.append(num)
        if headDate is not None:
            headDate = self.calendar.tdaysoffset(0, currDates=headDate)
        if tailDate is not None:
            tailDate = self.calendar.tdaysoffset(0, currDates=tailDate)
            extendTailDate = self.calendar.tdaysoffset(num=np.max(allNums), currDates=tailDate)     # 取期间数据时 需要额外的天数
        else:
            extendTailDate = None
        responseFields = ['OCRet', 'CORet', 'CCRet', 'NOTRADE']
        retsData = self.get_data(headDate=headDate,
                                 tailDate=extendTailDate,
                                 dateList=None,
                                 stkList=stkList,
                                 fields=responseFields,
                                 selectType=selectType,
                                 fromMysql=fromMysql,
                                 useCache=True)
        # 需要先校准 交易日期    先把过滤计算好
        retsData.reset_index(inplace=True)
        retsData['currDate'] = retsData[alf.DATE]
        retsData.set_index([alf.DATE, alf.STKCD], inplace=True)
        retsData['buyDate'] = retsData['currDate'].groupby(level=alf.STKCD,sort=False).shift(-1)     # 当前股票日期对应的买入日
        retsData['notrdCnt'] = retsData['NOTRADE'].groupby(level=alf.STKCD,sort=False).shift(-1)
        allTrdDates = pd.DataFrame(DataReader.calendar._tradeDates, columns=[alf.DATE])
        allTrdDates['buyDate'] = allTrdDates[alf.DATE].shift(-1)   # 实际指数交易日 对应的买入日
        # 提取 shift date
        for num in allNums:
            if num > 1:
                allTrdDates['sellDate{}'.format(num)] = allTrdDates[alf.DATE].shift(-num)
                retsData['sellDate{}'.format(num)] = retsData['currDate'].groupby(level=alf.STKCD,sort=False).shift(-num)
        allTrdDates.set_index(alf.DATE, inplace=True)
        # 计算收益率 有股票日期点 对应的收益率算出来是错误的 用以上矫正的日期过滤掉
        for rtype in retTypes:
            for num in retTypes[rtype]:
                retName = ''.join([rtype, 'Day{}'.format(num)])
                # 第一天收益率
                netval = 1 + retsData['{}Ret'.format('OC' if rtype in ('OC','OO') else rtype)].groupby(level=alf.STKCD, sort=False).shift(-1)
                netval.columns = [retName]
                if num > 1:
                    # 最后一天收益率
                    sellNet = 1 + retsData['{}Ret'.format('CC' if rtype in ('OC','CC') else 'CO')].groupby(level=alf.STKCD, sort=False).shift(-num)
                    sellNet.columns = [retName]
                    netval = netval*sellNet
                    # 期间收益率
                    if num > 2:     # 计算期间收益，都是 CC ret
                        lag1CCret = retsData['CCRet'].groupby(level=alf.STKCD, sort=False).shift(-1)
                        betweenNet = (1+lag1CCret).groupby(level=alf.STKCD, sort=False, group_keys=False).rolling(window=num-2,min_periods=0).apply(np.prod,raw=True)
                        betweenNet.columns = [retName]
                        netval = netval*betweenNet
                retsData[retName] = netval - 1
        if 'OODay1' in retsData.columns:
            self.logger.warning('Open to open return has no 1 day ret')
            retsData.drop('OODay1', inplace=True, axis=1)
        # 对计算好的收益率 进行过滤
        retsData = retsData.join(allTrdDates, rsuffix='Act')
        invalidBuy = (retsData['buyDate'] > retsData['buyDateAct']) | retsData['NOTRADE'].groupby(level=alf.STKCD,sort=False).shift(-1).fillna(True)
        noTrades = {}
        for num in allNums:         # 计算各个日期天数对应的无效标记
            if num==1:
                noTrades[num] = invalidBuy
            else:
                notrd = retsData[['NOTRADE']].groupby(level=alf.STKCD, sort=False, as_index=False, group_keys=False).rolling(window=num, min_periods=0).sum() > 0
                notrd = notrd['NOTRADE'].groupby(level=alf.STKCD, sort=False).shift(-num).fillna(True)
                noTrades[num] = notrd
        retNamesOut = []
        for rtype in retTypes:
            for num in retTypes[rtype]:
                retName = ''.join([rtype, 'Day{}'.format(num)])
                if retName == 'OODay1':
                    continue
                retsData.loc[noTrades[num], retName] = np.nan
                retNamesOut.append(retName)
        print('return data taken with {0} seconds from {1}'.format(time.time() - start, 'mysql' if fromMysql else 'h5'))
        retsData.sort_values([alf.DATE,alf.STKCD],inplace=True)
        return retsData.loc[(slice(headDate,tailDate),slice(None)),retNamesOut]

    def get_filterX(self, headDate=None, tailDate=None, dateList=None, stkList=None, selectType='CloseClose'):
        """
        提取 特征信息 对应的过滤
        买入日 停牌、涨停 导致的无法买入 （注：卖出日跌停无法预知，因而虽然会导致无法卖出但不应筛选掉）
        :return:    true 为应该被过滤掉
        """
        filterFields = [als.NOTRD, als.PNOTRD, als.ISST, als.LMUP, als.LMDW, als.INSFAMT, als.INSFTRD, als.INSFLST, als.INSFRSM]
        filterData = self.get_data(headDate=headDate,
                                   tailDate=tailDate,
                                   dateList=dateList,
                                   stkList=stkList,
                                   selectType=selectType,
                                   fields=filterFields)
        filterX = filterData.any(axis=1)
        return filterX

    def get_filterY(self, headDate=None, tailDate=None, dateList=None, stkList=None, selectType='CloseClose'):
        """
        提取 收益率 对应的过滤 ： 只提取交易行为性质的过滤
        买入日 涨停
        :return:
        """
        responseFields = ['COLIMITUP', 'COLIMITDOWN', 'CCLIMITUP', 'CCLIMITDOWN']




if __name__=='__main__':
    obj = DataReader()

    # t = obj.get_data(headDate=None,
    #                  tailDate=None,
    #                  stkList=['000001.SZ','000002.SZ','000004.SZ'],
    #                  fields=[alf.OPEN,alf.HIGH,alf.LOW],
    #                  tableName=None,
    #                  dbName=None)
    #
    # print(t.shape)

    # t = obj.get_responses(headDate=20160101,tailDate=None,retTypes={'OC': [1], 'OO': [2], })
    # t.to_csv('returns.csv')

    # t = obj.get_data(headDate=20160517, tailDate=20180518, fields = [als.NOTRD])
    # t.to_csv('notrd.csv')

    # t1 = obj.get_data(headDate=20170901,
    #                   tailDate=20180301,
    #                   stkList=['000002.SZ','000003.SZ','000004.SZ'],
    #                   fields=[alf.OPEN,alf.HIGH,alf.CLOSE],
    #                   tableName=None,
    #                   dbName=None,
    #                   fromMysql=False)
    # t2 = obj.get_data(headDate=20170901,
    #                   tailDate=20180301,
    #                   stkList=['000002.SZ','000003.SZ','000004.SZ'],
    #                   fields=[alf.OPEN,alf.HIGH,alf.CLOSE],
    #                   tableName=None,
    #                   dbName=None,
    #                   fromMysql=True)
    # print(t1-t2)

    # t = obj.get_responses(headDate=20170801,
    #                       tailDate=20180201,
    #                       retTypes={'OC': [1], 'CC': [2], })
    # print(t)