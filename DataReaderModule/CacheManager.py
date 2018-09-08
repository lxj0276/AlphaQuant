#coding=utf8
__author__ = 'wangjp'

import os
import time
import datetime as dt
import configparser as cp

import numpy as np
import pandas as pd
import mysql.connector

from HelpModules.Logger import Logger
from HelpModules.Calendar import Calendar
from DataReaderModule.Constants import CacheLevlels, ALIAS_FIELDS, ALIAS_TABLES,DatabaseNames,rootPath

class CacheManager:

    _tableSaved = {}     # 用于存储缓存的数据表 key 为表名 ： dataFrame (以 trade_dt, stkcd 为 index)
    _fieldsSaved = {}    # 用于记录已经缓存的数据的 字段名 key 为表名 ：[field1, field2, ...]
    _fieldsIndex = {}
    _fieldStkCnt = {}
    _calendar = None
    _stockCounts = None   # 存储当前所有的 日期 股票 对组合

    def __init__(self, basePath=None, cacheLevel='Level1',dateSource='h5'):
        if basePath is None:
            basePath = os.path.join(rootPath, 'DataReaderModule')
        cfp = cp.ConfigParser()
        self.dateSource = dateSource
        if self.dateSource=='mysql':
            cfp.read(os.path.join(rootPath, 'Configs', 'loginInfo.ini'))
            loginfoMysql = dict(cfp.items('Mysql'))
            self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                         password=loginfoMysql['password'],
                                                         host=loginfoMysql['host'])
        else:
            cfp.read(os.path.join(rootPath, 'Configs', 'dataPath.ini'))
            self.h5File = os.path.join(cfp.get('data','h5'),'{}.h5'.format(ALIAS_TABLES.DAILYCNT))
        self._cacheLevel = cacheLevel   # 可以被缓存的最低级别
        self.logger = Logger(logPath=os.path.join(basePath, 'log')).get_logger(loggerName=__name__, logName='cache_manager')
        self.logger.info('')
        self._cacheCalendar()
        self._cacheStockCounts()
        self._nan_rep = 10      # 用于替换数据中 允许存在的 NaN 避免stock count 时出现短缺

    def _cacheCalendar(self):
        if CacheManager._calendar is None:
            CacheManager._calendar = Calendar()
            self.logger.info('Calendar cached')

    def _cacheStockCounts(self):
        if CacheManager._stockCounts is None:
            if self.dateSource=='mysql':
                mysqlCursor = self.connMysqlRead.cursor()
                mysqlCursor.execute('USE {}'.format(DatabaseNames.MysqlDaily))
                sqlLines = 'SELECT {0},{1} FROM {2}'.format(ALIAS_FIELDS.DATE,ALIAS_FIELDS.STKCNT,ALIAS_TABLES.DAILYCNT)
                stkCounts = pd.read_sql(con=self.connMysqlRead, sql=sqlLines)
                stkCounts.sort_values(by=ALIAS_FIELDS.DATE, inplace=True)
                stkCounts.set_index(ALIAS_FIELDS.DATE, inplace=True)
            else:
                stkCounts = pd.read_hdf(path_or_buf=self.h5File,
                                        key=ALIAS_TABLES.DAILYCNT,
                                        mode='r')
            CacheManager._stockCounts = stkCounts
            self.logger.info('Stock counts cached')

    def checkinCache(self, tableName, tableData, sort=True):
        """
        对tableData进行检查，符合条件的数据将被加入缓存
        :param tableName:
        :param tableData:r
        :return:
        """
        if tableData.empty:
            self.logger.warning('given table {0} is empty, will not be cached!'.format(tableName))
            return
        indexFields = [ALIAS_FIELDS.DATE, ALIAS_FIELDS.STKCD]
        tableName = tableName.upper()
        cacheCandidable = False
        for level in CacheLevlels:
            if tableName in CacheLevlels[level]:
                cacheCandidable = True
                if level<=self._cacheLevel: # 符合缓存级别的数据，将被缓存
                    self.logger.info('{0} : cache level {1} , will be saved in cache'.format(tableName, level))
                    dataIndex = tableData.index.values
                    dataFields = [col for col in tableData.columns if col not in indexFields]   # by col
                    if tableName in ('RESPONSE',):      # 处理 response 中， 允许存在的 NaN 的情况
                        tableData = tableData.fillna(self._nan_rep)
                    stockCounts = tableData.groupby(level=ALIAS_FIELDS.DATE).count()
                    if tableName not in self._tableSaved:   # 该表格数据第一次被缓存
                        self._tableSaved[tableName] = tableData
                        self._fieldsSaved[tableName] = dataFields
                        self._fieldsIndex[tableName] = {fld : dataIndex for fld in dataFields}
                        self._fieldStkCnt[tableName] = {fld : stockCounts.loc[:,[fld]] for fld in dataFields}
                        self.logger.info('NEW Table {0} cached, Fields {1} with {2} obs'
                                         .format(tableName, ','.join(dataFields), tableData.shape[0]))
                        if sort:    # 将缓存的数据进行 sort
                            self._tableSaved[tableName].sort_values(by=indexFields, inplace=True)
                            self.logger.info('table {0} in cache is sorted'.format(tableName))
                    else:   # 数据表 被缓存过， 需要查看已经缓存的字段
                        commonFields = [fld for fld in dataFields if fld in self._fieldsSaved[tableName]]
                        newFields = [fld for fld in dataFields if fld not in self._fieldsSaved[tableName]]
                        if newFields:   # 如果有新字段，则先将已有表格与新字段join 可能导致现有表格 index 变长
                            self._tableSaved[tableName] = self._tableSaved[tableName].join(other=tableData.loc[:, newFields], how='outer')
                            self._fieldsSaved[tableName].extend(newFields)
                            self._fieldsIndex[tableName].update({fld: dataIndex for fld in newFields})       ### check !!!
                            self._fieldStkCnt[tableName].update({fld: stockCounts.loc[:,[fld]] for fld in newFields})
                            self.logger.info('NEW Fields {0} cached in table {1} with {2} obs'
                                             .format(','.join(newFields), tableName, tableData.shape[0]))
                        if commonFields:   # 对已有字段， 需要检查字段 index 差异
                            for fld in commonFields:   # 需要逐个字段检查，因为缓存的各个字段长度可能不一样
                                savedIndex = self._fieldsIndex[tableName][fld]      # 当前存储的表格中 该字段存在 切 数值正确 的index
                                extraIndex = list(set(dataIndex) - set(savedIndex)) # 新表格提供的 数值正确的 index
                                notSavedExtraIndex = list(set(extraIndex) - set(self._tableSaved[tableName].index.values))  # 数值真确的新index中， 当前表格还未存储的
                                hasSavedExtraIndex = list(set(self._tableSaved[tableName].index.values) - set(savedIndex))  # 数值真确的新index中， 当前表格已经存储，但是数值不正确
                                if notSavedExtraIndex:  # 需要延长现存表
                                    self._tableSaved[tableName] = pd.concat([tableData.loc[notSavedExtraIndex, [fld]],
                                                                             self._tableSaved[tableName]]
                                                                            , axis=0, sort=False)
                                    self._tableSaved[tableName].loc[hasSavedExtraIndex, [fld]] = tableData.loc[hasSavedExtraIndex, fld]
                                else:   # 直接修改现存表
                                    self._tableSaved[tableName].loc[extraIndex, [fld]] = tableData.loc[extraIndex, fld]
                                self._fieldsIndex[tableName][fld] = self._tableSaved[tableName].index.values
                                self._fieldStkCnt[tableName][fld] = self._tableSaved[tableName].loc[self._fieldsIndex[tableName][fld], [fld]].groupby(level=ALIAS_FIELDS.DATE).count()
                                self.logger.info('Field {0} in table {1} filled with {2} extra obs'
                                                 .format(fld, tableName, len(extraIndex)))
                        if sort:
                            self._tableSaved[tableName].sort_values(by=indexFields, inplace=True)
                            self.logger.info('table {0} in cache is sorted'.format(tableName))
                else:   # 数据表 未达到缓存级别，将不会被缓存
                    self.logger.info('{0} : cache level {1} , low level will not be saved'.format(tableName, level))
                break   # 已找到当前表格所属level 不必继续查找了
        if not cacheCandidable:     # 非可缓存数据
            self.logger.info('{0} : not cache candidable ,will not be saved'.format(tableName))
        tbShape = self._tableSaved[tableName].shape
        self.logger.info('Table {0} shape after cache : {1} rows and {2} cols'.format(tableName, tbShape[0], tbShape[1]))

if __name__=='__main__':

    obj = CacheManager()
    obj._cacheStockCounts()

    # conn = mysql.connector.connect(user='root', password='Alpha2018', host='127.0.0.1')
    # cursor = conn.cursor()
    # cursor.execute('SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_OPEN, S_DQ_CLOSE FROM testdb.ASHAREEODPRICES WHERE TRADE_DT>20180801 AND TRADE_DT<=20180805')
    # data = pd.DataFrame(cursor.fetchall(), columns=['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_OPEN', 'S_DQ_CLOSE'])
    # data.set_index(['TRADE_DT','S_INFO_WINDCODE'], inplace=True)
    # obj.checkinCache(tableName='ASHAREEODPRICES', tableData=data)
    #
    # cursor.execute('SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_OPEN FROM testdb.ASHAREEODPRICES WHERE TRADE_DT>20180807 AND TRADE_DT<=20180809')
    # data2 = pd.DataFrame(cursor.fetchall(), columns=['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_OPEN'])
    # data2.set_index(['TRADE_DT','S_INFO_WINDCODE'], inplace=True)
    # obj.checkinCache(tableName='ASHAREEODPRICES', tableData=data2)
    #
    # cursor.execute('SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_OPEN, S_DQ_HIGH, S_DQ_PCTCHANGE,S_DQ_VOLUME FROM testdb.ASHAREEODPRICES WHERE TRADE_DT>20180807 AND TRADE_DT<=20180810')
    # data3 = pd.DataFrame(cursor.fetchall(), columns=['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_OPEN', 'S_DQ_HIHG', 'S_DQ_PCTCHANGE','S_DQ_VOLUME'])
    # data3.set_index(['TRADE_DT','S_INFO_WINDCODE'], inplace=True)
    # obj.checkinCache(tableName='ASHAREEODPRICES', tableData=data3)

