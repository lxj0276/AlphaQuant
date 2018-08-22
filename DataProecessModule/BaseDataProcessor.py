__author__ = 'wangjp'

import os
import sys
import time
import datetime as dt

import numpy as np
import pandas as pd
import configparser as cp

import mysql.connector
from sqlalchemy import create_engine
import sqlalchemy.types as sqltp
import pymysql
pymysql.install_as_MySQLdb()

from HelpModules.Logger import Logger
from HelpModules.Calendar import Calendar
from DataReaderModule.Constants import *
from DataReaderModule.DataReader import DataReader



class BaseDataProcessor:
    """
    在基础数据的基础上 生成因子检验、模型训练所需的 中间数据 如过滤条件等
    """

    def __init__(self, basePath=None):
        if basePath is None:
            basePath = os.path.join(rootPath, 'BaseDataProcessor')
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(basePath, 'configs', 'loginInfo.ini'))
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'
                                            .format(**loginfoMysql))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.dbName = DatabaseNames.MysqlDaily
        # create logger
        self.logger = Logger(logPath=os.path.join(basePath,'log')).get_logger(logName='data_processor',loggerName=__name__)
        self.logger.info('')
        # create calendar
        self.calendar = Calendar()
        # create reader
        self.dataReader  = DataReader(basePath=None, connectRemote=False)

    def update_stock_count(self):
        """
        计算每日 上市的 股票数量，包含停牌
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        tableName = ALIAS_TABLES.DAILYCNT
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(self.dbName))
        mysqlCursor.execute('SHOW TABLES')
        # check last update
        allTables = [tb[0].upper() for tb in mysqlCursor.fetchall()]
        if tableName.upper() not in allTables:
            lastUpdt = 0
        else:
            mysqlCursor.execute('SELECT MAX({0}) FROM {1}'.format(ALIAS_FIELDS.DATE, tableName))
            lastUpdt = mysqlCursor.fetchall()[0][0]
        # 查看最新数据进度
        t = ALIAS_FIELDS
        mysqlCursor.execute('SELECT MAX({0}) FROM {1}'.format(t.DATE, ALIAS_TABLES.TRDDATES))  # 通过日期表查询 速度较快
        availableDate = mysqlCursor.fetchall()[0][0]
        # extract base data
        if availableDate > str(lastUpdt):
            baseFields = [t.TRDSTAT]
            baseData = self.dataReader.get_data(headDate=lastUpdt,
                                                tailDate=None,
                                                stkList=None,
                                                fields=baseFields,
                                                dbName=self.dbName,
                                                useCache=False,
                                                selectType='OpenClose',
                                                )
            procData = baseData.groupby(by=t.DATE).count()
            procData.columns = [t.STKCNT]
            datesToUpdate = procData.index.values
            try:  # 写入数据库
                self.logger.info('{0} writing daily stocks count into database , from {1} to {2} to write...'
                                 .format(funcName, datesToUpdate[0], datesToUpdate[-1]))
                pd.io.sql.to_sql(procData,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype={t.STKCNT: sqltp.INT, t.DATE: sqltp.VARCHAR(8)})
                self.logger.info('{0} : Response Filter updated, from {1} to {2} updated'
                                 .format(funcName, datesToUpdate[0], datesToUpdate[1]))
            except Exception as e:
                if lastUpdt == 0:
                    mysqlCursor.execute('DROP TABLE {}'.format(tableName))
                else:
                    mysqlCursor.execute(
                        'DELETE FROM {0} WHERE {1}>{2}'.format(tableName, t.DATE, lastUpdt))  # 清理不能包含 lastupdt
                self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : no new data to update, last update {1}'.format(funcName, lastUpdt))

    def update_features_filter(self):
        """
        生成 特征 过滤条件 目前包括：
                    当日停牌
                    前日停牌 : 即复牌后第一天 不发信号
                    当日ST    注：ST还包含L,X,P,Z等，因这些状态也不会交易，故统一按照ST处理
                    当日涨停
                    当日跌停
                    成交额不足
                    上市不满100个交易日
                    因重组等原因导致停牌后，复牌不足100个交易日 新重组的股票按照新股对待
                    最近100个（指数）交易日中交易天数小于50，注： 股票数据的交易日，可能存在数据空缺
        注 ： 当日 指 info ate
        注 ： 涨跌停板制度是1996年12月13日发布、1996年12月16日开始实施的 此前标记的涨跌停 可能不准确
        :param dateList: None 则提取全部交易日期
        :param stkList:  None 则提取全部股票代码
        :return:
        """
        s = time.time()
        funcName = sys._getframe().f_code.co_name
        tableName = ALIAS_TABLES.XFILTER
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(self.dbName))
        mysqlCursor.execute('SHOW TABLES')
        # check last update
        allTables = [tb[0].upper() for tb in mysqlCursor.fetchall()]
        if tableName.upper() not in allTables:
            lastUpdt = 0
        else:
            mysqlCursor.execute('SELECT MAX({0}) FROM {1}'.format(ALIAS_FIELDS.DATE, tableName))
            lastUpdt = mysqlCursor.fetchall()[0][0]
        # 查看最新数据进度
        t = ALIAS_FIELDS
        mysqlCursor.execute('SELECT TRADE_DT FROM {0} WHERE TRADE_DT>{1}'.format(ALIAS_TABLES.TRDDATES, lastUpdt))   # 通过日期表查询 速度较快
        newTradeDates = pd.DataFrame(mysqlCursor.fetchall(), columns=[t.DATE])
        # extract base data
        if not newTradeDates.empty:    # 有新数据 可更新
            cutDate = self.calendar.tdaysoffset(num=-100, currDates=lastUpdt)  # 计算需要提前一定长度的数据, 注意下面取数据是 左开右闭 方式
            baseFields = [t.PCTCHG, t.AMOUNT, t.TRDSTAT, t.STSTAT]
            baseData = self.dataReader.get_data(headDate=cutDate,
                                                tailDate=None,
                                                stkList=None,
                                                fields=baseFields,
                                                dbName=self.dbName,
                                                useCache=False,
                                                selectType='CloseClose' if lastUpdt==0 else 'OpenClose')  # 不包含 lastupdt
            # 计算 filter
            baseData['NOTRADE'] = baseData[t.TRDSTAT].isin([5, 6])   # 停牌
            baseData['PRENOTRADE'] = baseData[t.TRDSTAT].groupby(level=t.STKCD).shift(1).isin([5, 6])  # 前一日停牌
            baseData['ISST'] = baseData[t.STSTAT]                   # ST
            baseData['LIMITUP'] = baseData[t.PCTCHG]>=0.099         # 涨停
            baseData['LIMITDOWN'] = baseData[t.PCTCHG]<=-0.099      # 跌停
            baseData['INSFAMT'] = baseData[t.AMOUNT]<=20000000/1000      # 成交额不足两千万 成交额 单位 千元
            baseData['listcnt'] = 1     # 只要有数据就是listed,还未退市
            baseData['INSFLIST'] = baseData['listcnt'].groupby(level=t.STKCD).cumsum()<100  # 上市不足100个交易日, 可以过滤掉新股上市首日
            baseData['HASTRADE'] = ~baseData['NOTRADE']
            baseData['INSFTRADE'] = baseData[['HASTRADE']].groupby(level=t.STKCD,as_index=False,group_keys=False,sort=False)\
                                        .rolling(window=100, min_periods=0).sum()<50  # 最近100个交易日中交易天数小于50
            # 复牌不足100个交易日的重组股等
            newTradeDates['REAL_SHIFT'] = newTradeDates[t.DATE].shift(100)
            newTradeDates.set_index(t.DATE, inplace=True)
            baseData = baseData.join(newTradeDates, how='left')
            baseData.reset_index(inplace=True)
            baseData['DATA_SHIFT'] = baseData.groupby(by=t.STKCD,as_index=False,sort=False).shift(100)[t.DATE]
            baseData['INSFRESUM'] = baseData['DATA_SHIFT'] < baseData['REAL_SHIFT']
            baseData.sort_values(by=[t.DATE, t.STKCD], inplace=True)
            baseData.set_index([t.DATE, t.STKCD], inplace=True)
            filterColumns = ['NOTRADE', 'PRENOTRADE', 'ISST', 'LIMITUP', 'LIMITDOWN', 'INSFAMT','INSFTRADE','INSFLIST','INSFRESUM']
            filterTypes = {col : sqltp.BOOLEAN for col in filterColumns}
            filterTypes[t.DATE] = sqltp.VARCHAR(8)
            filterTypes[t.STKCD] = sqltp.VARCHAR(40)
            outDates = baseData.index.levels[0].values      # baseData的 所有日期
            newTail = np.max(outDates)
            newHead = np.min(outDates[outDates>str(lastUpdt)])
            baseData = baseData.loc[(slice(newHead,newTail),slice(None)), filterColumns]
            dataShape = baseData.shape
            try:    # 写入数据库
                self.logger.info('{0} writing Features Filter into database , {1} rows and {2} cols to write...'
                                 .format(funcName,dataShape[0], dataShape[1]))
                pd.io.sql.to_sql(baseData,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype=filterTypes)
                self.logger.info('{0} : Features Filter updated, with {1} rows and {2} cols'
                                 .format(funcName, dataShape[0], dataShape[1]))
            except Exception as e:
                if lastUpdt==0:
                    mysqlCursor.execute('DROP TABLE {}'.format(tableName))
                else:
                    mysqlCursor.execute('DELETE FROM {0} WHERE {1}>{2}'.format(tableName, t.DATE, lastUpdt))    # 清理不能包含 lastupdt
                self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : no new data to update, last update {1}'.format(funcName, lastUpdt))
        print(time.time() - s)


    def update_response_filter(self):
        """
        生成 response 过滤条件：
        开盘（考虑买入）  收盘（考虑卖出)
        response 类型： 1日  停牌：1        无法交易 停牌优先级高于ST
                            开盘涨停：2    无法买入
                            开盘跌停：3
                            收盘涨停：4
                            收盘跌停：5    无法卖出
                            ST：6         不应买入
                            注：ST还包含L,X,P,Z等，因这些状态也不会交易，故统一按照ST处理
                                       ST本身标记优先级 高于ST涨跌停等
                                       因ST 状态通常提前被公布，因而可以过滤
        注 ： 涨跌停板制度是1996年12月13日发布、1996年12月16日开始实施的 此前标记的涨跌停 可能不准确
        :return:
        """
        s = time.time()
        funcName = sys._getframe().f_code.co_name
        tableName = ALIAS_TABLES.YFILTER
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(self.dbName))
        mysqlCursor.execute('SHOW TABLES')
        # check last update
        allTables = [tb[0].upper() for tb in mysqlCursor.fetchall()]
        if tableName.upper() not in allTables:
            lastUpdt = 0
        else:
            mysqlCursor.execute('SELECT MAX({0}) FROM {1}'.format(ALIAS_FIELDS.DATE, tableName))
            lastUpdt = mysqlCursor.fetchall()[0][0]
        # 查看最新数据进度
        t = ALIAS_FIELDS
        mysqlCursor.execute('SELECT MAX({0}) FROM {1}'.format(t.DATE, ALIAS_TABLES.TRDDATES))      # 通过日期表查询 速度较快
        availableDate = mysqlCursor.fetchall()[0][0]
        # extract base data
        if availableDate > str(lastUpdt):
            baseFields = [t.PCTCHG, t.OPEN, t.CLOSE, t.TRDSTAT, t.STSTAT]
            baseData = self.dataReader.get_data(headDate=lastUpdt,
                                                tailDate=None,
                                                stkList=None,
                                                fields=baseFields,
                                                dbName=self.dbName,
                                                useCache=False,
                                                selectType='CloseClose' if lastUpdt==0 else 'OpenClose',
                                                )
            baseData['CCRet'] = baseData[t.PCTCHG]
            baseData['OCRet'] = (baseData[t.CLOSE]/baseData[t.OPEN] - 1)    # 当天 开盘到收盘 收益
            baseData['CORet'] = ((1 + baseData['CCRet'])/(1 + baseData['OCRet']) - 1)  # 当日开盘 到 前一日收盘 收益
            baseData['NOTRADE'] = baseData[t.TRDSTAT].isin([5, 6])
            baseData['ISST'] = baseData[t.STSTAT]
            baseData['COLIMITUP'] = (baseData['CORet'] >= 0.099) & (~baseData['ISST']) | (baseData['CORet'] >= 0.049) & (baseData['ISST'])     # 开盘涨停
            baseData['COLIMITDOWN'] = (baseData['CORet'] <= -0.099) & (~baseData['ISST']) | (baseData['CORet'] <= -0.049) & (baseData['ISST'])  # 开盘跌停
            baseData['CCLIMITUP'] = (baseData['CCRet'] >= 0.099) & (~baseData['ISST']) | (baseData['CORet'] >= 0.049) & (baseData['ISST'])  # 收盘涨停
            baseData['CCLIMITDOWN'] = (baseData['CCRet'] <= -0.099) & (~baseData['ISST']) | (baseData['CCRet'] <= -0.049) & (baseData['ISST'])  # 收盘跌停
            baseData.sort_values(by=[t.DATE, t.STKCD], inplace=True)
            filterColumns = ['OCRet', 'CORet', 'CCRet', 'NOTRADE', 'ISST', 'COLIMITUP', 'COLIMITDOWN', 'CCLIMITUP', 'CCLIMITDOWN']
            filterTypes = {col : sqltp.BOOLEAN for col in filterColumns[3:]}
            filterTypes['OCRet'] = sqltp.FLOAT
            filterTypes['CORet'] = sqltp.FLOAT
            filterTypes['CCRet'] = sqltp.FLOAT
            filterTypes[t.DATE] = sqltp.VARCHAR(8)
            filterTypes[t.STKCD] = sqltp.VARCHAR(40)
            dataShape = baseData.shape
            try:    # 写入数据库
                self.logger.info('{0} writing Response Filter into database , {1} rows and {2} cols to write...'
                                 .format(funcName, dataShape[0], dataShape[1]))
                pd.io.sql.to_sql(baseData.loc[:, filterColumns],
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype=filterTypes)
                self.logger.info('{0} : Response Filter updated, with {1} rows and {2} cols'
                                 .format(funcName, dataShape[0], dataShape[1]))
            except Exception as e:
                if lastUpdt == 0:
                    mysqlCursor.execute('DROP TABLE {}'.format(tableName))
                else:
                    mysqlCursor.execute('DELETE FROM {0} WHERE {1}>{2}'.format(tableName, t.DATE, lastUpdt))        # 清理不能包含 lastupdt
                self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : no new data to update, last update {1}'.format(funcName, lastUpdt))
        print(time.time() - s)



if __name__=='__main__':
    obj = BaseDataProcessor()
    obj.update_stock_count()
    obj.update_features_filter()
    obj.update_response_filter()