
import cx_Oracle
import mysql.connector
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
import sqlalchemy.types as st

import os
import sys
import time
import configparser as cp
import datetime as dt
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


import numpy as np
import pandas as pd

from HelpModules.Logger import Logger
from HelpModules.DataConnector import DataConnector
from DataReaderModule.Constants import rootPath
from DataLocalizeModule.ConstantsDB import WPFX, DatabaseNames, TableNames, FieldNames
from DataLocalizeModule.ConstantsDB import FieldTypeDict, TableFieldsDict


class UpdaterOrigin:
    """
    从wind数据库更新至 本地 mysql 数据库
    保持字段名、表明 和wind库相同，时相同代码可以在两个库运行
    """

    def __init__(self, config=None):
        config = os.path.join(rootPath,'Configs','loginInfo.ini') if config is None else config
        cfp = cp.ConfigParser()
        cfp.read(config)
        loginfoWind = dict(cfp.items('Wind'))
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connWind = cx_Oracle.connect(r'{user}/{password}@{host}/{database}'.format(**loginfoWind))
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'.format(**loginfoMysql))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'],
                                                     database=loginfoMysql['database'])
        logPath = os.path.join(rootPath,'DataLocalizeModule','log')
        self.logger = Logger(logPath=logPath).get_logger(loggerName=__name__, logName='update_local_database_origin')
        self.logger.info('')
        self.dataConnector = DataConnector(logger=self.logger)

    def has_table(self, tableName, dbName=None, caseSens=False):
        """
        检查本地 mysql 数据库中是否具有 tableName 表格
        :param tableName:
        :return:
        """
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        mysqlCursor.execute('SHOW TABLES')
        if caseSens:
            allTables = [tb[0] for tb in mysqlCursor.fetchall()]
        else:
            tableName = tableName.lower()
            allTables = [tb[0].lower() for tb in mysqlCursor.fetchall()]
        return tableName in allTables

    def get_last_update(self, tableName, dbName=None, retType='obsNum',):
        """
        :param retType:  obsNum 保存的观测值数量  trdDate 最后的交易日期
        :return:
        """
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        mysqlCursor = self.connMysqlRead.cursor()
        if self.has_table(tableName=tableName, dbName=dbName):
            if retType == 'obsNum':
                mysqlCursor.execute('SELECT COUNT(*) FROM {0}'.format(tableName))
            elif retType in ('trdDate', 'annDate'):
                dateType = FieldNames.DATE if retType=='trdDate' else 'ANN_DT'
                mysqlCursor.execute('SELECT MAX({0}) FROM {1}'.format(dateType, tableName))
            else:
                raise NotImplementedError
            return mysqlCursor.fetchall()[0][0]
        else:
            self.logger.info('Table {0} does not exist in db {1}, will be created \n'.format(tableName,dbName))
            return 0

    def period_split(self, headDate, tailDate, gapYear=3):
        """
        将 headDate 至 tailDate 之间的日期 按照 gapYear 进行划分
        :param headDate:
        :param tailDate:
        :param gap:
        :return:
        """
        fieldDict = {
            'table': TableNames.TRDDATES,
            'date': FieldNames.DATE,
            'head': headDate,
            'tail': tailDate
        }
        sqlLine = 'SELECT * FROM {table} WHERE {date}>={head} AND {date}<={tail}'.format(**fieldDict)
        allDatesDB = pd.read_sql(sql=sqlLine, con=self.connMysqlRead)
        dateYears = allDatesDB[FieldNames.DATE].apply(lambda x: int(x[:4]))
        yearDiff = dateYears.diff(1)
        yearDiff.iloc[0] = 1
        if gapYear==1:
            cutDates = allDatesDB[yearDiff > 0]
        else:
            cutDates = allDatesDB[((yearDiff.cumsum() % gapYear)==1) & (yearDiff>0)]
        cutDates = list(cutDates.values[:,0])
        lastDate = allDatesDB.values[-1][0]     # 注意 不一定是tailDate
        if lastDate > cutDates[-1]:
            cutDates.append(lastDate)
        return cutDates

    def update_basic_info(self):
        """
        更新 股票基本信息 以及 交易日期
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        preStkNum = self.get_last_update(tableName=TableNames.STKBASIC, retType='obsNum')
        # read from remote wind
        tableName = 'ASHAREDESCRIPTION'
        fields = TableFieldsDict[tableName]
        updateSQL = 'SELECT {0} FROM {1}.{2} WHERE S_INFO_LISTDATE IS NOT NULL'.format(','.join(fields), WPFX, tableName)
        stkInfo = pd.read_sql(sql=updateSQL, con=self.connWind)
        if stkInfo.shape[0]>preStkNum:
            stkInfo.sort_values(by=[FieldNames.STKCD], inplace=True)
            stkInfo.set_index([FieldNames.STKCD], inplace=True)
            try:
                pd.io.sql.to_sql(stkInfo,
                                 name=TableNames.STKBASIC,
                                 con=self.connMysqlWrite,
                                 if_exists='replace',
                                 chunksize=2000,
                                 dtype=FieldTypeDict)
                self.logger.info('{0} : stocks basics updated, {1} new stocks \n'.format(funcName, stkInfo.shape[0] - preStkNum))
            except BaseException as e:
                self.logger.error('{0} : stocks basics updated FAILED \n'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : No new stock baisc data to update, stock num now : {1} \n'.format(funcName, preStkNum))
        ######## 更新交易日期列表 ###########
        lastTrdDate = self.get_last_update(tableName=TableNames.TRDDATES, retType='trdDate')
        # read from remote wind
        tableName = 'ASHAREEODPRICES'
        updateSQL = 'SELECT DISTINCT TRADE_DT FROM {0}.{1} WHERE TRADE_DT > {2}'.format(WPFX, tableName, lastTrdDate)
        trdDates = pd.read_sql(sql=updateSQL, con=self.connWind)
        trdDates.sort_values(by=FieldNames.DATE, inplace=True)
        if not trdDates.empty:
            try:
                pd.io.sql.to_sql(trdDates,
                                 name=TableNames.TRDDATES,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 index=False,
                                 dtype=FieldTypeDict)
                self.logger.info(
                    '{0} : trade dates updated, {1} new dates \n'.format(funcName, trdDates.shape[0]))
            except BaseException as e:
                self.logger.error('{0} : trade dates updated FAILED \n'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : No new trade date to update, lastupdt {1} \n'.format(funcName, lastTrdDate))

    def update_trade_info(self):
        """
        更新基本交易数据
        :param dbName:
        :return:
        """
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        # 提取 wind 数据库 最新数据
        tableName = 'ASHAREEODPRICES'
        windCursor = self.connWind.cursor()
        windCursor.execute('SELECT MAX(TRADE_DT) FROM {0}.{1}'.format(WPFX, tableName))
        newAvailable = windCursor.fetchall()[0][0]
        latestDate = self.get_last_update(tableName=tableName, retType='trdDate')
        # dict to map
        statDict = {'交易': 0, 'XR': 1, 'XD': 2, 'DR': 3, 'N': 4, '停牌': 5, np.nan: 6, }
        #### 读取交易数据
        cutDates = self.period_split(headDate=latestDate, tailDate=newAvailable)
        if len(cutDates) >= 2:
            fields = TableFieldsDict[tableName]
            print(cutDates)
            updateType = 'Separatedly' if len(cutDates)>2 else 'Whole'
            self.logger.info('{0} updating {1} ...'.format(updateType, tableName))
            for dumi in range(1, len(cutDates)):
                updateSQL = 'SELECT {0} FROM {1}.{2} WHERE TRADE_DT>{3} AND TRADE_DT<={4}' \
                    .format(','.join(fields), WPFX, tableName,cutDates[dumi-1], cutDates[dumi])
                tradeInfo = pd.read_sql(sql=updateSQL, con=self.connWind)
                tradeInfo.sort_values(by=[FieldNames.DATE, FieldNames.STKCD], inplace=True)
                tradeInfo.set_index([FieldNames.DATE, FieldNames.STKCD], inplace=True)
                tradeInfo['S_DQ_PCTCHANGE'] = tradeInfo['S_DQ_PCTCHANGE'] / 100
                tradeInfo['S_DQ_TRADESTATUS'] = tradeInfo['S_DQ_TRADESTATUS'].map(statDict)
                try:
                    print('{0} : {1} obs to update'.format(funcName, tradeInfo.shape[0]))
                    pd.io.sql.to_sql(tradeInfo,
                                     name=tableName,
                                     con=self.connMysqlWrite,
                                     if_exists='replace' if (dumi==1) and (len(cutDates)>2) else 'append',
                                     chunksize=2000,
                                     dtype=FieldTypeDict)
                    self.logger.info('{0} : {1} obs updated with from {2} to {3} \n'
                                     .format(funcName, tradeInfo.shape[0], cutDates[dumi-1], cutDates[dumi] ))
                except BaseException as e:
                    mysqlCursor = self.connMysqlRead.cursor()
                    if dumi==1:
                        mysqlCursor.execute('DROP TABLE {}'.format(tableName))
                    else:
                        mysqlCursor.execute('DELETE FROM {0} WHERE TRADE_DT>={1}'.format(tableName, cutDates[dumi-1]))
                    self.connMysqlRead.commit()
                    self.logger.error('{0} : update failed， table cleaned \n'.format(funcName))
                    raise e
        else:
            self.logger.info('{0} : No new trade_info to update, lastupdt {1} \n'.format(funcName, latestDate))

    def update_eod_derived(self):
        """
        更新基本交易数据
        :param dbName:
        :return:
        """
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        # 提取 wind 数据库 最新数据
        tableName = 'ASHAREEODDERIVATIVEINDICATOR'
        windCursor = self.connWind.cursor()
        windCursor.execute('SELECT MAX(TRADE_DT) FROM {0}.{1}'.format(WPFX, tableName))
        newAvailable = windCursor.fetchall()[0][0]
        # latestDate = self.get_last_update(tableName=tableName, retType='trdDate')
        latestDate = self.dataConnector.get_last_update(tableName=tableName, isH5=True)
        #### 读取交易数据
        cutDates = self.period_split(headDate=latestDate, tailDate=newAvailable, gapYear=1)
        if len(cutDates) >= 2:
            print(cutDates)
            fields = TableFieldsDict[tableName]
            fieldTypeDict = {fld : st.FLOAT for fld in fields}
            fieldTypeDict[FieldNames.DATE] = st.VARCHAR(8)
            fieldTypeDict[FieldNames.STKCD] = st.VARCHAR(40)
            updateType = 'Separatedly' if len(cutDates)>2 else 'Whole'
            self.logger.info('{0} updating {1} ...'.format(updateType, tableName))
            for dumi in range(1, len(cutDates)):
                updateSQL = 'SELECT {0} FROM {1}.{2} WHERE TRADE_DT>{3} AND TRADE_DT<={4}' \
                    .format(','.join(fields), WPFX, tableName,cutDates[dumi-1], cutDates[dumi])
                tradeInfo = pd.read_sql(sql=updateSQL, con=self.connWind)
                tradeInfo.sort_values(by=[FieldNames.DATE, FieldNames.STKCD], inplace=True)
                tradeInfo.set_index([FieldNames.DATE, FieldNames.STKCD], inplace=True)
                tradeInfo = tradeInfo.fillna(np.nan)  # 处理数据中的空值 类型被dataframe存为 object
                try:
                    print('{0} : {1} obs to update'.format(funcName, tradeInfo.shape[0]))
                    # pd.io.sql.to_sql(tradeInfo,
                    #                  name=tableName,
                    #                  con=self.connMysqlWrite,
                    #                  if_exists='replace' if (dumi==1) and (len(cutDates)>2) else 'append',
                    #                  chunksize=2000,
                    #                  dtype=FieldTypeDict)
                    self.dataConnector.store_table(tableData=tradeInfo,
                                                   tableName=tableName,
                                                   if_exist='append',
                                                   isH5=True)
                    self.logger.info('{0} : {1} obs updated with from {2} to {3} \n'
                                     .format(funcName, tradeInfo.shape[0], cutDates[dumi-1], cutDates[dumi] ))
                except BaseException as e:
                    mysqlCursor = self.connMysqlRead.cursor()
                    if dumi==1:
                        mysqlCursor.execute('DROP TABLE {}'.format(tableName))
                    else:
                        mysqlCursor.execute('DELETE FROM {0} WHERE TRADE_DT>{1}'.format(tableName, cutDates[dumi-1]))
                    self.connMysqlRead.commit()
                    self.logger.error('{0} : update failed， table cleaned \n'.format(funcName))
                    raise e
        else:
            self.logger.info('{0} : No new derivative indicators to update, lastupdt {1} \n'.format(funcName, latestDate))

    def update_st_info(self):
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        tableName = 'ASHAREST'
        obsNum = self.get_last_update(tableName=tableName, retType='obsNum')
        # 从远程数据库读取 数量不多，全部都取出来
        fields = TableFieldsDict[tableName]
        updateSQL = 'SELECT {0} FROM {1}.{2}'.format(','.join(fields), WPFX, tableName)
        stInfo = pd.read_sql(sql=updateSQL, con=self.connWind)
        if stInfo.shape[0]>obsNum:
            try:
                print('{0} : {1} obs to update'.format(funcName, stInfo.shape[0]))
                pd.io.sql.to_sql(stInfo,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='replace',
                                 index=False,
                                 chunksize=2000,
                                 dtype=FieldTypeDict)
                self.logger.info('{0} : updated finished with {1} seconds'.format(funcName, time.time() - start))
            except BaseException as e:
                self.logger.error('{0} : update failed'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : No new data to update, obs num now: {1} \n'.format(funcName, obsNum))

    def update_holders_info(self, dbName=None):
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        tableName = 'ASHAREFLOATHOLDER'
        latestDate = self.get_last_update(tableName=tableName, retType='annDate')
        windCursor = self.connWind.cursor()
        updateSQL = ''.join(['SELECT S_INFO_WINDCODE, ANN_DT, COUNT(S_HOLDER_HOLDERCATEGORY) as TOT_HOLDERS,',
                             'sum(case when S_HOLDER_HOLDERCATEGORY = 1 then 1 else 0 end) as PERS_HOLDERS, ',
                             'sum(case when S_HOLDER_HOLDERCATEGORY = 2 then 1 else 0 end) as INST_HOLDERS, ',
                             'sum(S_HOLDER_QUANTITY) as TOT_QUANTITY, ',
                             'sum(case when S_HOLDER_HOLDERCATEGORY = 1 then S_HOLDER_QUANTITY else 0 end) as PERS_QUANTITY, ',
                             'sum(case when S_HOLDER_HOLDERCATEGORY = 2 then S_HOLDER_QUANTITY else 0 end) as INST_QUANTITY ',
                             'FROM {0}.{1} ',
                             'WHERE ANN_DT>{2} ',
                             'GROUP BY S_INFO_WINDCODE, ANN_DT']).format(WPFX,tableName,latestDate)
        windCursor.execute(updateSQL)
        fields = ['S_INFO_WINDCODE', 'ANN_DT', 'TOT_HOLDERS', 'PERS_HOLDERS', 'INST_HOLDERS', 'TOT_QUANTITY', 'PERS_QUANTITY', 'INST_QUANTITY']
        holdersInfo = pd.DataFrame(windCursor.fetchall(),columns=fields)
        if (not holdersInfo.empty) and (holdersInfo['ANN_DT'].max()>str(latestDate)):
            dataShape = holdersInfo.shape
            self.logger.info('{0} : {1} rows and {2} columns data extracted'.format(funcName, dataShape[0], dataShape[1]))
            holdersInfo.sort_values(by=['S_INFO_WINDCODE', 'ANN_DT'], inplace=True)
            holdersInfo['NEXT_ANN_DT'] = holdersInfo.loc[:,['S_INFO_WINDCODE','ANN_DT']].groupby(by=['S_INFO_WINDCODE']).shift(-1)
            holdersInfo.set_index(['S_INFO_WINDCODE', 'ANN_DT'], inplace=True)
            try:
                print('{0} : {1} obs to update'.format(funcName, holdersInfo.shape[0]))
                pd.io.sql.to_sql(holdersInfo,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype=FieldTypeDict)
                self.logger.info('{0} : {1} obs updated with {2} seconds'
                                 .format(funcName, holdersInfo.shape[0], time.time() - start))
            except BaseException as e:
                mysqlCursor = self.connMysqlRead.cursor()
                mysqlCursor.execute('DELETE FROM {0} WHERE ANN_DT>{1}'.format(tableName, latestDate))
                self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : No new data to update, last ann_dt {1} \n'.format(funcName, latestDate))

    def patch_next_anndt(self, tableName, dbName=None):
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        sqlLines = 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME="{0}"'.format(tableName, dbName)
        mysqlCursor.execute(sqlLines)
        fields = [fld[0] for fld in mysqlCursor.fetchall()]
        sqlLines = 'SELECT * FROM {} WHERE NEXT_ANN_DT IS NULL ORDER BY S_INFO_WINDCODE, ANN_DT'.format(tableName)
        mysqlCursor.execute(sqlLines)
        data = pd.DataFrame(mysqlCursor.fetchall(), columns=fields)
        data['NEXT_ANN_DT'] = data.loc[:, ['S_INFO_WINDCODE', 'ANN_DT']].groupby(by=['S_INFO_WINDCODE']).shift(-1)
        patchedData = data[~data['NEXT_ANN_DT'].isna()]
        for dumi in range(patchedData.shape[0]):
            row = patchedData.iloc[dumi]
            sql = 'UPDATE {0} SET NEXT_ANN_DT={1} WHERE S_INFO_WINDCODE="{2}" AND ANN_DT={3}'.\
                format(tableName, row['NEXT_ANN_DT'],row['S_INFO_WINDCODE'],row['ANN_DT'])
            mysqlCursor.execute(sql)
        self.connMysqlRead.commit()
        self.logger.info('{0} obs patched in table {1} \n'.format(patchedData.shape[0], tableName))


    ############### 类似 此类带有 anndt 的数据可以集合到 财务更新模块， 目前暂时有些问题 ###
    def update_shares_info(self, dbName=None):
        """
        更新 shares
        :param dbName:
        :return:
        """
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        tableName = 'ASHARECAPITALIZATION'
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        if self.has_table(tableName=tableName, dbName=dbName):
            mysqlCursor.execute('SELECT MAX(ANN_DT) FROM {}'.format(tableName))
            latestDate = mysqlCursor.fetchall()[0][0]
        else:
            latestDate = 0
            self.logger.info('{0} : table {1} does not exist in db {2}, will be created'.
                             format(funcName,tableName,dbName))
        # 从wind远程读取
        windCursor = self.connWind.cursor()
        fields = ['S_INFO_WINDCODE', 'ANN_DT', 'CHANGE_DT', 'TOT_SHR', 'FLOAT_SHR', 'FLOAT_A_SHR', 'S_SHARE_TOTALA']
        updateSQL = 'SELECT {0} FROM {1}.{2} WHERE ANN_DT>{3}'.format(','.join(fields), WPFX, tableName,latestDate)
        windCursor.execute(updateSQL)
        shareInfo = pd.DataFrame(windCursor.fetchall(),columns=fields)
        if (not shareInfo.empty) and (shareInfo['ANN_DT'].max()>str(latestDate)):
            dataShape = shareInfo.shape
            self.logger.info('{0} : {1} rows and {2} columns data extracted'.format(funcName, dataShape[0], dataShape[1]))
            shareInfo.sort_values(by=['S_INFO_WINDCODE', 'ANN_DT', 'CHANGE_DT'], inplace=True)
            shareInfo['NEXT_ANN_DT'] = shareInfo.loc[:,['S_INFO_WINDCODE','ANN_DT']].groupby(by=['S_INFO_WINDCODE']).shift(-1)
            shareInfo.set_index(['S_INFO_WINDCODE', 'ANN_DT'], inplace=True)
            try:
                print('{0} : {1} obs to update...'.format(funcName, shareInfo.shape[0]))
                pd.io.sql.to_sql(shareInfo,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype=FieldTypeDict)
                self.logger.info('{0} : {1} obs updated with {2} seconds \n'
                                 .format(funcName, shareInfo.shape[0], time.time() - start))
            except BaseException as e:
                # mysqlCursor.execute('DELETE FROM {0} WHERE ANN_DT>{1}'.format(tableName, latestDate))
                # self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned \n'.format(funcName))
                raise e
            self.patch_next_anndt(tableName=tableName, dbName=dbName)
        else:
            self.logger.info('{0} : No new data to update \n'.format(funcName))



if __name__=='__main__':

    obj = UpdaterOrigin()

    obj.update_basic_info()
    obj.update_trade_info()
    obj.update_eod_derived()
    obj.update_st_info()

    # obj.update_holders_info()
    # obj.patch_next_anndt(tableName='ASHAREFLOATHOLDER')

    # obj.update_shares_info()
    # obj.patch_next_anndt(tableName='ASHARECAPITALIZATION')
