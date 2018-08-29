
import cx_Oracle
import mysql.connector
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

import os
import sys
import time
import configparser as cp
import datetime as dt
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


import numpy as np
import pandas as pd

from HelpModules.Logger import Logger
from DataLocalizeModule.ConstantsDB import DatabaseNames,TableNames,FieldTypeDict,WPFX


class UpdaterOrigin:
    """
    从wind数据库更新至 本地 mysql 数据库
    保持字段名、表明 和wind库相同，时相同代码可以在两个库运行
    """

    def __init__(self, config=None):
        config = r'.\configs\loginInfo.ini' if config is None else config
        cfp = cp.ConfigParser()
        cfp.read(config)
        loginfoWind = dict(cfp.items('Wind'))
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connWind = cx_Oracle.connect(r'{user}/{password}@{host}/{database}'.format(**loginfoWind))
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'.format(**loginfoMysql))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.logger = Logger(logPath=r'D:\AlphaQuant\DataLocalizeModule\log').get_logger(loggerName=__name__, logName='update_local_database_origin')
        self.logger.info('')

    def has_table(self,tableName,dbName=None,caseSens=False):
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


    def update_basic_info(self, dbName=None):
        """
        更新 股票基本信息 以及 交易日期
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        ########### 更新股票基本信息列表 ##########
        if self.has_table(tableName=TableNames.STKBASIC, dbName=dbName):
            mysqlCursor.execute('SELECT COUNT(*) FROM {}'.format(TableNames.STKBASIC))
            preStkNum = mysqlCursor.fetchall()[0][0]
        else:
            preStkNum = 0
            self.logger.info('{0} : table {1} does not exist in db {2}, will be created'
                             .format(funcName, TableNames.STKBASIC, dbName))
        # read from remote wind
        tableName = 'ASHAREDESCRIPTION'
        windCursor = self.connWind.cursor()
        fields = ['S_INFO_WINDCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE']
        updateSQL = 'SELECT {0} FROM {1}.{2} WHERE S_INFO_LISTDATE IS NOT NULL'.format(','.join(fields), WPFX, tableName)
        windCursor.execute(updateSQL)
        stkInfo = pd.DataFrame(windCursor.fetchall(),columns=fields)
        if stkInfo.shape[0]>preStkNum:
            stkInfo.sort_values(by=['S_INFO_WINDCODE'], inplace=True)
            stkInfo.set_index(['S_INFO_WINDCODE'], inplace=True)
            try:
                pd.io.sql.to_sql(stkInfo,
                                 name=TableNames.STKBASIC,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype=FieldTypeDict)
                self.logger.info('{0} : stocks basics updated, {1} new stocks'.format(funcName, stkInfo.shape[0] - preStkNum))
            except BaseException as e:
                self.logger.error('{0} : stocks basics updated FAILED'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : no new stock baisc data to update'.format(funcName))
        ######## 更新交易日期列表 ###########
        if self.has_table(tableName=TableNames.TRDDATES, dbName=dbName):
            mysqlCursor.execute('SELECT * FROM {}'.format(TableNames.TRDDATES))
            lastTrdDate = mysqlCursor.fetchall()[-1][0]
        else:
            lastTrdDate = 0
            self.logger.info('{0} : table {1} does not exist in db {2}, will be created'.
                             format(funcName, TableNames.TRDDATES, dbName))
            # read from remote wind
        tableName = 'AShareEODPrices'
        updateSQL = 'SELECT DISTINCT TRADE_DT FROM {0}.{1} WHERE TRADE_DT > {2}'.format(WPFX, tableName, lastTrdDate)
        windCursor.execute(updateSQL)
        trdDates = pd.DataFrame(windCursor.fetchall(),columns=['TRADE_DT']).sort_values(by='TRADE_DT')
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
                    '{0} : trade dates updated, {1} new dates'.format(funcName, trdDates.shape[0]))
            except BaseException as e:
                self.logger.error('{0} : trade dates updated FAILED'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : no new trade date to update, lastupdt {1}'.format(funcName, lastTrdDate))

    def update_trade_info(self, dbName=None):
        """
        更新基本交易数据
        :param dbName:
        :return:
        """
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        tableName = 'AShareEODPrices'
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        # dict to map
        statDict = {'交易': 0, 'XR': 1, 'XD': 2, 'DR': 3, 'N': 4, '停牌': 5, np.nan: 6, }
        if self.has_table(tableName=tableName, dbName=dbName):
            mysqlCursor.execute('SELECT MAX(TRADE_DT) FROM {}'.format(tableName))
            latestDate = [adt[0] for adt in mysqlCursor.fetchall()][-1]
        else:
            self.logger.info('{0} : table {1} does not exist in db {2}, will be created from local file'.
                             format(funcName,tableName,dbName))
            # 从本地文件读入
            tradeInfo = pd.read_csv(r'.\tradeData.csv', encoding='gbk')
            tradeInfo.sort_values(by=['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
            tradeInfo.set_index(['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
            tradeInfo['S_DQ_PCTCHANGE'] = tradeInfo['S_DQ_PCTCHANGE'] / 100
            tradeInfo['S_DQ_TRADESTATUS'] = tradeInfo['S_DQ_TRADESTATUS'].map(statDict)
            pd.io.sql.to_sql(tradeInfo,
                             name=tableName,
                             con=self.connMysqlWrite,
                             if_exists='replace',
                             chunksize=2000,
                             dtype=FieldTypeDict)
            # 重新提取最近更新日期
            mysqlCursor.execute('SELECT MAX(TRADE_DT) FROM {}'.format(tableName))
            latestDate = mysqlCursor.fetchall()[0][0]
        #### 读取交易数据
        fields = ['S_INFO_WINDCODE','TRADE_DT','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT','S_DQ_TRADESTATUS']
        windCursor = self.connWind.cursor()
        updateSQL = 'SELECT {0} FROM {1}.{2} WHERE TRADE_DT>{3}'.format(','.join(fields), WPFX, tableName, latestDate)
        windCursor.execute(updateSQL)
        tradeInfo = pd.DataFrame(windCursor.fetchall(), columns=fields)
        if not tradeInfo.empty:
            tradeInfo.sort_values(by=['TRADE_DT','S_INFO_WINDCODE'],inplace=True)
            tradeInfo.set_index(['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
            tradeInfo['S_DQ_PCTCHANGE'] = tradeInfo['S_DQ_PCTCHANGE']/100
            tradeInfo['S_DQ_TRADESTATUS'] = tradeInfo['S_DQ_TRADESTATUS'].map(statDict)
            try:
                print('{0} : {1} obs to update'.format(funcName, tradeInfo.shape[0]))
                pd.io.sql.to_sql(tradeInfo,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists='append',
                                 chunksize=2000,
                                 dtype=FieldTypeDict)
                self.logger.info('{0} : {1} obs updated with {2} seconds'
                                 .format(funcName, tradeInfo.shape[0], time.time() - start))
            except BaseException as e:
                mysqlCursor.execute('DELETE FROM {0} WHERE TRADE_DT>{1}'.format(tableName, latestDate))
                self.connMysqlRead.commit()
                self.logger.error('{0} : update failed， table cleaned'.format(funcName))
                raise e
        else:
            self.logger.info('{0} : no new data to update'.format(funcName))

    def update_st_info(self, dbName=None):
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        tableName = 'ASHAREST'
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        if self.has_table(tableName=tableName, dbName=dbName):
            mysqlCursor.execute('SELECT COUNT(*) FROM {}'.format(tableName))
            obsNum = mysqlCursor.fetchall()[0][0]
        else:
            obsNum = 0
            self.logger.info('{0} : table {1} does not exist in db {2}, will be created'
                             .format(funcName, tableName, dbName))
        # 从远程数据库读取 数量不多，全部都取出来
        windCursor = self.connWind.cursor()
        fields = ['S_INFO_WINDCODE','S_TYPE_ST','ENTRY_DT', 'REMOVE_DT', 'ANN_DT']
        updateSQL = 'SELECT {0} FROM {1}.{2}'.format(','.join(fields), WPFX, tableName)
        windCursor.execute(updateSQL)
        stInfo = pd.DataFrame(windCursor.fetchall(), columns=fields)
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
            self.logger.info('{0} : no new data to update'.format(funcName))

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
                self.logger.info('{0} : {1} obs updated with {2} seconds'
                                 .format(funcName, shareInfo.shape[0], time.time() - start))
            except BaseException as e:
                # mysqlCursor.execute('DELETE FROM {0} WHERE ANN_DT>{1}'.format(tableName, latestDate))
                # self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned'.format(funcName))
                raise e
            self.patch_next_anndt(tableName=tableName, dbName=dbName)
        else:
            self.logger.info('{0} : no new data to update'.format(funcName))

    def update_holders_info(self, dbName=None):
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        tableName = 'ASHAREFLOATHOLDER'
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        if self.has_table(tableName=tableName, dbName=dbName):
            mysqlCursor.execute('SELECT MAX(ANN_DT) FROM {}'.format(tableName))
            latestDate = mysqlCursor.fetchall()[0][0]
        else:
            latestDate = 0
            self.logger.info('{0} : table {1} does not exist in db {2}, will be created'.
                             format(funcName, tableName, dbName))
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
                mysqlCursor.execute('DELETE FROM {0} WHERE ANN_DT>{1}'.format(tableName, latestDate))
                self.connMysqlRead.commit()
                self.logger.error('{0} : update failed, table cleaned'.format(funcName))
                raise e
            self.patch_next_anndt(tableName=tableName, dbName=dbName)
        else:
            self.logger.info('{0} : no new data to update'.format(funcName))

    def update_industry_info(self):
        pass

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
        self.logger.info('{0} obs patched in table {1}'.format(patchedData.shape[0], tableName))


if __name__=='__main__':

    obj = UpdaterOrigin()
    obj.update_basic_info()     # 其中 trade_dates 表
    obj.update_shares_info()
    obj.patch_next_anndt(tableName='ASHARECAPITALIZATION')
    obj.update_holders_info()
    obj.patch_next_anndt(tableName='ASHAREFLOATHOLDER')
    obj.update_st_info()
    obj.update_trade_info()
