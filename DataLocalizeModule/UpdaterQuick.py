#coding=utf8

import mysql.connector
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

import os
import sys
import time
import datetime as dt
import configparser as cp

import numpy as np
import pandas as pd

from HelpModules.Logger import Logger
from DataLocalizeModule.ConstantsDB import DatabaseNames,TableNames,quickTableDict,FieldTypeDict



class UpdaterQuick:
    """
    将需要 join 的数据 如 holders,shares 等信息提前 处理 与交易数据 的股票 日期 对其
    并存储在数据库，在因子计算时可以快速提取，不必每次都join
    """

    def __init__(self, config=None):
        config = r'.\configs\loginInfo.ini' if config is None else config
        cfp = cp.ConfigParser()
        cfp.read(config)
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'.format(**loginfoMysql))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.logger = Logger(logPath=r'D:\AlphaQuant\DataLocalizeModule\log').get_logger(loggerName=__name__, logName='update_local_database_quick')
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

    def get_last_trade_update(self, dbName=None):
        """
        查看 交易数据存储的 最新更新日期
        :return:
        """
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        funcName = sys._getframe().f_code.co_name
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        # 先查看 AshareEodPrices 存储的最新日期
        mysqlCursor.execute('SELECT MAX(TRADE_DT) FROM ASHAREEODPRICES')
        newAvailableDate = mysqlCursor.fetchall()[0][0]
        self.logger.info('{0} : lastest update for AhsareEodPrices is {1}'.format(funcName, newAvailableDate))
        return newAvailableDate

    def _get_joined_table(self, mysqlCursor, subTableCuts, subCutHead, table, fields, bounds, checkSize=True):
        """
        返回 拼接好的 dataframe， 不做 排序 与 加索引
        :param mysqlCursor:
        :param subTableCuts:
        :param table:
        :param fields:
        :param bounds:
        :param checkSize:
        :return:
        """
        # 提取的 trade_info: 需要处理 trade_dt > max(ann_dt) 的情况
        subTable = 'SELECT S_INFO_WINDCODE, TRADE_DT FROM ASHAREEODPRICES WHERE TRADE_DT>{0}{1} AND TRADE_DT<{2}'\
            .format('=' if subCutHead else '',subTableCuts[0], subTableCuts[1])
        formatDict = {
            'fields': ','.join(['.'.join(['r',fd]) for fd in fields]),
            'subTable': subTable,
            'table': table,
            'boundL': bounds[0],
            'boundR': bounds[1]
        }
        sqlLines = ''.join(['SELECT l.S_INFO_WINDCODE, l.TRADE_DT, {fields} ',
                            'FROM ({subTable}) AS l ',
                            'LEFT JOIN {table} AS r ',
                            'ON l.S_INFO_WINDCODE=r.S_INFO_WINDCODE AND ',
                            '((l.TRADE_DT>r.{boundL} AND l.TRADE_DT<=r.{boundR}) ',
                            'OR (l.TRADE_DT>r.{boundL} AND r.{boundR} IS NULL))']).format(**formatDict)
        mysqlCursor.execute(sqlLines)
        joinedTable = pd.DataFrame(mysqlCursor.fetchall(), columns=['S_INFO_WINDCODE', 'TRADE_DT'] + fields)
        joinedTable.sort_values(by=['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
        joinedTable.set_index(['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
        if checkSize:
            # 获取 subtable size 用于检查
            mysqlCursor.execute('SELECT COUNT(*) FROM ({}) AS new'.format(subTable))
            subTableSize = mysqlCursor.fetchall()[0][0]
            assert joinedTable.shape[0] == subTableSize, 'size missmath !'
        return joinedTable

    def _get_joined_table_st(self,mysqlCursor, subTableCuts, subCutHead, table, fields, bounds, checkSize=True):
        """
        ST 状态的表格需要特殊处理
        """
        subTable = 'SELECT S_INFO_WINDCODE, TRADE_DT FROM ASHAREEODPRICES WHERE TRADE_DT>{0}{1} AND TRADE_DT<{2}'. \
            format('=' if subCutHead else '',subTableCuts[0], subTableCuts[1])
        formatDict = {
            'fields': ','.join(['.'.join(['r',fd]) for fd in fields]),
            'subTable': subTable,
            'table': table,
            'boundL': bounds[0],
            'boundR': bounds[1]
        }
        sqlLines = ''.join(['SELECT l.S_INFO_WINDCODE, l.TRADE_DT, {fields} ',
                            'FROM ({subTable}) AS l ',
                            'LEFT JOIN (SELECT * FROM {table} WHERE S_TYPE_ST!="T") AS r ',
                            'ON l.S_INFO_WINDCODE=r.S_INFO_WINDCODE AND ',
                            '((l.TRADE_DT>=r.{boundL} AND l.TRADE_DT<r.{boundR} AND r.{boundR} IS NOT NULL)',
                            'OR (l.TRADE_DT>=r.{boundL} AND r.{boundR} IS NULL))']).format(**formatDict)
        mysqlCursor.execute(sqlLines)
        joinedTable = pd.DataFrame(mysqlCursor.fetchall(), columns=['S_INFO_WINDCODE', 'TRADE_DT'] + fields)
        stDict = {np.nan: False, 'S': True, 'L': True, 'Z': True, 'P': True, 'X': True, 'T': True}
        joinedTable['TYPE_ST'] = joinedTable['S_TYPE_ST'].map(stDict)
        joinedTable.drop(['S_TYPE_ST'], axis=1, inplace=True)
        joinedTable.sort_values(by=['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
        joinedTable.set_index(['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
        joinedTable = joinedTable[~joinedTable.index.duplicated()]  # 剔除重复数据 600056
        if checkSize:
            # 获取 subtable size 用于检查
            mysqlCursor.execute('SELECT COUNT(*) FROM ({}) AS new'.format(subTable))
            subTableSize = mysqlCursor.fetchall()[0][0]
            assert joinedTable.shape[0] == subTableSize, 'size missmath !'
        return joinedTable


    def update_quick_tables(self, dbName=None):
        """
        更新所有可以quick表格，需要确保对应origin表格已经更新
        :param dbName:
        :return:
        """
        dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        funcName = sys._getframe().f_code.co_name
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('USE {}'.format(dbName))
        # 先查看 AshareEodPrices 存储的最新日期
        newAvailableDate = self.get_last_trade_update(dbName=dbName)
        # 再更新需要拼接的表格
        for table in quickTableDict:
            tableName = '_'.join([table, 'quick'])
            self.logger.info('updating {}'.format(tableName))
            # 读取现存的最近更新日期
            if not self.has_table(tableName=tableName, dbName=dbName):
                lastUpdate = 0   # 有空表格，需要全部提取
            else:
                mysqlCursor.execute('SELECT MAX(TRADE_DT) FROM {}'.format(tableName))
                lastUpdate = mysqlCursor.fetchall()[0][0]
            self.logger.info('{0} : last update {1}, {2}'.format(funcName, lastUpdate, tableName))
            ####### 通过 sql join ##########
            bounds = quickTableDict[table]['bounds']
            fields = quickTableDict[table]['fields']
            eteranlDate = 20891230
            if lastUpdate==0: # 数据太多，分次join
                cutDates = [lastUpdate, 19950101, 20000101, 20050101, 20100101, 20150101, eteranlDate]
                self.logger.info('{0} : SPLITTEDLY update table {1}'.format(funcName, tableName))
                for dumi in range(1, len(cutDates)):
                    if table.lower()=='asharest':
                        joinedTable = self._get_joined_table_st(mysqlCursor=mysqlCursor,
                                                                subCutHead=True, # 采用 左闭右开 [) 方式截取sub_table
                                                                subTableCuts=[cutDates[dumi-1], cutDates[dumi]],
                                                                table=table,
                                                                fields=fields,
                                                                bounds=bounds,
                                                                checkSize=True)
                    else:
                        joinedTable = self._get_joined_table(mysqlCursor=mysqlCursor,
                                                             subCutHead=True, # 采用 左闭右开 [) 方式截取sub_table
                                                             subTableCuts=[cutDates[dumi-1], cutDates[dumi]],
                                                             table=table,
                                                             fields=fields,
                                                             bounds=bounds,
                                                             checkSize=True)
                    try:
                        dataShape = joinedTable.shape
                        self.logger.info('{0} : {1} rows and {2} cols to update ...'.format(funcName, dataShape[0], dataShape[1]))
                        pd.io.sql.to_sql(joinedTable,
                                         name=tableName,
                                         con=self.connMysqlWrite,
                                         if_exists='replace' if dumi==1 else 'append',
                                         chunksize=2000,
                                         dtype=FieldTypeDict)
                        self.logger.info('{0} : table {1} updated from {2} to {3} with {4} obs, head included'
                                         .format(funcName, tableName, cutDates[dumi-1], cutDates[dumi], joinedTable.shape[0]))
                    except BaseException as e:
                        # subtable 切割 采取 含头不含尾， 所以需要用 >= !!!
                        mysqlCursor.execute('DELETE FROM {0} WHERE TRADE_DT>={1}'.format(tableName, cutDates[dumi-1]))
                        self.connMysqlRead.commit()
                        self.logger.error('{0} : table {1} updated failed'.format(funcName, tableName))
                        raise e
            else:   # 一次 join 全部所需更新额数据
                if newAvailableDate > str(lastUpdate):
                    self.logger.info('{0} : WHOLE update table {1}'.format(funcName, tableName))
                    if table.lower() == 'asharest':
                        joinedTable = self._get_joined_table_st(mysqlCursor=mysqlCursor,
                                                                subCutHead=False,  # 采用 左开右开 方式提取数据
                                                                subTableCuts=[lastUpdate, eteranlDate],
                                                                table=table,
                                                                fields=fields,
                                                                bounds=bounds,
                                                                checkSize=True)
                    else:
                        joinedTable = self._get_joined_table(mysqlCursor=mysqlCursor,
                                                             subCutHead=False,  # 采用 左开右开 方式提取数据
                                                             subTableCuts=[lastUpdate, eteranlDate],
                                                             table=table,
                                                             fields=fields,
                                                             bounds=bounds,
                                                             checkSize=True)
                    try:
                        dataShape = joinedTable.shape
                        self.logger.info('{0} : {1} rows and {2} cols to update ...'.format(funcName, dataShape[0], dataShape[1]))
                        pd.io.sql.to_sql(joinedTable,
                                         name=tableName,
                                         con=self.connMysqlWrite,
                                         if_exists='append',
                                         chunksize=2000,
                                         dtype=FieldTypeDict)
                        self.logger.info('{0} : table {1} updated from {2} to {3} with {4} obs, head excluded'.
                                         format(funcName, tableName, lastUpdate, newAvailableDate, joinedTable.shape[0]))
                    except BaseException as e:
                        # lastUpdate 为之前更新过的数据，清理时 不应被删除， 因而使用 >
                        mysqlCursor.execute('DELETE FROM {0} WHERE TRADE_DT>{1}'.format(tableName, lastUpdate))
                        self.connMysqlRead.commit()
                        self.logger.error('{0} : table {1} updated failed'.format(funcName, tableName))
                        raise e
                else:
                    self.logger.info('{0} : table {1} has no new data to update '.format(funcName, tableName))


if __name__=='__main__':
    obj = UpdaterQuick()
    obj.update_quick_tables()

