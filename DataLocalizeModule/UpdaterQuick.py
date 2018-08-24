#coding=utf8

import mysql.connector
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

import re
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
        config = r'.\configs' if config is None else config
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(config, 'dataPath.ini'))
        self.h5Path = cfp.get('data','h5')
        cfp.read(os.path.join(config,'loginInfo.ini'))
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'.format(**loginfoMysql))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.connMysqlRead.cursor().execute('USE {}'.format(DatabaseNames.MysqlDaily))
        self.logger = Logger(logPath=r'D:\AlphaQuant\DataLocalizeModule\log').get_logger(loggerName=__name__, logName='update_local_database_quick')
        self.logger.info('')

    def has_table(self,tableName,caseSens=False):
        """
        检查本地 mysql 数据库中是否具有 tableName 表格
        :param tableName:
        :return:
        """
        mysqlCursor = self.connMysqlRead.cursor()
        mysqlCursor.execute('SHOW TABLES')
        if caseSens:
            allTables = [tb[0] for tb in mysqlCursor.fetchall()]
        else:
            tableName = tableName.lower()
            allTables = [tb[0].lower() for tb in mysqlCursor.fetchall()]
        return tableName in allTables

    def get_last_available_update(self):
        """
        查看 交易数据存储的 最新更新日期
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        mysqlCursor = self.connMysqlRead.cursor()
        # 先查看 AshareEodPrices 存储的最新日期
        mysqlCursor.execute('SELECT MAX(TRADE_DT) FROM ASHAREEODPRICES')
        newAvailableDate = mysqlCursor.fetchall()[0][0]
        self.logger.info('{0} : lastest update for AhsareEodPrices is {1}'.format(funcName, newAvailableDate))
        return newAvailableDate

    def _get_joined_table(self, mysqlCursor, subTableCuts, subCutHead, table, fields, bounds, checkSize=True):
        """
        返回 拼接好的 dataframe， 不做 排序 与 加索引
        :param mysqlCursor:
        :param subTableCuts:  list of len 2 : subtable（左表） 选取的 开始 结束 日期
        :param subCutHead:  bool 开始日期 是否包含
        :param table:       需要拼接的表（右表） ex asharecapitalization
        :param fields:      需要拼接的表中 将被取出的字段
        :param bounds:      右表中用来分割左表日期的字段  ex [ANN_DT, NEXT_ANN_DT]
        :param checkSize:   bool 是否检查合并后的表格大小，应该与左表长度相同
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

    def get_joined_table(self, mysqlCursor, subTableCuts, subCutHead, table, fields, bounds, checkSize, isST=True):
        """
        整合 joined table , arguments def same as _get_joined_table
        :param isST:
        :return:
        """
        if isST:
            joinedTable = self._get_joined_table_st(mysqlCursor, subTableCuts, subCutHead, table, fields, bounds, checkSize)
        else:
            joinedTable = self._get_joined_table(mysqlCursor, subTableCuts, subCutHead, table, fields, bounds, checkSize)
        joinedTable[bounds[0]] = joinedTable[bounds[0]].map(lambda x: None if x is np.nan else x)
        joinedTable[bounds[1]] = joinedTable[bounds[1]].map(lambda x: None if x is np.nan else x)
        return joinedTable

    def get_last_update(self, tableName, isH5=False):
        if isH5:
            h5File = os.path.join(self.h5Path,'{}.h5'.format(tableName))
            if not os.path.exists(h5File):
                lastUpdate = 0
            else:
                with pd.HDFStore(h5File) as h5Store:
                    h5Info = h5Store.info()
                    lastRowNum = re.search(r'nrows->(\d+)', h5Info).groups()[0]
                    lastRow = h5Store.select(key=tableName, start=int(lastRowNum)-1)
                    lastUpdate = lastRow.index.values[0][0]
        else:
            if not self.has_table(tableName=tableName):
                lastUpdate = 0   # 有空表格，需要全部提取
            else:
                cursor = self.connMysqlRead.cursor()
                cursor.execute('SELECT MAX(TRADE_DT) FROM {}'.format(tableName))
                lastUpdate = cursor.fetchall()[0][0]
        if lastUpdate==0:
            self.logger.info('Table {} does not exist, will be created'.format(tableName))
        return lastUpdate

    def store_table(self, tableData, tableName, if_exist='replace', isH5=False):
        """
        存储 table 数据
        :param tableData:
        :param tableName:
        :param if_exist:
        :param isH5:
        :return:
        """
        start = time.time()
        dataShape = tableData.shape
        self.logger.info('{0} rows and {1} cols to update ...'.format(dataShape[0], dataShape[1]))
        if isH5:
            h5File = os.path.join(self.h5Path, '{}.h5'.format(tableName))
            tableData.to_hdf(path_or_buf=h5File,
                             key=tableName,
                             mode='a',
                             format='table',
                             append=True,
                             complevel=4)
        else:
            pd.io.sql.to_sql(tableData,
                             name=tableName,
                             con=self.connMysqlWrite,
                             if_exists=if_exist,
                             chunksize=2000,
                             dtype=FieldTypeDict)
        self.logger.info('Table {0} updated of {1} obs , with {2} seconds'
                         .format(tableName, dataShape[0], time.time()-start))

    def clear_table(self, tableName, startDate=None, includeHead=True, isH5=False):
        """
        在数据更新失败的情况下调用， 清理未完成的更新
        :param tableName:
        :param startDate:  开始清除的日期， 如果为 None 则清除 整张表
        :param isH5:
        :return:
        """
        if isH5:
            pass
        else:
            cursor = self.connMysqlRead.cursor()
            if int(startDate)>0:
                cursor.execute('DELETE FROM {0} WHERE TRADE_DT>{1}{2}'
                               .format(tableName, '=' if includeHead else '', startDate))
                self.connMysqlRead.commit()
                self.logger.info('table {0} cleared from {1}'.format(tableName, startDate))
            else:
                cursor.execute('DROP TABLE {0}'.format(tableName))
                self.connMysqlRead.commit()
                self.logger.info('table {0} dropped !'.format(tableName))

    def update_quick_tables(self, updateH5=False):
        """
        更新所有可以quick表格，需要确保对应origin表格已经更新
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        mysqlCursor = self.connMysqlRead.cursor()
        # 先查看 AshareEodPrices 存储的最新日期
        newAvailableDate = self.get_last_available_update()
        # 再更新需要拼接的表格
        for table in quickTableDict:
            tableName = table if updateH5 else '_'.join([table, 'quick'])
            # 读取现存的最近更新日期
            lastUpdate = self.get_last_update(tableName=tableName, isH5=updateH5)
            self.logger.info('{0} : {1}, last update {2},'.format(funcName, tableName, lastUpdate))
            ####### 通过 sql join ##########
            bounds = quickTableDict[table]['bounds']
            fields = quickTableDict[table]['fields']
            eteranlDate = 20891230
            if lastUpdate==0: # 数据太多，分次join
                cutDates = [lastUpdate, 19950101, 20000101, 20050101, 20100101, 20150101, eteranlDate]
                self.logger.info('{0} : SPLITTEDLY update table {1}'.format(funcName, tableName))
                for dumi in range(1, len(cutDates)):
                    joinedTable = self.get_joined_table(mysqlCursor=mysqlCursor,
                                                        subCutHead=True, # 采用 左闭右开 [) 方式截取sub_table
                                                        subTableCuts=[cutDates[dumi-1], cutDates[dumi]],
                                                        table=table,
                                                        fields=fields,
                                                        bounds=bounds,
                                                        checkSize=True,
                                                        isST=table.lower()=='asharest')
                    try:     # 写入更新
                        if dumi==3:
                            t=1
                        self.store_table(tableData=joinedTable,
                                         tableName=tableName,
                                         if_exist='replace' if dumi==1 else 'append',
                                         isH5=updateH5)
                        self.logger.info('{0} : {1} updated from {2} to {3} with {4} obs, head INCLUDED'.
                                         format(funcName, tableName, cutDates[dumi-1], cutDates[dumi], joinedTable.shape[0]))
                    except BaseException as e:  #  处理更新失败
                        self.clear_table(tableName=tableName,
                                         startDate=cutDates[dumi-1],
                                         includeHead=True,      # subtable 切割 采取 含头不含尾， 所以需要用 >= !!!
                                         isH5=updateH5)
                        self.logger.error('{0} : {1} updated failed and cleared'.format(funcName, tableName))
                        raise e
            else:   # 一次 join 全部所需更新额数据
                if newAvailableDate > str(lastUpdate):
                    self.logger.info('{0} : WHOLE update table {1}'.format(funcName, tableName))
                    joinedTable = self.get_joined_table(mysqlCursor=mysqlCursor,
                                                        subCutHead=False,  # 采用 左开右开 方式提取数据
                                                        subTableCuts=[lastUpdate, eteranlDate],
                                                        table=table,
                                                        fields=fields,
                                                        bounds=bounds,
                                                        checkSize=True,
                                                        isST=table.lower() == 'asharest')
                    try:        # 写入数据
                        self.store_table(tableData=joinedTable,
                                         tableName=tableName,
                                         if_exist='append',
                                         isH5=updateH5)
                        self.logger.info('{0} : {1} updated from {2} to {3} with {4} obs, head EXCLUDED'.
                                         format(funcName, tableName, lastUpdate, newAvailableDate, joinedTable.shape[0]))
                    except BaseException as e:      # 清除更新失败
                        self.clear_table(tableName=tableName,
                                         startDate=lastUpdate,
                                         includeHead=False,      # lastUpdate 为之前更新过的数据，清理时 不应被删除， 因而使用 > !!!
                                         isH5=updateH5)
                        self.logger.error('{0} : {1} updated failed and cleared'.format(funcName, tableName))
                        raise e
                else:
                    self.logger.info('{0} : {1} has no new data to update '.format(funcName, tableName))


if __name__=='__main__':
    obj = UpdaterQuick()
    # obj.update_quick_tables(updateH5=False)
    obj.update_quick_tables(updateH5=True)
