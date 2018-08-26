#coding=utf8

import sys

import numpy as np
import pandas as pd

from HelpModules.Logger import Logger
from HelpModules.DataConnector import DataConnector
from DataReaderModule.Constants import ALIAS_TABLES, ALIAS_FIELDS
from DataLocalizeModule.ConstantsDB import quickTableDict,FieldTypeDict



class UpdaterQuick:
    """
    将需要 join 的数据 如 holders,shares 等信息提前 处理 与交易数据 的股票 日期 对其
    并存储在数据库，在因子计算时可以快速提取，不必每次都join
    """

    def __init__(self):
        self.logger = Logger(logPath=r'D:\AlphaQuant\DataLocalizeModule\log').get_logger(loggerName=__name__, logName='update_local_database_quick')
        self.logger.info('')
        self.dataConnector = DataConnector(logger=self.logger)

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

    def update_quick_tables(self, updateH5=False):
        """
        更新所有可以quick表格，需要确保对应origin表格已经更新
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        mysqlCursor = self.dataConnector.connMysqlRead.cursor()
        # 先查看 AshareEodPrices 存储的最新日期
        newAvailableDate = self.dataConnector.get_last_available(fast=True)
        # 再更新需要拼接的表格
        for table in quickTableDict:
            tableName = table if updateH5 else '_'.join([table, 'quick'])
            # 读取现存的最近更新日期
            lastUpdate = self.dataConnector.get_last_update(tableName=tableName, isH5=updateH5)
            ####### 通过 sql join ##########
            bounds = quickTableDict[table]['bounds']
            fields = quickTableDict[table]['fields']
            eteranlDate = 20891230
            hasTable = self.dataConnector.has_table(tableName=tableName, isH5=updateH5)
            if not hasTable: # 数据太多，分次join
                cutDates = [lastUpdate, 20050101, 20100101, 20150101, eteranlDate]
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
                    self.dataConnector.store_table(tableData=joinedTable,
                                                   tableName=tableName,
                                                   if_exist='replace' if dumi==1 else 'append',
                                                   isH5=updateH5,
                                                   typeDict=FieldTypeDict)
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
                    self.dataConnector.store_table(tableData=joinedTable,
                                                   tableName=tableName,
                                                   if_exist='append',
                                                   isH5=updateH5,
                                                   typeDict=FieldTypeDict)
                else:
                    self.logger.info('{0} : {1} has no new data to update '.format(funcName, tableName))

    def update_trade_info_h5(self):
        """
        更新 ashareeodprices H5 file
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        tableName = ALIAS_TABLES.TRAEDINFO
        # 查看最新数据进度
        availableDate = self.dataConnector.get_last_available(fast=True)
        lastUpdt = self.dataConnector.get_last_update(tableName=tableName, isH5=True)
        if availableDate > str(lastUpdt):
            sqlLines = 'SELECT * FROM {0} WHERE {1}>{2}'.format(tableName, ALIAS_FIELDS.DATE, lastUpdt)
            tradeData = pd.read_sql(sql=sqlLines, con=self.dataConnector.connMysqlRead)
            tradeData.sort_values(by=[ALIAS_FIELDS.DATE, ALIAS_FIELDS.STKCD], inplace=True)
            tradeData.set_index([ALIAS_FIELDS.DATE, ALIAS_FIELDS.STKCD], inplace=True)
            self.dataConnector.store_table(tableData=tradeData,
                                           tableName=tableName,
                                           if_exist='append',
                                           isH5=True)
        else:
            self.logger.info('{0} : {1} has no new data to update '.format(funcName, tableName))


if __name__=='__main__':
    obj = UpdaterQuick()
    obj.update_quick_tables(updateH5=False)
    obj.update_quick_tables(updateH5=True)
    obj.update_trade_info_h5()
