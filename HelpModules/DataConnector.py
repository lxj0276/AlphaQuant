# coding = utf8
__author__ = 'wangjp'

import re
import os
import sys
import time
import numpy as np
import pandas as pd
import configparser as cp

import mysql.connector
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

from DataReaderModule.Constants import ALIAS_FIELDS as alf
from DataReaderModule.Constants import ALIAS_TABLES, DatabaseNames, rootPath



class DataConnector:
    """
    用于 数据库 与 本地数据（date, stkcd）类型 的交互
    """
    def __init__(self, logger, dbName=None):
        # setup logger
        self.logger = logger
        cfp = cp.ConfigParser()
        # get mysql
        cfp.read(os.path.join(rootPath,'Configs', 'loginInfo.ini'))
        loginfoMysql = dict(cfp.items('Mysql'))
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'], password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'
                                            .format(**loginfoMysql))
        self.dbName = DatabaseNames.MysqlDaily if dbName is None else dbName
        self.switch_db()
        # get hdf
        cfp.read(os.path.join(rootPath, 'Configs', 'dataPath.ini'))
        self.h5Path = cfp.get('data','h5')
        self.h5Backup = os.path.join(self.h5Path,'backup')
        self.h5Update = os.path.join(self.h5Path,'update')
        if not os.path.exists(self.h5Backup):
            os.mkdir(self.h5Backup)


    def switch_db(self, dbName=None):
        if dbName is None:
            dbName = self.dbName
        cursor = self.connMysqlRead.cursor()
        cursor.execute('USE {}'.format(dbName))
        self.logger.info('Switched to db {} \n'.format(dbName))

    def has_table(self, tableName, isH5):
        if isH5:
            h5File = os.path.join(self.h5Path, '{}.h5'.format(tableName))
            return os.path.exists(h5File)
        else:
            cursor = self.connMysqlRead.cursor()
            cursor.execute('SHOW TABLES')
            allTables = [tb[0].upper() for tb in cursor.fetchall()]
            return tableName.upper() in allTables

    def get_last_available(self, fast=True):
        """
        读取 ASHAREEODPRICES 的最近更新日期
        :param fast:
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        cursor = self.connMysqlRead.cursor()
        if fast:
            cursor.execute('SELECT MAX({0}) FROM {1}'.format(alf.DATE, ALIAS_TABLES.TRDDATES))  # 通过日期表查询 速度较快
        else:
            cursor.execute('SELECT MAX(TRADE_DT) FROM ASHAREEODPRICES')
        newAvailableDate = cursor.fetchall()[0][0]
        self.logger.info('{0} : Lastest available date : {1}'.format(funcName, newAvailableDate))
        return newAvailableDate

    def get_last_update(self, tableName, isH5=False, lastID=False):
        """
        提取 tableName 对应表格的最近更细日期
        :param tableName:
        :param isH5:
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        if not self.has_table(tableName=tableName, isH5=isH5):
            lastUpdate = 0
        else:
            if isH5:
                h5File = os.path.join(self.h5Path,'{}.h5'.format(tableName))
                with pd.HDFStore(h5File) as h5Store:
                    h5Info = h5Store.info()
                    lastRowNum = re.search(r'nrows->(\d+)', h5Info).groups()[0]
                    if lastID:
                        return int(lastRowNum)
                    lastRow = h5Store.select(key=tableName, start=int(lastRowNum)-1)
                    if lastRow.index.nlevels > 1:
                        lastUpdate = lastRow.index.values[0][0]
                    else:
                        lastUpdate = lastRow.index.values[0] if tableName not in ('trade_dates',) else lastRow.values[0][0]
            else:
                cursor = self.connMysqlRead.cursor()
                cursor.execute('SELECT MAX(TRADE_DT) FROM {}'.format(tableName))
                lastUpdate = cursor.fetchall()[0][0]
        if lastUpdate==0:
            self.logger.info('{0}: Table {1} does not exist, will be created'.format(funcName, tableName))
        else:
            self.logger.info('{0}: Last update for table {1} : {2},'.format(funcName, tableName, lastUpdate))
        return lastUpdate

    def store_table(self, tableData, tableName, if_exist='replace', isH5=False, typeDict=None):
        """
        存储 数据表 tableName
        :param tableData:
        :param tableName:
        :param if_exist:
        :param isH5:
        :return:
        """
        start = time.time()
        funcName = sys._getframe().f_code.co_name
        dataShape = tableData.shape
        hasTable = self.has_table(tableName=tableName, isH5=isH5)
        formatDict = {'funcName': funcName, 'tableName': tableName, 'shape0': dataShape[0],'shape1': dataShape[1]}
        self.logger.info('{funcName} : Table {tableName} has {shape0} rows and {shape1} cols to update ...'.format(**formatDict))
        if isH5:
            if 'UPDATE' in tableName:   # 需要更新的 文件是 存储有效数据的文件
                tableName = tableName.split('_')[0]
                h5File = os.path.join(self.h5Update, '{}_UPDATE.h5'.format(tableName))
            else:
                h5File = os.path.join(self.h5Path, '{}.h5'.format(tableName))
            if hasTable:
                h5BackupFile = os.path.join(self.h5Backup, '{}.h5'.format(tableName))
                cpResult = os.system('COPY {0} {1} /Y'.format(h5File, h5BackupFile))         # 写入前进行数据备份
                assert cpResult==0, '{0} ： backup h5 table {1} failed! \n'.format(funcName, tableName)
            try:
                tableData.to_hdf(path_or_buf=h5File,
                                 key=tableName,
                                 mode='w' if if_exist=='replace' else 'a',
                                 format='table',
                                 append=True,
                                 complevel=4)
            except BaseException as e:      # 更新数据失败 清理未完成数据
                print('rolling back')
                reset = os.system('COPY {0} {1} /Y'.format(h5BackupFile, h5File)) if hasTable else os.system('DEL {}'.format(h5File))
                assert reset==0
                self.logger.error('{funcName} : Table {tableName} update Failed and Reset \n'.format(**formatDict))
                raise e
        else:
            try:
                pd.io.sql.to_sql(tableData,
                                 name=tableName,
                                 con=self.connMysqlWrite,
                                 if_exists=if_exist,
                                 chunksize=2000,
                                 dtype=typeDict)
            except BaseException as e:
                cursor = self.connMysqlRead.cursor()
                if not hasTable:
                    cursor.execute('DROP TABLE {0}'.format(tableName))
                    self.connMysqlRead.commit()
                    self.logger.info('table {0} dropped ! \n'.format(tableName))
                else:
                    startDate = np.min(tableData.index.levels[0].values)
                    cursor.execute('DELETE FROM {0} WHERE TRADE_DT>={1}'.format(tableName, startDate))
                    self.connMysqlRead.commit()
                    self.logger.info('table {0} cleared from {1} \n'.format(tableName, startDate))
                raise e
        formatDict['timeUsed'] = time.time() - start
        self.logger.info('{funcName} : Table {tableName} updated of shape ({shape0},{shape1}), with {timeUsed} seconds \n'.format(**formatDict))

    def change_table(self,changeData, tableName, isH5=False):
        """
        修改现有数据表
        :param changeData:  用来替代现有数据的数据表，其index必须全部 包含于 已有数据
        :param tableName:
        :param isH5:
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        changeData.reset_index(inplace=True)
        dataColumns = changeData.columns.values
        self.logger.info('{0}: {1} obs to change in table {2} ...'.format(funcName, changeData.shape[0], tableName))
        if isH5:
            h5UpdateFile = os.path.join(self.h5Update, '{}_UPDATE.h5'.format(tableName))       # 存储全部有效数据的 file， 应该截断至changeData前一日
            h5UpdateBack = os.path.join(self.h5Update, '{}_UPDATE_COPY.h5'.format(tableName))
            cpResult = os.system('COPY {0} {1} /Y'.format(h5UpdateFile, h5UpdateBack))  # 拼接有效数据前 前进行数据备份
            assert cpResult == 0, '{0} ： backup h5 table {1} failed'.format(funcName, tableName)
            h5UpdateMove = os.path.join(self.h5Update, '{}_UPDATE_MOVE.h5'.format(tableName))
            cpResult = os.system('COPY {0} {1} /Y'.format(h5UpdateFile, h5UpdateMove))  # 用于修改成待更新文件，原_update.h5 需要保留做更新，将在baseDataProcessor 中进行
            assert cpResult == 0
            ################   修正待更新数据    ###########################
            try:    # 将 修改数据添加到 有效数据，形成新的 待更新数据
                changeData.set_index([alf.DATE,alf.STKCD], inplace=True)
                changeData.to_hdf(path_or_buf=h5UpdateMove,
                                 key=tableName,
                                 mode='a',
                                 format='table',
                                 append=True,
                                 complevel=4)
                savedNum = self.get_last_update(tableName=tableName, isH5=True, lastID=True)
                with pd.HDFStore(h5UpdateMove) as h5Store:
                    lastRowNum = re.search(r'nrows->(\d+)', h5Store.info()).groups()[0]
                if savedNum==int(lastRowNum):
                    self.logger.info('{}: new to update file row num correct'.format(funcName))
                else:
                    raise BaseException('row num miss match!')
            except BaseException as e:      # 更新数据失败 清理未完成数据
                print(funcName, 'rolling back')
                reset = os.system('COPY {0} {1} /Y'.format(h5UpdateBack, h5UpdateMove))     # 更新失败 回复数据
                assert reset==0
                self.logger.error('{0} : Table {1} changed Failed and Reset'.format(funcName, tableName))
                raise e
            ##################################################################################
            h5File = os.path.join(self.h5Path, '{}.h5'.format(tableName))         # 现存（旧的）待更新数据
            cpResult = os.system('COPY {0} {1} /Y'.format(h5UpdateMove, h5File))  # 用新的 待更新数据 替换旧的 待更新数据
            assert cpResult == 0
            delResult = os.system('DEL {}'.format(h5UpdateMove))    # 删除 已经被修改过得 有效数据
            assert delResult==0
        else:
            for dumi in range(changeData.shape[0]):
                row = changeData.iloc[dumi]
                rowNaN = row.isna()
                formatDict = {col:row[col] for col in dataColumns}
                formatDict['table'] = tableName
                formatDict['date'] = row[alf.DATE]
                formatDict['stkcd'] = row[alf.STKCD]
                setstr = ','.join([''.join([col,'="{',col,'}"']) if col==alf.STKCD else ''.join([col,'={',col,'}']) for col in dataColumns if not rowNaN[col]]).format(**formatDict)
                formatDict['setstr'] = setstr
                sql = 'UPDATE {table} SET {setstr} WHERE S_INFO_WINDCODE="{stkcd}" AND TRADE_DT={date}'. format(**formatDict)
                self.connMysqlRead.cursor().execute(sql)
            self.connMysqlRead.commit()
        self.logger.info('{0}: {1} obs changed in table {2}'.format(funcName, changeData.shape[0], tableName))


    def check_table(self, tableName):
        pass