# coding=utf8
__author__ = 'wangjp'

import os
import sys
import time

import numpy as np
import pandas as pd
import sqlalchemy.types as sqltp

from HelpModules.Logger import Logger
from HelpModules.Calendar import Calendar
from HelpModules.DataConnector import DataConnector
from DataReaderModule.DataReader import DataReader
from DataReaderModule.Constants import rootPath
from DataReaderModule.Constants import ALIAS_TABLES as alt
from DataReaderModule.Constants import ALIAS_FIELDS as alf
from DataReaderModule.Constants import ALIAS_STATUS as als




class BaseDataProcessor:
    """
    在基础数据的基础上 生成因子检验、模型训练所需的 中间数据 如过滤条件等
    """

    def __init__(self, basePath=None):
        if basePath is None:
            basePath = os.path.join(rootPath, 'DataProcessModule')
        self.logger = Logger(logPath=os.path.join(basePath,'log')).get_logger(logName='data_processor',loggerName=__name__)
        self.logger.info('')
        self.calendar = Calendar()
        self.dataReader = DataReader(basePath=None, connectRemote=False)
        self.dataConnector = DataConnector(logger=self.logger)

    def update_stock_count(self, updateH5=False):
        """
        计算每日 上市的 股票数量，包含停牌
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        tableName = alt.DAILYCNT
        # 查看最新数据进度
        availableDate = self.dataConnector.get_last_available(fast=True)
        lastUpdt = self.dataConnector.get_last_update(tableName=tableName, isH5=updateH5)
        # extract base data
        if availableDate > str(lastUpdt):
            # 提取数据
            baseFields = [alf.TRDSTAT]
            baseData = self.dataReader.get_data(headDate=lastUpdt,
                                                tailDate=None,
                                                stkList=None,
                                                fields=baseFields,
                                                dbName=self.dataConnector.dbName,
                                                useCache=False,
                                                selectType='OpenClose',)
            procData = baseData.groupby(by=alf.DATE).count()
            procData.columns = [alf.STKCNT]
            # 存储数据
            typeDict = {alf.STKCNT: sqltp.INT, alf.DATE: sqltp.VARCHAR(8)} if not updateH5 else None
            self.dataConnector.store_table(tableData=procData,
                                           tableName=tableName,
                                           if_exist='append',
                                           isH5=updateH5,
                                           typeDict=typeDict)
        else:
            self.logger.info('{0} : No new data to update, last update {1}'.format(funcName, lastUpdt))

    def update_features_filter(self, updateH5=False):
        """
        生成 特征 过滤条件 目前包括：
                    当日停牌
                    前日停牌 : 即复牌后第一天 不发信号
                    当日ST    注：ST还包含L,X,P,Z等，因这些状态也不会交易，故统一按照ST处理
                    当日涨停
                    当日跌停
                    成交额不足
                    上市不满100个交易日
                    最近100个（指数）交易日中交易天数小于50，注： 股票数据的交易日，可能存在数据空缺
                    因重组等原因导致停牌后，复牌不足100个交易日 新重组的股票按照新股对待
        注 ： 当日 指 info ate
        注 ： 涨跌停板制度是1996年12月13日发布、1996年12月16日开始实施的 此前标记的涨跌停 可能不准确
        :param dateList: None 则提取全部交易日期
        :param stkList:  None 则提取全部股票代码

        :return:
        """
        funcName = sys._getframe().f_code.co_name
        tableName = alt.XFILTER
        # 查看最新数据进度
        availableDate = self.dataConnector.get_last_available(fast=True)
        lastUpdt = self.dataConnector.get_last_update(tableName=tableName, isH5=updateH5)
        if availableDate > str(lastUpdt):    # 有新数据 可更新
            cutDate = self.calendar.tdaysoffset(num=-100, currDates=lastUpdt)  # 计算需要提前一定长度的数据, 注意下面取数据是 左开右闭 方式
            baseFields = [alf.PCTCHG, alf.AMOUNT, alf.TRDSTAT, alf.STSTAT]
            baseData = self.dataReader.get_data(headDate=cutDate,
                                                tailDate=None,
                                                stkList=None,
                                                fields=baseFields,
                                                dbName=self.dataConnector.dbName,
                                                useCache=False,
                                                selectType='CloseClose' if lastUpdt==0 else 'OpenClose')  # 不包含 lastupdt
            # 计算 filter
            baseData[als.NOTRD] = baseData[alf.TRDSTAT].isin([5, 6])   # 当日 停牌
            baseData[als.PNOTRD] = baseData[alf.TRDSTAT].groupby(level=alf.STKCD).shift(1).isin([5, 6])  # 前一日停牌
            baseData[als.ISST] = baseData[alf.STSTAT]==1                   # 当日 ST
            baseData[als.LMUP] = baseData[alf.PCTCHG]>=0.099         # 当日 涨停
            baseData[als.LMDW] = baseData[alf.PCTCHG]<=-0.099      # 当日 跌停
            baseData[als.INSFAMT] = baseData[alf.AMOUNT]<=20000000/1000      # 当日 成交额不足两千万 成交额 单位：千元
            baseData['listcnt'] = 1     # 只要有数据就是listed,还未退市
            baseData[als.INSFLST] = baseData['listcnt'].groupby(level=alf.STKCD, sort=False).cumsum()<100  # 上市不足100个交易日, 可以过滤掉新股上市首日
            baseData['hastrd'] = ~baseData[als.NOTRD]
            baseData[als.INSFTRD] = baseData[['hastrd']].groupby(level=alf.STKCD,as_index=False,group_keys=False,sort=False)\
                                        .rolling(window=100, min_periods=0).sum()<50  # 最近100个交易日中交易天数小于50
            # 复牌不足100个交易日的重组股等 可以处理存在数据缺失 的情况
            sqlLines = 'SELECT * FROM {0} WHERE {1}>={2}'.format(alt.TRDDATES, alf.DATE, cutDate)
            tradeDates = pd.read_sql(sql=sqlLines, con=self.dataConnector.connMysqlRead)
            tradeDates['REAL_SHIFT'] = tradeDates[alf.DATE].shift(100)      # 真实对应的 交易日 shift
            tradeDates.set_index(alf.DATE, inplace=True)
            baseData = baseData.join(tradeDates, how='left')
            baseData.reset_index(inplace=True)
            baseData['DATA_SHIFT'] = baseData.groupby(by=alf.STKCD,as_index=False,sort=False).shift(100)[alf.DATE]      # 数据中 对应的 交易日 shift
            baseData[als.INSFRSM] = baseData['DATA_SHIFT'] < baseData['REAL_SHIFT']
            baseData.sort_values(by=[alf.DATE, alf.STKCD], inplace=True)
            baseData.set_index([alf.DATE, alf.STKCD], inplace=True)
            # 清理 baseData 用于最终存储
            filterColumns = [als.NOTRD, als.PNOTRD, als.ISST, als.LMUP, als.LMDW, als.INSFAMT, als.INSFTRD, als.INSFLST, als.INSFRSM]
            outDates = baseData.index.levels[0].values      # baseData的 所有日期
            newTail = np.max(outDates)
            newHead = np.min(outDates[outDates>str(lastUpdt)])
            baseData = baseData.loc[(slice(newHead,newTail),slice(None)), filterColumns]    # 去除此前多取得部分，提取对应 列
            # type Dict
            filterTypes = {col : sqltp.BOOLEAN for col in filterColumns}
            filterTypes[alf.DATE] = sqltp.VARCHAR(8)
            filterTypes[alf.STKCD] = sqltp.VARCHAR(40)
            # 存储数据
            self.dataConnector.store_table(tableData=baseData,
                                           tableName=tableName,
                                           if_exist='append',
                                           isH5=updateH5,
                                           typeDict=filterTypes)
            self.logger.info('{0} : updated from {1} to {2}'.format(funcName, lastUpdt, availableDate))
        else:
            self.logger.info('{0} : No new data to update, last update {1}'.format(funcName, lastUpdt))

    def update_response_filter(self, updateH5=False):
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
        funcName = sys._getframe().f_code.co_name
        tableName = alt.YFILTER
        # 查看最新数据进度
        availableDate = self.dataConnector.get_last_available(fast=True)
        lastUpdt = self.dataConnector.get_last_update(tableName=tableName, isH5=updateH5)
        # extract base data
        if availableDate > str(lastUpdt):
            baseFields = [alf.PCTCHG, alf.OPEN, alf.CLOSE, alf.TRDSTAT, alf.STSTAT]
            baseData = self.dataReader.get_data(headDate=lastUpdt,
                                                tailDate=None,
                                                stkList=None,
                                                fields=baseFields,
                                                dbName=self.dataConnector.dbName,
                                                useCache=False,
                                                selectType='CloseClose' if lastUpdt==0 else 'OpenClose',
                                                )
            baseData['CCRet'] = baseData[alf.PCTCHG]
            baseData['OCRet'] = (baseData[alf.CLOSE]/baseData[alf.OPEN] - 1)    # 当天 开盘到收盘 收益
            baseData['CORet'] = ((1 + baseData['CCRet'])/(1 + baseData['OCRet']) - 1)  # 当日开盘 到 前一日收盘 收益
            baseData['NOTRADE'] = baseData[alf.TRDSTAT].isin([5, 6])
            baseData['ISST'] = baseData[alf.STSTAT]==1
            baseData['COLIMITUP'] = (baseData['CORet'] >= 0.099) & (~baseData['ISST']) | (baseData['CORet'] >= 0.049) & (baseData['ISST'])     # 开盘涨停
            baseData['COLIMITDOWN'] = (baseData['CORet'] <= -0.099) & (~baseData['ISST']) | (baseData['CORet'] <= -0.049) & (baseData['ISST'])  # 开盘跌停
            baseData['CCLIMITUP'] = (baseData['CCRet'] >= 0.099) & (~baseData['ISST']) | (baseData['CORet'] >= 0.049) & (baseData['ISST'])  # 收盘涨停
            baseData['CCLIMITDOWN'] = (baseData['CCRet'] <= -0.099) & (~baseData['ISST']) | (baseData['CCRet'] <= -0.049) & (baseData['ISST'])  # 收盘跌停
            baseData.sort_values(by=[alf.DATE, alf.STKCD], inplace=True)
            filterColumns = ['OCRet', 'CORet', 'CCRet', 'NOTRADE', 'ISST', 'COLIMITUP', 'COLIMITDOWN', 'CCLIMITUP', 'CCLIMITDOWN']
            baseData = baseData.loc[:, filterColumns]
            # type Dict
            filterTypes = {col : sqltp.BOOLEAN for col in filterColumns[3:]}
            filterTypes['OCRet'] = sqltp.FLOAT
            filterTypes['CORet'] = sqltp.FLOAT
            filterTypes['CCRet'] = sqltp.FLOAT
            filterTypes[alf.DATE] = sqltp.VARCHAR(8)
            filterTypes[alf.STKCD] = sqltp.VARCHAR(40)
            # 存储数据
            self.dataConnector.store_table(tableData=baseData,
                                           tableName=tableName,
                                           if_exist='append',
                                           isH5=updateH5,
                                           typeDict=filterTypes)
            self.logger.info('{0} : updated from {1} to {2}'.format(funcName, lastUpdt, availableDate))
        else:
            self.logger.info('{0} : no new data to update, last update {1}'.format(funcName, lastUpdt))

    def update_response(self, updateH5=False):
        """
        更新收益率 日期对应的收益率为未来收益率
        每次更新 需要修正之前的部分
        :param updateH5:
        :return:
        """
        funcName = sys._getframe().f_code.co_name
        tableName = alt.RESPONSE
        # 查看最新数据进度
        availableDate = self.dataConnector.get_last_available(fast=True)
        lastUpdt = self.dataConnector.get_last_update(tableName=tableName, isH5=updateH5)
        # extract base data
        if availableDate > str(lastUpdt):
            cutDate = self.calendar.tdaysoffset(num=-10,currDates=lastUpdt)     # 需要更新已存储的 但是 不完整的部分数据
            stockReturns = self.dataReader.get_responses(headDate=cutDate,
                                                         tailDate=None, #20020101,
                                                         selectType='CloseClose' if lastUpdt==0 else 'OpenClose',
                                                         retTypes={'OC': [1, 10], 'CC': [1]})
            for gap in range(2, 6):  # 构建单日收益率 的 gap 2-5
                stockReturns['OCDay1Gap{}'.format(gap)] = stockReturns[['OCDay1']].groupby(level=alf.STKCD, sort=False,
                                                                                            as_index=False).shift(-gap)
                stockReturns['CCDay1Gap{}'.format(gap)] = stockReturns[['CCDay1']].groupby(level=alf.STKCD, sort=False,
                                                                                            as_index=False).shift(-gap)
            # save data
            # 先修正现存数据
            if self.dataConnector.has_table(tableName=tableName, isH5=updateH5):
                idx = pd.IndexSlice
                changeData = stockReturns.loc[idx[cutDate:lastUpdt,:],:]
                self.dataConnector.change_table(changeData=changeData, tableName=tableName, isH5=updateH5)
            filterTypes = {col : sqltp.Float for col in stockReturns.columns.values}
            filterTypes[alf.DATE] = sqltp.VARCHAR(8)
            filterTypes[alf.STKCD] = sqltp.VARCHAR(40)
            self.dataConnector.store_table(tableData=stockReturns,
                                           tableName=tableName,
                                           if_exist='append',
                                           isH5=updateH5,
                                           typeDict=filterTypes)
            self.logger.info('{0} : updated from {1} to {2}'.format(funcName, lastUpdt, availableDate))
        else:
            self.logger.info('{0} : no new data to update, last update {1}'.format(funcName, lastUpdt))


if __name__=='__main__':
    obj = BaseDataProcessor()

    obj.update_stock_count(updateH5=False)
    obj.update_features_filter(updateH5=False)
    obj.update_response_filter(updateH5=False)

    obj.update_stock_count(updateH5=True)
    obj.update_features_filter(updateH5=True)
    obj.update_response_filter(updateH5=True)

    obj.update_response(updateH5=False)
