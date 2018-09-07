#coding=utf8
__author__ = 'wangjp'

import os
import time

import numpy as np
import pandas as pd

from HelpModules.Logger import Logger
from HelpModules.Calendar import Calendar
from DataReaderModule.Constants import rootPath
from DataReaderModule.Constants import ALIAS_FIELDS as alf
from DataReaderModule.Constants import ALIAS_RESPONSE as alr

class FactorIO:
    """
    用于 因子数据 读写
    """

    def __init__(self, basePath=None, fctDataPath=None):
        if basePath is None:
            basePath = os.path.join(rootPath,'FactorModule')
        self.factorDataPath = fctDataPath
        self.logger = Logger(logPath=os.path.join(basePath,'log')).get_logger(loggerName=__name__, logName='factorIO_log')
        self.logger.info('')
        self.fctScoreFile = 'factor_scores.h5'
        self.fctIndicatorFile = 'factor_indicators.h5'
        self.calendar = Calendar()

    def read_factor_scores(self,
                           factorName,
                           scoreTypes=None,
                           headDate=None,
                           tailDate=None,
                           dateList=None,
                           stkList=None):
        """
        读取因子得分
        :param factorName:
        :param scoreTypes:
        :param fields:
        :param headDate:
        :param tailDate:
        :param dateList:
        :param stockList:
        :return:
        """
        readAll = False
        if dateList is None:
            if headDate is None and tailDate is None:   # 将读取全部日期
                readAll = True
            else:
                headDate = self.calendar.HeadDate if headDate is None else headDate
                tailDate = self.calendar.TailDate if tailDate is None else tailDate
                dateList = self.calendar.tdaysbetween(headDate=headDate,
                                                      tailDate=tailDate,
                                                      selectType='CloseClose')
        else:
            dateList = [str(tdt) for tdt in dateList]
        scoreTypes = ['raw','rank','zscore'] if scoreTypes is None else scoreTypes
        factorScorePath = os.path.join(self.factorDataPath, factorName, self.fctScoreFile)
        whereLines = None if readAll else '{0} in ({1})'.format(alf.DATE, ','.join(dateList))
        if whereLines is not None:
            if stkList is not None:
                stkList = [str(stk) for stk in stkList]
                whereLines = ''.join([whereLines, ' and {0} in {1}'.format(alf.STKCD, ','.join(stkList))])
        factorScores = pd.read_hdf(path_or_buf=factorScorePath,
                                   key=factorName,
                                   where=whereLines,
                                   columns=scoreTypes,
                                   mode='r')
        return factorScores

    def write_factor_scores(self,factorName, factorScores,ifExist='replace'):
        """
        优化： 协程，加写入前检查
        :param factorScores:    dict of factor scores
        :return:
        """
        start = time.time()
        factorStorePath = os.path.join(self.factorDataPath,factorName)
        if not os.path.exists(factorStorePath):
            os.mkdir(factorStorePath)
        self.logger.info('updating factor {}...'.format(factorName))
        allFactorScores = None
        for fctSC in factorScores:
            scoreType = fctSC.split('_')[-1]
            factorScores[fctSC].columns = [scoreType]
            allFactorScores = factorScores[fctSC] if allFactorScores is None else allFactorScores.join(factorScores[fctSC], how='outer')
            allFactorScores.to_hdf(path_or_buf=os.path.join(factorStorePath, self.fctScoreFile),
                                   key=factorName,
                                   mode='w' if ifExist == 'replace' else 'a',
                                   format='table',
                                   append=True,
                                   complevel=4)
        self.logger.info('factor {0} updated with {1} seconds'.format(factorName, time.time() - start))

    def read_factor_indicators(self,
                               factorName,
                               indicators,
                               responses=None,
                               headDate=None,
                               tailDate=None,
                               dateList=None):
        """
        读取因子截面指标
        :param factorName:
        :param indicators:
        :param headDate:
        :param tailDate:
        :param dateList:
        :return:
        """
        readAll = False
        if dateList is None:
            if headDate is None and tailDate is None:   # 将读取全部日期
                readAll = True
            else:
                headDate = self.calendar.HeadDate if headDate is None else headDate
                tailDate = self.calendar.TailDate if tailDate is None else tailDate
                dateList = self.calendar.tdaysbetween(headDate=headDate,
                                                      tailDate=tailDate,
                                                      selectType='CloseClose')
        else:
            dateList = [str(tdt) for tdt in dateList]
        fctIndicatorsPath = os.path.join(self.factorDataPath, factorName, self.fctIndicatorFile)
        whereLines = None if readAll else '{0} in ({1})'.format(alf.DATE, ','.join(dateList))
        if responses is None:
            responses = [alr.OC1, alr.OC10, alr.OCG1, alr.OCG2, alr.OCG3, alr.OCG4, alr.CCG1, alr.CCG2, alr.CCG3, alr.CCG4]
        factorIndicators = {}
        for indType in indicators:
            factorIndicators[indType] = pd.read_hdf(path_or_buf=fctIndicatorsPath,
                                                    key=indType,
                                                    where=whereLines,
                                                    columns=responses,
                                                    mode='r')
        return factorIndicators


    def write_factor_indcators(self, factorName, factorIndicators,ifExist='replace'):
        """
        优化： 协程，加写入前检查
        :param factorIndicators:  dict of factor indicators
        :return:
        """
        start = time.time()
        factorStorePath = os.path.join(self.factorDataPath,factorName)
        if not os.path.exists(factorStorePath):
            os.mkdir(factorStorePath)
        self.logger.info('updating factor {}...'.format(factorName))
        first = True
        for indType in factorIndicators:
            factorIndicators[indType].to_hdf(path_or_buf=os.path.join(factorStorePath, self.fctIndicatorFile),
                                             key=indType,
                                             mode='w' if (ifExist == 'replace' and first) else 'a',
                                             format='table',
                                             append=True,
                                             complevel=4)
            first = False
            print(indType,' updated')
        self.logger.info('factor {0} updated with {1} seconds'.format(factorName, time.time() - start))


if __name__=='__main__':
    obj = FactorIO(fctDataPath=r'D:\AlphaQuant\FactorPool\factors_data')
    # obj.read_factor_indicators(factorName='mom5',indicators=['beta','IC','groupIC'], fields=[alr.OC1,alr.OCG1])
    obj.read_factor_scores(factorName='mom5',)