#coding=utf8
__author__ = 'wangjp'


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

from DataReaderModule.Constants import rootPath
from HelpModules.Logger import Logger

class FactorIO:
    """
    用于 因子数据 读写
    """

    def __init__(self, basePath=None, fctDataPath=None):
        if basePath is None:
            basePath = os.path.join(rootPath,'FactorModule')
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(basePath,'configs', 'pathInfo.ini'))
        self.factorDataPath = fctDataPath
        cfp.read(os.path.join(basePath,'configs', 'loginInfo.ini'))
        # create logger
        self.logger = Logger(logPath=os.path.join(basePath,'log')).get_logger(loggerName=__name__, logName='factorIO_log')
        self.logger.info('')

    def read_factor_scores(self):
        pass

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
        first = True
        for fctSC in factorScores:
            scoreType = fctSC.split('_')[-1]
            factorScores[fctSC].to_hdf(path_or_buf=os.path.join(factorStorePath, 'factor_scores.h5'),
                                       key=scoreType,
                                       mode='w' if (ifExist == 'replace' and first)else 'a',
                                       format='table',
                                       append=True,
                                       complevel=4)
            first = False
            print(fctSC, ' updated')
        self.logger.info('factor {0} updated with {1} seconds'.format(factorName, time.time() - start))

    def read_factor_indicators(self, indicators, headDate=None, tailDate=None, dateList=None):
        pass

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
            factorIndicators[indType].to_hdf(path_or_buf=os.path.join(factorStorePath, 'factor_indicators.h5'),
                                             key=indType,
                                             mode='w' if (ifExist == 'replace' and first) else 'a',
                                             format='table',
                                             append=True,
                                             complevel=4)
            first = False
            print(indType,' updated')
        self.logger.info('factor {0} updated with {1} seconds'.format(factorName, time.time() - start))