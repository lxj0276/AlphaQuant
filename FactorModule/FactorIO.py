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

    def __init__(self, basePath):
        if basePath is None:
            basePath = os.path.join(rootPath,'FactorModule')
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(basePath,'configs', 'pathInfo.ini'))
        self.factorDataPath = cfp.get('path','factorData')
        cfp.read(os.path.join(basePath,'configs', 'loginInfo.ini'))
        loginfoMysql = dict(cfp.items('Mysql'))
        # create logger
        self.logger = Logger(logPath=os.path.join(basePath,'log')).get_logger(loggerName=__name__, logName='factorIO_log')
        self.logger.info('')
        self.connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                                     password=loginfoMysql['password'],
                                                     host=loginfoMysql['host'])
        self.connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'
                                            .format(**loginfoMysql))

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
        append = ifExist=='append'
        h5 = pd.HDFStore(path=os.path.join(factorStorePath, 'factor_scores.h5'),
                         mode='a' if append else 'w',
                         complevel=4,
                         complib='blosc')
        self.logger.info('updating factor {}...'.format(factorName))
        for fctSC in factorScores:
            scoreType = fctSC.split('_')[-1]
            h5.put(key=scoreType, value=factorScores[fctSC], format='table', append=append)
            print(fctSC,' updated')
        self.logger.info('factor {0} updated with {1} seconds'.format(factorName, time.time() - start))

    def read_factor_indicators(self):
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
        append = ifExist=='append'
        h5 = pd.HDFStore(path=os.path.join(factorStorePath, 'factor_indicators.h5'),
                         mode='a' if append else 'w',
                         complevel=4,
                         complib='blosc')
        self.logger.info('updating factor {}...'.format(factorName))
        for indType in factorIndicators:
            h5.put(key=indType, value=factorIndicators[indType], format='table', append=append)
            print(indType,' updated')
        self.logger.info('factor {0} updated with {1} seconds'.format(factorName, time.time() - start))