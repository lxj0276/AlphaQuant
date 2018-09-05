#coding=utf8
__author__ = 'wangjp'

import os
import sys
import time
import configparser as cp

from DataReaderModule.Constants import rootPath
from DataReaderModule.Constants import ALIAS_FIELDS as alf
from DataReaderModule.DataReader import DataReader
from FactorModule.__update__ import update
from FactorModule.FactorIO import FactorIO
from FactorModule.FactorScore import FactorScores
from FactorModule.FactorTests import FactorTests

class FactorBase:

    calculator = None
    dataReader = None
    factorIO = None

    def __init__(self, basePath=None):
        if basePath is None:
            basePath = rootPath
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(basePath,'FactorModule','configs', 'pathInfo.ini'))
        self.headDate = 20000101
        self.tailDate = None
        self.needFields = None
        self.factorName = None
        self.scoreObj = FactorScores()
        self.testsObj = FactorTests()
        if FactorBase.factorIO is None:
            FactorBase.factorIO = FactorIO(basePath=None)
        if FactorBase.dataReader is None:
            FactorBase.dataReader = DataReader(basePath=None, cacheLevel='Level1', connectRemote=False)


    def factor_definition(self):
        raise NotImplementedError

    def run(self):
        """
        完成 因子的 计算，打分，测试，入库 等
        :return:
        """
        # put the generator here

        # self.tailDate=20010101
        start = time.time()
        # 提取需要的数据
        self.needData = self.dataReader.get_data(fields=self.needFields,
                                                 headDate=self.headDate,
                                                 tailDate=self.tailDate,
                                                 selectType='CloseClose')
        # 因子计算
        rawFactor = self.factor_definition()
        # 获取filter X
        filterX = self.dataReader.get_filterX(headDate=self.headDate,
                                              tailDate=self.tailDate,
                                              selectType='CloseClose')
        # 因子打分
        factorScores = self.scoreObj.factor_scores_section(rawFactor=rawFactor,
                                                           filterX=filterX)
        # 因子存储
        ifExist = 'replace' if update.startOver else 'append'
        self.factorIO.write_factor_scores(factorName=self.factorName,
                                          factorScores=factorScores,
                                          ifExist=ifExist)
        # 提取收益率
        stockResponse = self.dataReader.get_responses(headDate=self.headDate,
                                                      tailDate=self.tailDate,
                                                      selectType='CloseClose',
                                                      retTypes={'OC':[1, 10],'CC':[1]})
        stockReturns = {'OCDay1': stockResponse[['OCDay1']], 'OCDay10': stockResponse[['OCDay10']]}
        for gap in range(1,5):    # 构建单日收益率 的 gap 1-4
            stockReturns['OCDay1Gap{}'.format(gap)] = stockResponse[['OCDay1']].groupby(level=alf.STKCD, sort=False, as_index=False).shift(-gap)
            stockReturns['CCDay1Gap{}'.format(gap)] = stockResponse[['CCDay1']].groupby(level=alf.STKCD, sort=False, as_index=False).shift(-gap)
        # 计算因子 统计量
        factorIndicators = self.testsObj.factor_indicators_section(factorScores=factorScores,
                                                             stockRets=stockReturns,
                                                             factorName=self.factorName)
        self.factorIO.write_factor_indcators(factorName=self.factorName,
                                             factorIndicators=factorIndicators,
                                             ifExist=ifExist)
        print('{0} all done with {1} seconds'.format(self.factorName, time.time()-start))