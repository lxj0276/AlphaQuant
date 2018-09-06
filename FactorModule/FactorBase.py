#coding=utf8
__author__ = 'wangjp'

import os
import time

from DataReaderModule.Constants import rootPath
from DataReaderModule.DataReader import DataReader
from DataReaderModule.Constants import ALIAS_FIELDS as alf
from DataReaderModule.Constants import ALIAS_RESPONSE as alr
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
        self.headDate = 20000101
        self.tailDate = None
        self.needFields = None
        self.factorName = None
        self.scoreObj = FactorScores()
        self.testsObj = FactorTests()
        if FactorBase.factorIO is None:
            FactorBase.factorIO = FactorIO(fctDataPath=update.fctDataPath)
        if FactorBase.dataReader is None:
            FactorBase.dataReader = DataReader(cacheLevel='Level1', connectRemote=False)


    def factor_definition(self):
        raise NotImplementedError

    def run(self):
        """
        完成 因子的 计算，打分，测试，入库 等
        :return:
        """

        self.headDate = 20180101
        # self.tailDate=20010101
        print('updating factor {0} , {1}'.format(self.factorName, 'start new' if update.startOver else 'update exist'))
        start = time.time()
        # 提取需要的数据       # 注： 因子更新 数据需要部分 日期提前，取决于因子定义 ！！！！ 还没完成， 完成后 rawFactor 需要重新 切割 ！！！
        self.needData = self.dataReader.get_data(fields=self.needFields,
                                                 headDate=self.headDate,
                                                 tailDate=self.tailDate,
                                                 selectType='CloseClose',
                                                 fromMysql=False,
                                                 useCache=update.useCache)
        # 因子计算
        rawFactor = self.factor_definition()
        # 获取filter X
        filterX = self.dataReader.get_data(headDate=self.headDate,
                                           tailDate=self.tailDate,
                                           selectType='CloseClose',
                                           fields=['FilterX'],
                                           fromMysql=False,
                                           useCache=update.useCache)['FilterX']
        # 因子打分
        factorScores = self.scoreObj.factor_scores_section(rawFactor=rawFactor, filterX=filterX)
        # 因子存储
        ifExist = 'replace' if update.startOver else 'append'
        self.factorIO.write_factor_scores(factorName=self.factorName,
                                          factorScores=factorScores,
                                          ifExist=ifExist)
        # 提取收益率
        responseFields = [alr.OC1, alr.OC10, alr.OCG1, alr.OCG2, alr.OCG3, alr.OCG4, alr.CCG1, alr.CCG2, alr.CCG3, alr.CCG4]
        stockResponse = self.dataReader.get_data(headDate=self.headDate,
                                                 tailDate=self.tailDate,
                                                 selectType='CloseClose',
                                                 fromMysql=False,
                                                 useCache=False,
                                                 fields=responseFields)
        # 计算因子 统计量
        factorIndicators = self.testsObj.factor_indicators_section(factorScores=factorScores,
                                                                   stockRets=stockResponse,
                                                                   factorName=self.factorName)
        # 因子统计量 存储
        self.factorIO.write_factor_indcators(factorName=self.factorName,
                                             factorIndicators=factorIndicators,
                                             ifExist=ifExist)
        print('{0} all done with {1} seconds'.format(self.factorName, time.time()-start))