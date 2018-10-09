# coding=utf8
__author__ = 'wangjp'

import time

import numpy as np
import pandas as pd
from FactorModule.FactorBase import FactorBase
from CalculatorModule.Calculator import Calculator
from DataReaderModule.Constants import ALIAS_FIELDS as t

class Factor(FactorBase):

    def __init__(self):
        super(Factor,self).__init__()
        self.factorName = __name__.split('.')[-1]
        self.needFields = [t.HIGH, t.LOW, t.CLOSE, t.VOLUME, t.ADJFCT, t.TRDSTAT]  # 设置需要的字段

    def factor_definition(self):
        """
        :return:
        """
        s = time.time()
        needData = self.needData                                # 计算所需数据
        adjLow = needData[t.LOW] * needData[t.ADJFCT]
        adjHigh = needData[t.HIGH] * needData[t.ADJFCT]
        adjClose = needData[t.CLOSE] * needData[t.ADJFCT]
        preClose = Calculator.Delay(x=adjClose, num=1)
        distrib = (adjClose >= preClose)*(adjClose - Calculator.cmpMin(preClose, adjLow)) + (adjClose < preClose)*(adjClose - Calculator.cmpMax(preClose, adjHigh))
        factor = -Calculator.Sum(x=distrib, num=5)/adjClose
        factor = factor.to_frame()
        factor.columns = [self.factorName]                          # 因子计算结果应该只有一列， 如果此处报错 请检查因子定义
        print('factor {0} done with {1} seconds'.format(self.factorName, time.time() - s))
        return factor

    def run_factor(self):
        self.run()



fct = Factor()
fct.run_factor()