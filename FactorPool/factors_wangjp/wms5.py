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
        self.needFields = [t.HIGH, t.LOW, t.CLOSE, t.ADJFCT, t.TRDSTAT]  # 设置需要的字段


    def factor_definition(self):
        s = time.time()
        needData = self.needData                                # 计算所需数据
        needData.loc[needData[t.TRDSTAT].isin([5,6]), t.ADJFCT] = np.nan        # 将停牌对应 股票收益率 设为 NaN

        num = 5
        rHigh = Calculator.Max(x=needData[t.HIGH]*needData[t.ADJFCT], num=num)
        rLow = Calculator.Max(x=needData[t.LOW]*needData[t.ADJFCT], num=num)
        rChg = Calculator.Sum(x = Calculator.Diff(x=needData[t.CLOSE], num=1), num=num)
        factor = -rChg/(rHigh - rLow)
        factor = factor.to_frame()
        factor.columns=[self.factorName]                         # 因子计算结果应该只有一列， 如果此处报错 请检查因子定义
        print('factor {0} done with {1} seconds'.format(self.factorName, time.time() - s))
        return factor

    def run_factor(self):
        self.run()



fct = Factor()
fct.run_factor()