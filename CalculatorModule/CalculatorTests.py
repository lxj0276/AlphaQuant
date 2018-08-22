import unittest

import pandas as pd
import numpy as np

from DBReaderModule.Constants import ALIAS_FIELDS
from CalculatorModule.Calculator import Calculator


def simDataGen(stkNum=4,dayNum=6,colnames=('colX',)):
    idx = pd.MultiIndex.from_product([range(stkNum), range(200,200+dayNum)], names=[ALIAS_FIELDS.DATE, ALIAS_FIELDS.STKCD])
    rawdata = np.transpose(np.array([range(dayNum * stkNum)])).astype(np.float32)
    # rawdata[2:3, :] = np.nan
    data = pd.DataFrame(rawdata, columns=colnames, index=idx)
    return data

class TestCalculator(unittest.TestCase):

    def test_init(self):
        calculator = Calculator()
        self.assertFalse(calculator._centered)

    def test_return_shape(self):
        """
        测试 返回值形状
        :return:
        """
        calculator = Calculator()
        simDataX = simDataGen(colnames=('colX',))
        simDataY = simDataGen(colnames=('colY',))
        dataShape = simDataX.shape
        # test functions
        self.assertTrue(np.all(dataShape == calculator.Delay(x=simDataX, num=3,by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Sumif(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD, condition=simDataX>0).shape))
        self.assertTrue(np.all(dataShape == calculator.Diff(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Max(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Min(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Mean(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Sum(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Var(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Std(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Wma(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Sma(x=simDataX, n=3, m=2, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Decaylinear(x=simDataX, d=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Rank(x=simDataX, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.TsToMax(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.TsToMin(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.FindRank(x=simDataX, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.Corr(x=simDataX, y=simDataY, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.RegAlpha(x=simDataX, y=simDataY, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.RegBeta(x=simDataX, y=simDataY, num=3, by=ALIAS_FIELDS.STKCD).shape))
        self.assertTrue(np.all(dataShape == calculator.RegResi(x=simDataX, y=simDataY, num=3, by=ALIAS_FIELDS.STKCD).shape))
        print('return shape test passed')

    def test_return_result(self):
        """
        测试返回值的 计算结果
        :return:
        """
        calculator = Calculator()
        simDataX = simDataGen(colnames=('colX',), stkNum=3, dayNum=4)
        simDataY = simDataGen(colnames=('colY',), stkNum=3, dayNum=4)
        # 最常见情况 没有缺失值


    def test_missing_values(self):
        pass


if __name__=='__main__':
    unittest.main()