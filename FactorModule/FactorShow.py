#coding=utf8
__author__ = 'wangjp'

import os
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

from FactorModule.FactorIO import FactorIO
from DataReaderModule.Constants import ALIAS_RESPONSE as alr
from DataReaderModule.Constants import ALIAS_INDICATORS as ali

class FactorShow:
    """
    用于 因子 展示
    """

    def __init__(self, fctDataPath):
        self.fctDataPath = fctDataPath
        self.fctIO = FactorIO(fctDataPath=self.fctDataPath)

    def show_curves(self, factorName, indicator, responses, headDate=None, tailDate=None):
        """
        画 指标的累计求和 曲线
        :param factorName:
        :param indicator:
        :param responses:
        :param headDate:
        :param tailDate:
        :return:
        """
        indSingle = self.fctIO.read_factor_indicators(factorName=factorName,
                                                      indicators=[indicator],
                                                      responses=responses,
                                                      headDate=headDate,
                                                      tailDate=tailDate)
        indSingle = indSingle[indicator]
        # indSingle['OCRet2'] = indSingle['OCDay1'] + indSingle['CCDay1Gap1']
        indSingle = indSingle.cumsum()
        plt.figure(figsize=(20, 13))
        for dumi, col in enumerate(indSingle.columns):
            plt.plot(indSingle[col], label=col, lw=1)
        xticksNum = 20
        stepSize = int(indSingle.shape[0]/xticksNum)
        xtickSteps = range(0, indSingle.shape[0] - 1, stepSize)
        plt.xticks(xtickSteps, indSingle.index.values[xtickSteps], rotation=70)
        plt.legend(loc='upper left')
        plt.title('{0} : CUMSUM_{1}'.format(factorName, indicator))
        plt.show()

    def show_statistics(self, factorName, indicators = None, responses=None, headDate=None, tailDate=None, outPath=None):
        """
        计算截面指标的统计量 ：均值、方差、t统计量等
        :param factorName:
        :param indicator:
        :param responses:
        :param headDate:
        :param tailDate:
        :return:
        """
        if indicators is None:
            indicators = [ali.BETA, ali.TBDF, ali.IC, ali.RKIC, ali.GPIC]
        if responses is None:
            responses = [alr.OC1, alr.OCG1, alr.OCG2, alr.OCG3, alr.OCG4, alr.CCG1, alr.CCG2, alr.CCG3, alr.CCG4]
        indValues = self.fctIO.read_factor_indicators(factorName=factorName,
                                                      indicators=indicators,
                                                      responses=responses,
                                                      headDate=headDate,
                                                      tailDate=tailDate)
        outStats = pd.DataFrame([])
        for indType in indicators:
            indData = indValues[indType]
            indAvg = indData.mean()
            indStd = indData.std()
            indTst = indAvg/indStd*np.sqrt(indData.shape[0])
            outStats = pd.concat([outStats,
                                  indAvg.to_frame('avg_{}'.format(indType)),
                                  indTst.to_frame('tstat_{}'.format(indType))],
                                 axis=1)
        outFile = os.path.join(outPath,'{0}_statistics.csv'.format(factorName))
        outStats.to_csv(outFile)


if __name__=='__main__':
    obj = FactorShow(r'D:\AlphaQuant\FactorPool\factors_data')
    obj.show_curves(factorName='positiveMom5', indicator='tbdf', responses=[alr.OC1,alr.CCG1,alr.OCG1])
    # obj.show_statistics(factorName='positiveMom5',outPath='D:\AlphaQuant')