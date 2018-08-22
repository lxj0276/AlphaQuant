#coding=utf8
__author__ = 'wangjp'

import numpy as np
import pandas as pd

from DataReaderModule.Constants import ALIAS_FIELDS as alf

class FactorTests:
    """
    用于 因子检验
    """

    def __init__(self):
        pass

    def _indicator_section(self, x, y, indicator, minStockNum):
        """
        计算 两列 数据 的横截面指标
        :param x:
        :param y:
        :param indicator:
        :param minStockNum:
        :return:
        """
        xName = x.columns
        yName = y.columns
        y = y.dropna()
        if ('rank' in indicator) or ('Rank' in indicator):     # 需要对 收益率 进行排序
            y = y.groupby(level=alf.DATE, as_index=False, sort=False).rank(pct=True)
        joined = x.join(y, how='inner')
        joined['xy'] = joined[xName].values*joined[yName].values
        validStkCnt = joined[xName].groupby(level=alf.DATE, as_index=True, sort=False).count()
        validStkCnt.columns = ['stkCnt']
        if indicator in ('beta','IC','rankIC'):
            meanX = joined[xName].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            meanY = joined[yName].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            meanXY = joined[['xy']].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            if indicator in ('beta',):
                varX = joined[xName].groupby(level=alf.DATE, as_index=True, sort=False).var(ddof=0)
                validStkCnt[indicator] = (meanX.values*meanY.values - meanXY.values)/varX.values
            else:
                stdX = joined[xName].groupby(level=alf.DATE, as_index=True, sort=False).std(ddof=0)
                stdY = joined[yName].groupby(level=alf.DATE, as_index=True, sort=False).std(ddof=0)
                validStkCnt[indicator] = (meanX.values*meanY.values - meanXY.values)/(stdX.values*stdY.values)
        elif indicator in ('weightedIC','weightedRankIC'):
            pass
        elif indicator in ('tbdf',):
            topRet = joined[(joined[xName]<=0.1).values][yName].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            botRet = joined[(joined[xName]>=0.9).values][yName].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            validStkCnt[indicator] = topRet.values - botRet.values
        elif indicator in ('groupIC',):
            joined['xGroupRank'] = joined[xName.values[0]].map(lambda x : int(np.ceil(x*100)))
            joined.reset_index(inplace=True)
            groupY = joined.groupby(by=[alf.DATE, 'xGroupRank'], as_index=False, sort=False)[yName].mean()
            groupY['xy'] = groupY[yName.values[0]].values*groupY['xGroupRank'].values
            numerator = groupY.groupby(by=alf.DATE,as_index=True,sort=False)['xy'].mean() - \
                        (groupY.groupby(by=alf.DATE,as_index=True,sort=False)['xGroupRank'].mean() *
                         groupY.groupby(by=alf.DATE,as_index=True,sort=False)[yName.values[0]].mean())
            denominator = groupY.groupby(by=alf.DATE, as_index=True, sort=False)[yName.values[0]].std(ddof=0) * \
                          groupY.groupby(by=alf.DATE, as_index=True, sort=False)['xGroupRank'].std(ddof=0)
            validStkCnt[indicator] = numerator/denominator
        else:
            raise NotImplementedError
        validStkCnt.loc[validStkCnt['stkCnt']<minStockNum, indicator] = np.nan
        validStkCnt.sort_values(by=alf.DATE, inplace=True)
        return validStkCnt.loc[:, indicator]


    def factor_indicators_section(self, factorName, factorScores, stockRets, minStockNum=200):
        """
        计算因子 截面统计量 beta IC rankIC weightedIC weightedRankIC groupIC tbdf
        ref : https://mp.weixin.qq.com/s/meGaS8cPcvzz08EvK7oTsg
        factorScores 和 stockRets 的无效值都应该已经被过滤掉
        :param factorScores:   dataFrame of factor Scores, index tdt,stk : raw, rank, zscore for now
        :param stockRets:      dataFrame of stock returns, index tdt,stk : OC1-5 CC2-5 OC10    tot 10 cols
        :param minStockNum:    有效截面结果 所需的 最小股票数量
        :return:
        """
        allIndicators = {'beta': pd.DataFrame([]),
                         'IC':  pd.DataFrame([]),
                         'rankIC': pd.DataFrame([]),
                         'groupIC': pd.DataFrame([]),
                         'tbdf': pd.DataFrame([]),
                         }
        for retType in stockRets:
            allIndicators['beta'][retType] = self._indicator_section(x=factorScores['_'.join([factorName,'zscore'])],
                                                                     y=stockRets[retType],
                                                                     indicator='beta',
                                                                     minStockNum=minStockNum)
            allIndicators['IC'][retType] = self._indicator_section(x=factorScores['_'.join([factorName,'zscore'])],
                                                                   y=stockRets[retType],
                                                                   indicator='IC',
                                                                   minStockNum=minStockNum)
            allIndicators['rankIC'][retType] = self._indicator_section(x=factorScores['_'.join([factorName,'rank'])],
                                                                       y=stockRets[retType],
                                                                       indicator='rankIC',
                                                                       minStockNum=minStockNum)
            allIndicators['tbdf'][retType] = self._indicator_section(x=factorScores['_'.join([factorName,'rank'])],
                                                                     y=stockRets[retType],
                                                                     indicator='tbdf',
                                                                     minStockNum=minStockNum)
            allIndicators['groupIC'][retType] = self._indicator_section(x=factorScores['_'.join([factorName,'rank'])],
                                                                        y=stockRets[retType],
                                                                        indicator='groupIC',
                                                                        minStockNum=minStockNum)
        return allIndicators

