#coding=utf8
__author__ = 'wangjp'
import time
import numpy as np
import pandas as pd

from DataReaderModule.Constants import ALIAS_FIELDS as alf

class FactorTests:
    """
    用于 因子检验
    """

    def __init__(self):
        pass

    def _indicator_section(self, x, y, indicator):
        """
        计算 两列 数据 的横截面指标
        :param x:
        :param y:
        :param indicator:
        :return:
        """
        start = time.time()
        xName = x.columns
        yName = y.columns
        y = y.dropna()
        if ('rank' in indicator) or ('Rank' in indicator):     # 需要对 收益率 进行排序
            y = y.groupby(level=alf.DATE, as_index=False, sort=False).rank(pct=True)
        joined = x.join(y, how='inner')
        # joined['xy'] = joined[xName].values*joined[yName].values
        xy = pd.DataFrame(joined[xName].values*joined[yName].values, columns=yName, index=joined.index)
        validStkCnt = joined[xName].groupby(level=alf.DATE, as_index=True, sort=False).count()
        validStkCnt.columns = ['stkCnt']
        if indicator in ('beta','IC','rankIC'):
            groupObj = joined.groupby(level=alf.DATE, as_index=True, sort=False)
            meanX = groupObj[xName].mean()
            meanY = groupObj[yName].mean()
            meanXY = xy.groupby(level=alf.DATE, as_index=True, sort=False).mean()
            if indicator in ('beta',):
                varX = groupObj[xName].var(ddof=0)
                indiResult = (meanX.values*meanY.values - meanXY.values)/varX.values
            else:
                stdX = groupObj[xName].std(ddof=0)
                stdY = groupObj[yName].std(ddof=0)
                indiResult = (meanX.values*meanY.values - meanXY.values)/(stdX.values*stdY.values)
        elif indicator in ('weightedIC','weightedRankIC'):
            pass
        elif indicator in ('tbdf',):
            topRet = joined[(joined[xName]<=0.1).values][yName].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            botRet = joined[(joined[xName]>=0.9).values][yName].groupby(level=alf.DATE, as_index=True, sort=False).mean()
            indiResult = (topRet - botRet).values
        elif indicator in ('groupIC',):
            # joined['xGroupRank'] = joined[xName.values[0]].map(lambda x : int(np.ceil(x*100)))  # 将X值对应分成100组
            joined.reset_index(inplace=True)
            joined['xGroupRank'] = np.ceil(joined[xName].values).astype(np.int)
            groupY = joined.groupby(by=[alf.DATE, 'xGroupRank'], as_index=False, sort=False)[yName].mean()  # 计算每组平均收益
            groupXY = pd.DataFrame(groupY[yName].values*groupY[['xGroupRank']].values, columns=yName)
            groupXY[alf.DATE] = groupY[alf.DATE]
            groupYObj = groupY.groupby(by=alf.DATE, as_index=True, sort=False)
            groupXYObj = groupXY.groupby(by=alf.DATE, as_index=True, sort=False)
            numerator = groupXYObj.mean().values - (groupYObj[['xGroupRank']].mean().values * groupYObj[yName.values].mean().values)
            denominator = groupXYObj.std(ddof=0).values * groupYObj[['xGroupRank']].std(ddof=0).values
            indiResult = numerator/denominator
        else:
            raise NotImplementedError
        indicatorsOut = pd.DataFrame(indiResult, columns=yName, index=validStkCnt.index)
        indicatorsOut = validStkCnt.join(indicatorsOut)

        print('{0} updated with {1} seconds'.format(indicator, time.time()-start))
        return indicatorsOut


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
        start = time.time()
        allIndicators = {}

        allIndicators['beta'] = self._indicator_section(x=factorScores['_'.join([factorName,'zscore'])],
                                                        y=stockRets,
                                                        indicator='beta',)
        allIndicators['IC'] = self._indicator_section(x=factorScores['_'.join([factorName,'zscore'])],
                                                      y=stockRets,
                                                      indicator='IC',)
        allIndicators['rankIC'] = self._indicator_section(x=factorScores['_'.join([factorName,'rank'])],
                                                          y=stockRets,
                                                          indicator='rankIC',)
        allIndicators['tbdf'] = self._indicator_section(x=factorScores['_'.join([factorName,'rank'])],
                                                        y=stockRets,
                                                        indicator='tbdf',)
        allIndicators['groupIC'] = self._indicator_section(x=factorScores['_'.join([factorName,'rank'])],
                                                           y=stockRets,
                                                           indicator='groupIC',)

        print('indicators calced with {} seconds'.format(time.time() - start))
        return allIndicators

