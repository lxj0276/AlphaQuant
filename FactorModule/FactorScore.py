#coding=utf8
__author__ = 'wangjp'

import numpy as np
import pandas as pd

from DataReaderModule.Constants import ALIAS_FIELDS as alf

class FactorScores:
    """
    用于因子打分
    """

    def __init__(self):
        pass

    def factor_scores_section(self, rawFactor, filterX, scoreTypes=('rank', 'zscore'), outliersOut=True):
        """
        因子 横截面 打分函数     # 可以拓展 中性化处理
        :param rawFactor:   原始因子值   应该是一个 以日期、股票代码 为index 的 dataframe
        :param filterX:     x 过滤      应该是一个 以日期、股票代码 为index 的 dataframe
        :param scoreType:  rank, z-score, norminv
        :return:
        """
        factorName = rawFactor.columns[0]
        rawFactor[filterX] = np.nan     # 被过滤掉的股票不应参与打分， 设为空值
        rawFactor.sort_values(by=[alf.DATE, alf.STKCD], inplace=True)
        outScores = {}
        outScores['_'.join([factorName, 'raw'])] = rawFactor.loc[~filterX, :]
        for tp in scoreTypes:
            if tp == 'rank':     # 使用 pct 避免过多 nan 对直接排名 的影响
                rankFactor = rawFactor.groupby(level=alf.DATE,sort=False,as_index=False,group_keys=False).rank(na_option='keep', pct=False)
                rankMax = rankFactor.groupby(level=alf.DATE,sort=False,as_index=True).max()     # 直接在上设置pct=True在全NaN时会报除0错误
                fctScore = rankFactor/rankMax
            elif tp == 'zscore':
                if outliersOut:     # 取极值处理
                    c = 1.5 # 1.483 ~ 3sigma, 采用稍大尽量少过滤一些极端值
                    rawFactor = rawFactor.copy(deep=True)
                    medianFactor = rawFactor.groupby(level=alf.DATE,sort=False).median()
                    madE = c*(rawFactor - medianFactor).abs().groupby(level=alf.DATE,sort=False).median()
                    upper = medianFactor + madE
                    lower = medianFactor - madE
                    upperIdx = rawFactor - upper > 0
                    lowerIdx = rawFactor - lower < 0
                    rawFactor[upperIdx.values] = upper
                    rawFactor[lowerIdx.values] = lower
                meanFactor = rawFactor.groupby(level=alf.DATE,sort=False,as_index=True).mean()
                stdFactor = rawFactor.groupby(level=alf.DATE,sort=False,as_index=True).std()
                fctScore = (rawFactor - meanFactor)/stdFactor
            elif tp == 'norminv':
                raise NotImplementedError
            else:
                raise NotImplementedError
            fctScore = fctScore.loc[~filterX, :]
            fctScore.sort_values(by=[alf.DATE, alf.STKCD])
            outScores['_'.join([factorName, tp])] = fctScore
        return outScores

    def factor_scores_timeseries(self):
        raise NotImplementedError