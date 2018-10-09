# coding=utf8
__author__ = 'wangjp'

import os
from FactorModule.FactorUpdate import update_factors



if __name__=='__main__':
    factorDefPath = r'..\FactorPool\factors_wangjp'     # 因子定义路径
    factorDataPath = r'..\FactorPool\factors_data'      # 因子数据路径

    newFcts = list(set([fct.split('.')[0] for fct in os.listdir(factorDefPath) if fct not in ['__init__.py', '__pycache__']]) - set(os.listdir(factorDataPath)))
    print(newFcts)

    update_factors(factorList=['worldquant41'],
                   factorDefPath=r'..\FactorPool\factors_wangjp',
                   factorDataPath=r'..\FactorPool\factors_data',
                   startOver=True,)