

import os
import time

from FactorModule.__update__ import update


def update_all_factors(factorPath, factorList=None, startOver=True):
    allFactors = os.listdir(factorPath)
    allFactors.remove('__init__.py')
    allFactors.remove('__pycache__')

    update.startOver = startOver
    print(allFactors)
    for fct in allFactors:
        print(fct)
        __import__('FactorPool.factors_wangjp.{}'.format(fct.split('.')[0]))


if __name__=='__main__':
    update_all_factors(factorPath=r'..\FactorPool\factors_wangjp', startOver=False)