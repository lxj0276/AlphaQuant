

import os
import time

from FactorModule.__update__ import update


def update_factors(factorDefPath, factorDataPath, factorList=None, startOver=True):
    if factorList is None:
        factorList = [fct.split('.')[0] for fct in os.listdir(factorDefPath) if fct not in ('__init__.py','__pycache__')]
    print(factorList)

    update.startOver = startOver
    update.fctDataPath = factorDataPath

    factorPkg = factorDefPath.split('\\')[-1]
    for fct in factorList:
        __import__('FactorPool.{0}.{1}'.format(factorPkg, fct))


if __name__=='__main__':
    update_factors(factorDefPath=r'..\FactorPool\factors_wangjp',
                   factorDataPath=r'..\FactorPool\factors_data',
                   startOver=True)