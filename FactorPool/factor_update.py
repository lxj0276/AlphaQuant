__author__ = 'wangjp'

import os
from FactorModule.FactorUpdate import update_factors



if __name__=='__main__':
    factorDefPath = r'..\FactorPool\factors_wangjp'
    factorDataPath = r'..\FactorPool\factors_data'

    newFcts = list(set([fct.split('.')[0] for fct in os.listdir(factorDefPath) if fct not in ['__init__.py', '__pycache__']]) - set(os.listdir(factorDataPath)))
    print(newFcts)

    update_factors(factorList=['wms5'],
                   factorDefPath=r'..\FactorPool\factors_wangjp',
                   factorDataPath=r'..\FactorPool\factors_data',
                   startOver=True,)