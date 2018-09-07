__author__ = 'wangjp'

from FactorModule.FactorUpdate import update_factors



if __name__=='__main__':
    update_factors(factorList=None,
                   factorDefPath=r'..\FactorPool\factors_wangjp',
                   factorDataPath=r'..\FactorPool\factors_data',
                   startOver=False,)