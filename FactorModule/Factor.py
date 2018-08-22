__author__ = 'wangjp'

import time

from CalculatorModule.Calculator import Calculator
from FactorModule.FactorBase import FactorBase
from DBReaderModule.Constants import ALIAS_FIELDS

class Factor(FactorBase):

    def __init__(self):
        super(Factor,self).__init__()
        # put remote conn here if need extra data

    def factor_definition(self):
        pass

    def run_factor(self, headDate=None, tailDate=None):
        self.run()

if __name__=='__main__':
    fct = Factor()
    fct.run_factor()