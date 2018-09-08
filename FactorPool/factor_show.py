#coding=utf8
__author__ = 'wangjp'


from FactorModule.FactorShow import FactorShow
from DataReaderModule.Constants import ALIAS_RESPONSE as alr
from DataReaderModule.Constants import ALIAS_INDICATORS as ali

if __name__=='__main__':
    obj = FactorShow(r'D:\AlphaQuant\FactorPool\factors_data')
    obj.show_curves(factorName='mom5',
                    indicator='tbdf',
                    headDate=20170101,
                    responses=[alr.OC1, alr.CCG1, alr.OCG1, alr.CCG2, alr.OCG2])
    # obj.show_statistics(factorName='positiveMom5',outPath='D:\AlphaQuant')