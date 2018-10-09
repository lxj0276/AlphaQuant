#coding=utf8
__author__ = 'wangjp'


from FactorModule.FactorShow import FactorShow
from DataReaderModule.Constants import ALIAS_RESPONSE as alr
from DataReaderModule.Constants import ALIAS_INDICATORS as ali

if __name__=='__main__':
    obj = FactorShow(fctDataPath=r'D:\AlphaQuant\FactorPool\factors_data')

    fctName = 'acd5'
    headDate = 20150601

    obj.show_curves(factorName=fctName,
                    indicator=ali.TBDF,
                    headDate=headDate,
                    responses=[alr.OC1, alr.CCG1, alr.OCG1, alr.CCG2, alr.OCG2])

    # obj.show_statistics(factorName=fctName,
    #                     headDate=headDate,
    #                     outPath='D:\AlphaQuant')