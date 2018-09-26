__author__ = 'wangjp'


rootPath = r'D:\AlphaQuant'

class DatabaseNames:
    MysqlDaily = 'testdb' #''stocks_data_daily'

class ALIAS_TABLES:         # 表名标记
    TRDDATES = 'trade_dates'
    STKBASIC = 'stocks_info'
    DAILYCNT = 'daily_stocks_count'
    XFILTER = 'FEATURES_FILTER'
    YFILTER = 'RESPONSE_FILTER'
    RESPONSE = 'RESPONSE'
    TRAEDINFO = 'ASHAREEODPRICES'
    DERIVINFO = 'ASHAREEODDERIVATIVEINDICATOR'

class ALIAS_FIELDS:         # 基础数据字段标记
    DATE = 'TRADE_DT'
    STKCD = 'S_INFO_WINDCODE'
    STKCNT = 'DAILY_COUNT'

    OPEN = 'S_DQ_OPEN'
    HIGH = 'S_DQ_HIGH'
    LOW = 'S_DQ_LOW'
    CLOSE = 'S_DQ_CLOSE'
    VOLUME = 'S_DQ_VOLUME'
    AMOUNT = 'S_DQ_AMOUNT'
    PCTCHG = 'S_DQ_PCTCHANGE'
    TRDSTAT = 'S_DQ_TRADESTATUS'
    ADJFCT = 'S_DQ_ADJFACTOR'
    STSTAT = 'TYPE_ST'

class ALIAS_RESPONSE:
    OC1 = 'OCDay1'
    CC1 = 'CCDay1'
    OC10 = 'OCDay10'
    OCG1 = 'OCDay1Gap1'
    CCG1 = 'CCDay1Gap1'
    OCG2 = 'OCDay1Gap2'
    CCG2 = 'CCDay1Gap2'
    OCG3 = 'OCDay1Gap3'
    CCG3 = 'CCDay1Gap3'
    OCG4 = 'OCDay1Gap4'
    CCG4 = 'CCDay1Gap4'

class ALIAS_STATUS:        # 状态字段标记
    NOTRD = 'NOTRADE'
    PNOTRD = 'PRENOTRADE'
    ISST = 'ISST'
    LMUP = 'LIMITUP'
    LMDW = 'LIMITDOWN'
    INSFAMT = 'INSFAMT'
    INSFTRD = 'INSFTRADE'
    INSFLST = 'INSFLIST'
    INSFRSM = 'INSFRESUM'

class ALIAS_INDICATORS:
    BETA = 'beta'
    IC = 'IC'
    RKIC = 'rankIC'
    GPIC = 'groupIC'
    TBDF = 'tbdf'


NO_QUICK = ['ASHAREEODPRICES', 'FEATURES_FILTER', 'RESPONSE_FILTER', 'RESPONSE']

QuickTableFieldsDict = {
    'FEATURES_FILTER': ['NOTRADE', 'PRENOTRADE', 'ISST', 'LIMITUP', 'LIMITDOWN', 'INSFAMT', 'INSFTRADE', 'INSFLIST',
                        'INSFRESUM', 'FilterX'],
    'RESPONSE_FILTER': ['OCRet', 'CORet', 'CCRet', 'NOTRADE', 'ISST', 'COLIMITUP', 'COLIMITDOWN', 'CCLIMITUP',
                        'CCLIMITDOWN'],
    'RESPONSE': ['OCDay1', 'CCDay1', 'OCDay10', 'OCDay1Gap1', 'CCDay1Gap1', 'OCDay1Gap2', 'CCDay1Gap2', 'OCDay1Gap3',
                 'CCDay1Gap3', 'OCDay1Gap4', 'CCDay1Gap4'],
    'ASHAREEODPRICES': ['S_DQ_OPEN', 'S_DQ_HIGH', 'S_DQ_LOW', 'S_DQ_CLOSE', 'S_DQ_VOLUME', 'S_DQ_AMOUNT',
                        'S_DQ_PCTCHANGE', 'S_DQ_TRADESTATUS', 'S_DQ_ADJFACTOR'],
    'ASHAREST': ['TYPE_ST'],
    'ASHAREFLOATHOLDER': ['TOT_HOLDERS', 'PERS_HOLDERS', 'INST_HOLDERS', 'TOT_QUANTITY', 'PERS_QUANTITY',
                          'INST_QUANTITY'],
    'ASHARECAPITALIZATION': ['TOT_SHR', 'FLOAT_SHR', 'FLOAT_A_SHR', 'S_SHARE_TOTALA'],
    'ASHAREEODDERIVATIVEINDICATOR': [
                                     'S_VAL_MV',  # 当日总市值
                                     'S_DQ_MV',  # 当日流通市值
                                     # 'S_PQ_HIGH_52W_',               # 52周最高价
                                     # 'S_PQ_LOW_52W_',                # 52周最低价
                                     'S_VAL_PE',  # 市盈率(PE)
                                     'S_VAL_PB_NEW',  # 市净率(PB)
                                     'S_VAL_PE_TTM',  # 市盈率(PE,TTM)
                                     'S_VAL_PCF_OCF',  # 市现率(PCF,经营现金流)
                                     'S_VAL_PCF_OCFTTM',  # 市现率(PCF,经营现金流TTM)
                                     'S_VAL_PCF_NCF',  # 市现率(PCF,现金净流量)
                                     'S_VAL_PCF_NCFTTM',  # 市现率(PCF,现金净流量TTM)
                                     'S_VAL_PS',  # 市销率(PS)
                                     'S_VAL_PS_TTM',  # 市销率(PS,TTM)
                                     'S_DQ_TURN',  # 换手率
                                     'S_DQ_FREETURNOVER',  # 换手率(基准.自由流通股本)
                                     'TOT_SHR_TODAY',  # 当日总股本
                                     'FLOAT_A_SHR_TODAY',  # 当日流通股本
                                     # 'S_DQ_CLOSE_TODAY',             # 当日收盘价
                                     'S_PRICE_DIV_DPS',  # 股价/每股派息
                                     'S_PQ_ADJHIGH_52W',  # 52周最高价(复权)
                                     'S_PQ_ADJLOW_52W',  # 52周最低价(复权)
                                     'FREE_SHARES_TODAY',  # 当日自由流通股本
                                     'NET_PROFIT_PARENT_COMP_TTM',  # 归属母公司净利润(TTM)
                                     'NET_PROFIT_PARENT_COMP_LYR',  # 归属母公司净利润(LYR)
                                     'NET_ASSETS_TODAY',  # 当日净资产
                                     'NET_CASH_FLOWS_OPER_ACT_TTM',  # 经营活动产生的现金流量净额(TTM)
                                     'NET_CASH_FLOWS_OPER_ACT_LYR',  # 经营活动产生的现金流量净额(LYR)
                                     'OPER_REV_TTM',  # 营业收入(TTM)
                                     'OPER_REV_LYR',  # 营业收入(LYR)
                                     'NET_INCR_CASH_CASH_EQU_TTM',  # 现金及现金等价物净增加额(TTM)
                                     'NET_INCR_CASH_CASH_EQU_LYR',  # 现金及现金等价物净增加额(LYR)
                                     # 'UP_DOWN_LIMIT_STATUS',         # 涨跌停状态
                                     # 'LOWEST_HIGHEST_STATUS',        # 最高最低价状态
                                     ],

}

CacheLevlels = {
    'LEVEL1' : ['ASHAREEODPRICES', 'FEATURES_FILTER', 'RESPONSE_FILTER', 'RESPONSE'],
    'LEVEL2' : ['ASHARECAPITALIZATION', 'ASHAREFLOATHOLDER']
}
