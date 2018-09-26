#coding=utf8
import sqlalchemy.types as st


WPFX = 'c##wind'

class DatabaseNames:
    MysqlDaily = 'testdb' #''stocks_data_daily'

class TableNames:
    TRDDATES = 'trade_dates'
    STKBASIC = 'stocks_info'

class FieldNames:
    DATE = 'TRADE_DT'
    STKCD = 'S_INFO_WINDCODE'

TableFieldsDict = {
    'ASHAREDESCRIPTION': ['S_INFO_WINDCODE', 'S_INFO_LISTDATE', 'S_INFO_DELISTDATE'],
    'ASHAREEODPRICES': ['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_OPEN', 'S_DQ_HIGH', 'S_DQ_LOW', 'S_DQ_CLOSE',
                        'S_DQ_PCTCHANGE', 'S_DQ_VOLUME', 'S_DQ_AMOUNT', 'S_DQ_TRADESTATUS', 'S_DQ_ADJFACTOR'],
    'ASHAREEODDERIVATIVEINDICATOR': ['S_INFO_WINDCODE', 'TRADE_DT',
                                     'S_VAL_MV',                     # 当日总市值
                                     'S_DQ_MV',                      # 当日流通市值
                                     # 'S_PQ_HIGH_52W_',               # 52周最高价
                                     # 'S_PQ_LOW_52W_',                # 52周最低价
                                     'S_VAL_PE',                     # 市盈率(PE)
                                     'S_VAL_PB_NEW',                 # 市净率(PB)
                                     'S_VAL_PE_TTM',                 # 市盈率(PE,TTM)
                                     'S_VAL_PCF_OCF',                # 市现率(PCF,经营现金流)
                                     'S_VAL_PCF_OCFTTM',             # 市现率(PCF,经营现金流TTM)
                                     'S_VAL_PCF_NCF',                # 市现率(PCF,现金净流量)
                                     'S_VAL_PCF_NCFTTM',             # 市现率(PCF,现金净流量TTM)
                                     'S_VAL_PS',                     # 市销率(PS)
                                     'S_VAL_PS_TTM',                 # 市销率(PS,TTM)
                                     'S_DQ_TURN',                    # 换手率
                                     'S_DQ_FREETURNOVER',            # 换手率(基准.自由流通股本)
                                     'TOT_SHR_TODAY',                # 当日总股本
                                     'FLOAT_A_SHR_TODAY',            # 当日流通股本
                                     # 'S_DQ_CLOSE_TODAY',             # 当日收盘价
                                     'S_PRICE_DIV_DPS',              # 股价/每股派息
                                     'S_PQ_ADJHIGH_52W',             # 52周最高价(复权)
                                     'S_PQ_ADJLOW_52W',              # 52周最低价(复权)
                                     'FREE_SHARES_TODAY',            # 当日自由流通股本
                                     'NET_PROFIT_PARENT_COMP_TTM',   # 归属母公司净利润(TTM)
                                     'NET_PROFIT_PARENT_COMP_LYR',   # 归属母公司净利润(LYR)
                                     'NET_ASSETS_TODAY',             # 当日净资产
                                     'NET_CASH_FLOWS_OPER_ACT_TTM',  # 经营活动产生的现金流量净额(TTM)
                                     'NET_CASH_FLOWS_OPER_ACT_LYR',  # 经营活动产生的现金流量净额(LYR)
                                     'OPER_REV_TTM',                 # 营业收入(TTM)
                                     'OPER_REV_LYR',                 # 营业收入(LYR)
                                     'NET_INCR_CASH_CASH_EQU_TTM',   # 现金及现金等价物净增加额(TTM)
                                     'NET_INCR_CASH_CASH_EQU_LYR',   # 现金及现金等价物净增加额(LYR)
                                     # 'UP_DOWN_LIMIT_STATUS',         # 涨跌停状态
                                     # 'LOWEST_HIGHEST_STATUS',        # 最高最低价状态
                                     ],
    'ASHAREST': ['S_INFO_WINDCODE', 'S_TYPE_ST', 'ENTRY_DT', 'REMOVE_DT', 'ANN_DT'],
}


FieldTypeDict = {
    # basic info
    'S_INFO_WINDCODE': st.VARCHAR(40),
    'S_INFO_NAME': st.VARCHAR(40),
    'S_INFO_LISTDATE': st.VARCHAR(8),
    'S_INFO_DELISTDATE': st.VARCHAR(8),
    # share info
    'ANN_DT': st.VARCHAR(8),
    'NEXT_ANN_DT': st.VARCHAR(8),
    'CHANGE_DT': st.VARCHAR(8),
    'TOT_SHR': st.FLOAT,
    'FLOAT_SHR': st.FLOAT,
    'FLOAT_A_SHR': st.FLOAT,
    'S_SHARE_TOTALA': st.FLOAT,
    # holders info
    'TOT_HOLDERS': st.BigInteger(),
    'PERS_HOLDERS': st.BigInteger(),
    'INST_HOLDERS':st.BigInteger(),
    'TOT_QUANTITY':st.BigInteger(),
    'PERS_QUANTITY':st.BigInteger(),
    'INST_QUANTITY':st.BigInteger(),
    # trade_info
    'TRADE_DT': st.VARCHAR(8),
    'S_DQ_OPEN': st.FLOAT,
    'S_DQ_HIGH': st.FLOAT,
    'S_DQ_LOW': st.FLOAT,
    'S_DQ_CLOSE': st.FLOAT,
    'S_DQ_VOLUME': st.FLOAT,
    'S_DQ_AMOUNT': st.FLOAT,
    'S_DQ_PCTCHANGE': st.FLOAT,
    'S_DQ_TRADESTATUS': st.INTEGER,
    # st info
    'ENTRY_DT':st.VARCHAR(8),
    'REMOVE_DT':st.VARCHAR(8),
    'TYPE_ST': st.BOOLEAN,
    'S_TYPE_ST': st.TEXT,
}



# 用于写入quick table
quickTableDict = {
    'ASHARECAPITALIZATION': {
        'bounds': ['ANN_DT','NEXT_ANN_DT'],
        'fields': ['ANN_DT', 'NEXT_ANN_DT',' TOT_SHR', 'FLOAT_SHR', 'FLOAT_A_SHR', 'S_SHARE_TOTALA']
    },
    'ASHAREFLOATHOLDER': {
        'bounds': ['ANN_DT','NEXT_ANN_DT'],
        'fields': ['ANN_DT', 'NEXT_ANN_DT', 'TOT_HOLDERS', 'PERS_HOLDERS', 'INST_HOLDERS', 'TOT_QUANTITY', 'PERS_QUANTITY', 'INST_QUANTITY']
    },
    'ASHAREST': {
        'bounds': ['ENTRY_DT', 'REMOVE_DT'],
        'fields': ['ENTRY_DT', 'REMOVE_DT', 'S_TYPE_ST']
    }
}