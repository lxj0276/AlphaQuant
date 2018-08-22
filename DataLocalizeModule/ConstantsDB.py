#coding=utf8
import sqlalchemy.types as st


class DatabaseNames:
    MysqlDaily = 'testdb' #''stocks_data_daily'

class TableNames:
    TRDDATES = 'trade_dates'
    STKBASIC = 'stocks_basic_info'

WPFX = 'c##wind'

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