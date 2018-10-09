import os
import cx_Oracle
from sqlalchemy import create_engine
import pymysql
import mysql.connector
import sqlalchemy.types as st
pymysql.install_as_MySQLdb()
import pandas as pd
import configparser as cp

from FactorModule.FactorScore import FactorScores
from FactorModule.FactorTests import FactorTests
from CalculatorModule.Calculator import Calculator



date = 'TRADE_DT'
stkcd = 'S_INFO_WINDCODE'
anndt = 'ANN_DT'
nxtanndt = 'NEXT_ANN_DT'
rept = 'REPORT_PERIOD'

def get_next_anndt(data):
    needData = data.loc[:, [stkcd, anndt]]
    needData = needData.drop_duplicates()
    needData.sort_values(by=[stkcd,anndt], inplace=True)
    needData[nxtanndt] = needData.groupby(by=[stkcd], sort=False)[anndt].shift(-1)
    return needData


if __name__=='__main__':

    rootPath = r'D:\AlphaQuant'
    config = os.path.join(rootPath, 'Configs', 'loginInfo.ini')
    cfp = cp.ConfigParser()
    cfp.read(config)
    loginfoWind = dict(cfp.items('Wind'))
    conn = cx_Oracle.connect(r'{user}/{password}@{host}/{database}'.format(**loginfoWind))
    cursor = conn.cursor()

    loginfoMysql = dict(cfp.items('Mysql'))
    connMysqlWrite = create_engine(r'mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset={charset}'.format(**loginfoMysql))
    connMysqlRead = mysql.connector.connect(user=loginfoMysql['user'],
                                            password=loginfoMysql['password'],
                                            host=loginfoMysql['host'])

    # columns = [stkcd, anndt, rept, 'S_FA_OCFTOOR']
    # sqlLine = 'SELECT {0} FROM C##WIND.ASHAREFINANCIALINDICATOR WHERE REPORT_PERIOD>=20170101'.format(','.join(columns))
    # finInd = pd.read_sql(con=conn, sql=sqlLine)
    # finInd.sort_values(by=[stkcd, rept, anndt], inplace=True)
    # # finNxtInd = get_next_anndt(data=finInd)
    # # finInd = finInd.merge(finNxtInd, on=[stkcd, anndt], how='left')
    # finInd[nxtanndt] = finInd.groupby(by=[stkcd],as_index=False,sort=False).shift(-1)[anndt]
    # idx = finInd[nxtanndt] < finInd[anndt]
    # finInd.loc[idx, anndt] = finInd[idx][nxtanndt]
    # finInd[nxtanndt] = finInd.groupby(by=[stkcd], as_index=False, sort=False).shift(-1)[anndt]
    # pd.io.sql.to_sql(finInd,
    #                  name='ASHAREFINANCIALINDICATOR',
    #                  con=connMysqlWrite,
    #                  if_exists='replace',
    #                  chunksize=2000,
    #                  index=False,
    #                  dtype={stkcd:st.VARCHAR(40),
    #                         anndt:st.VARCHAR(8),
    #                         nxtanndt:st.VARCHAR(8),
    #                         rept:st.VARCHAR(8),
    #                         'S_FA_OCFTOOR':st.Float})


    # columns = [stkcd, anndt, rept, 'MONETARY_CAP','INVENTORIES','TOT_ASSETS']
    # sqlLine = 'SELECT {0} FROM C##WIND.ASHAREBALANCESHEET WHERE REPORT_PERIOD>=20170101 AND STATEMENT_type=408001000'.format(','.join(columns))
    # balance = pd.read_sql(con=conn, sql=sqlLine)
    # balance.sort_values(by=[stkcd, rept, anndt], inplace=True)
    # # balanceNxt = get_next_anndt(data=balance)
    # # balance = balance.merge(balanceNxt, on=[stkcd, anndt], how='left')
    # balance[nxtanndt] = balance.groupby(by=[stkcd],as_index=False,sort=False).shift(-1)[anndt]
    # balance['inv2ass'] = balance['INVENTORIES']/balance['TOT_ASSETS']
    # balance['inv2assGth'] = balance['inv2ass']/balance.groupby(by=stkcd,as_index=False, sort=False).shift(1)['inv2ass']-1
    # balance = balance.loc[:, [stkcd, anndt, nxtanndt, rept, 'inv2assGth', 'MONETARY_CAP']]
    # idx = balance[nxtanndt] < balance[anndt]
    # balance.loc[idx, anndt] = balance[idx][nxtanndt]
    # balance[nxtanndt] = balance.groupby(by=[stkcd], as_index=False, sort=False).shift(-1)[anndt]
    # pd.io.sql.to_sql(balance,
    #                  name='ASHAREBALANCESHEET',
    #                  con=connMysqlWrite,
    #                  if_exists='replace',
    #                  chunksize=2000,
    #                  index=False,
    #                  dtype={stkcd:st.VARCHAR(40),
    #                         anndt:st.VARCHAR(8),
    #                         nxtanndt:st.VARCHAR(8),
    #                         rept:st.VARCHAR(8),
    #                         'inv2assGth':st.Float,
    #                         'MONETARY_CAP': st.Float,
    #                         })

    newCursor = connMysqlRead.cursor()
    newCursor.execute('use testdb')

    tbCols = {'ASHAREEODPRICES': [date, stkcd, 'S_DQ_TRADESTATUS'],
              'ASHAREFINANCIALINDICATOR': ['S_FA_OCFTOOR'],
              'ASHAREBALANCESHEET': ['MONETARY_CAP', 'inv2assGth']
              }


    # temp = pd.read_sql(sql='SELECT TRADE_DT, S_INFO_WINDCODE, S_DQ_TRADESTATUS FROM ASHAREEODPRICES WHERE TRADE_DT>=20180101', con=connMysqlRead)
    # pd.io.sql.to_sql(temp,
    #                  name='trd_temp',
    #                  con=connMysqlWrite,
    #                  if_exists='replace',
    #                  chunksize=2000,
    #                  index=False,
    #                  dtype={stkcd:st.VARCHAR(40),
    #                         anndt:st.VARCHAR(8),
    #                         'S_DQ_TRADESTATUS': st.INT,
    #                         })

    # sqlLines1 = ''.join(['SELECT {0},r.S_FA_OCFTOOR FROM trd_temp AS l LEFT JOIN ASHAREFINANCIALINDICATOR AS r ON '.format(','.join(['.'.join(['l',cl]) for cl in tbCols['ASHAREEODPRICES']])),
    #                      'l.S_INFO_WINDCODE=r.S_INFO_WINDCODE AND ((l.TRADE_DT > r. ANN_DT AND l.TRADE_DT<=r.NEXT_ANN_DT) OR (l.TRADE_DT > r.ANN_DT AND r.NEXT_ANN_DT IS NULL));'])
    # sqlLines2 = ''.join(['SELECT {0},r.MONETARY_CAP, r.inv2assGth FROM trd_temp AS l LEFT JOIN ASHAREBALANCESHEET AS r ON '.format(','.join(['.'.join(['l',cl]) for cl in tbCols['ASHAREEODPRICES']])),
    #                      'l.S_INFO_WINDCODE=r.S_INFO_WINDCODE AND ((l.TRADE_DT > r. ANN_DT AND l.TRADE_DT<=r.NEXT_ANN_DT) OR (l.TRADE_DT > r.ANN_DT AND r.NEXT_ANN_DT IS NULL));'])
    # print(sqlLines1)
    # print(sqlLines2)
    #
    # data1 = pd.read_sql(sql=sqlLines1, con=connMysqlRead)
    # data1.sort_values(by = ['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
    # data1.set_index(['TRADE_DT','S_INFO_WINDCODE'], inplace=True)
    # print(data1.shape)
    # data1.to_hdf('data1.h5',
    #              key='data1',
    #              mode='w',
    #              format='table',
    #              append=True,
    #              complevel=4)
    #
    # data2 = pd.read_sql(sql=sqlLines2, con=connMysqlRead)
    # data2.sort_values(by=['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
    # data2.set_index(['TRADE_DT', 'S_INFO_WINDCODE'], inplace=True)
    # print(data2.shape)
    # data2.to_hdf('data2.h5',
    #              key='data2',
    #              mode='w',
    #              format='table',
    #              append=True,
    #              complevel=4)


    data1 = pd.read_hdf('data1.h5', key='data1')
    data2 = pd.read_hdf('data2.h5', key='data2')

    data = data1.join(data2, rsuffix='r')
    data.drop('S_DQ_TRADESTATUSr', inplace=True, axis=1)

    response = pd.read_hdf(r'D:\AlphaQuant\data\response.h5', where='TRADE_DT>="20180101"')
    data = data.join(response[['OCDay10']])
    data = data[~data['S_DQ_TRADESTATUS'].isin((5,6))]

    fctTest = FactorTests()
    fctScore = FactorScores()

    filterX = data['S_DQ_TRADESTATUS'].isin((5, 6))

    outIC = None
    for fct in ['S_FA_OCFTOOR', 'MONETARY_CAP', 'inv2assGth']:
        fctScores = fctScore.factor_scores_section(rawFactor=data.loc[:, [fct]],
                                                   filterX=filterX,
                                                   scoreTypes=('zscore',),
                                                   outliersOut=True)
        fctIC = fctTest._indicator_section(fctScores['{}_zscore'.format(fct)], data[['OCDay10']], 'IC')
        fctIC = fctIC.loc[:, ['OCDay10']]
        fctIC.columns = [fct]
        outIC = fctIC.loc[:, [fct]] if outIC is None else outIC.join(fctIC.loc[:, [fct]])
    outIC.to_csv('factors_IC_2018.csv')