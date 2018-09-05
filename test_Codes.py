# coding=utf8
import cx_Oracle
import pandas as pd
import numpy as np
import sqlalchemy
import time

import pickle
import re
import os


# s = time.time()
# b = pd.DataFrame({'a':range(10),'b':range(1,11),'c':range(10)})
# b.set_index(['a','b'], inplace=True)
# print(b)

# b.to_hdf('test.h5',key='test',mode='a',format='table')

with pd.HDFStore(r'D:\AlphaQuant\data\update\RESPONSE_UPDATE.h5',complevel=4, complib='blosc') as h5:
    print(h5.info())
    print(h5.select(key='RESPONSE', start=9443747-1))


raise
# # ####压缩格式存储
# h5 = pd.HDFStore('test_c4.h5','w', complevel=4, complib='blosc')
# h5['data'] = b
# h5.close()
# print(time.time()-s)

# h5 = pd.HDFStore('test_c4.h5','a', complevel=4, complib='blosc')
# h5.put('data2',b,format='table',append=True)
# h5.put('data3',b,format='table',append=True)
# t=h5.info()
# s=1
# h5.close()


# s = time.time()
# t = pd.read_hdf(r'D:\AlphaQuant\FactorPool\factors_data\mom5\factor_scores.h5',key='raw')
# print(t.shape)
# print(time.time() - s)


# conn = cx_Oracle.connect(r'c##sensegain/sensegain@220.194.41.75/wind')
# cursor = conn.cursor()
#
#
# print('20180831')
#
# def test(i):
#     return i
#
# def ty(num):
#     for i in range(num):
#         yield test(i)
#
# print(list(ty(10)))
#
#
# import pandas as pd
# import numpy as np
#
# t = np.array([[1,1,1],[2,2,2],[3,3,3],[1,1,1],[2,2,2],[3,3,3]])
#
# a = pd.DataFrame(t,columns=['a','b','c'])
# a.set_index(['a','b'],inplace=True)
# print(a)
# b = pd.DataFrame(t[:3,:2],columns=['a','c'])
# b.set_index(['a'],inplace=True)
# print(b)
# # print(pd.concat([a,b], sort=False))
#
# print(a - b)
# print(a.all(axis=1))
#
# print(sorted(np.array([1,3,2])))