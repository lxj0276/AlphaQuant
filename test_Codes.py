# coding=utf8
import cx_Oracle
import pandas as pd
import numpy as np
import sqlalchemy
import time

import pickle
import re
import os

import threading

# s = time.time()
# b = pd.DataFrame({'a':range(10),'b':range(1,11),'c':range(10)})
# b.set_index(['a','b'], inplace=True)
# print(b)

# b.to_hdf('test.h5',key='test',mode='a',format='table')


# ind = True
# with pd.HDFStore(r'D:\AlphaQuant\FactorPool\factors_data\mom5\factor_scores.h5',complevel=4, complib='blosc') as h5:
#     print(h5.info())
#     # data = h5.select(key='zscore',start=0)
#     data = h5.select(key='mom5', start=-1)
#     # data = h5.select(key='rank',start=4749300)


def writedata(name, data):
    data.to_hdf(name,key='test',format='table',mode='w')

data = pd.DataFrame(np.random.rand(100000,300))

start = time.time()
for dumi in range(10):
    name = 'data{}.h5'.format(dumi)
    writedata(name, data)
print(time.time() - start)

start = time.time()
threads = []
for dumi in range(10):
    name = 'data{}.h5'.format(dumi)
    threads.append(threading.Thread(target=writedata, args=[name, data]))
for dumi in range(10):
    threads[dumi].start()
print(1)
for dumi in range(10):
    threads[dumi].join()
print(time.time() - start)