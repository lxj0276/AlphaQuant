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
import asyncio
import multiprocessing as mlp



t = pd.read_hdf(r'D:\AlphaQuant\data\update\RESPONSE_UPDATE.h5',key='RESPONSE', where='TRADE_DT>="20180820"')

raise

def writedata(name, data):
    data.to_hdf(name,key='test',format='table',mode='w')

if __name__=='__main__':

    data = pd.DataFrame(np.random.rand(100,30))

    start1 = time.time()
    for dumi in range(10000):
        name = 'data{}.h5'.format(dumi)
        writedata(os.path.join(r'C:\Users\Administrator\Desktop\test',name), data)
    print('normal', time.time() - start1)

    pool = mlp.Pool(3)
    start1 = time.time()
    for dumi in range(10000):
        name = 'data{}.h5'.format(dumi)
        pool.apply_async(func=writedata, args=[os.path.join(r'C:\Users\Administrator\Desktop\test1',name), data])
    pool.close()
    pool.join()
    print('multiprocess',time.time() - start1)

# start = time.time()
# threads = []
# for dumi in range(10):
#     name = 'data{}.h5'.format(dumi)
#     threads.append(threading.Thread(target=writedata, args=[os.path.join(r'.\test',name), data]))
# for dumi in range(10):
#     threads[dumi].start()
# print(1)
# for dumi in range(10):
#     threads[dumi].join()
# print(time.time() - start)

# async def writedata(name, data):
#     data.to_hdf(name,key='test',format='table',mode='w')
#
# tasks = []
# for dumi in range(10):
#     name = 'data{}.h5'.format(dumi)
#     crou = writedata(name, data)
#     tasks.append(asyncio.ensure_future(crou))
#
# start = time.time()
# loop = asyncio.get_event_loop()
# loop.run_until_complete(asyncio.wait(tasks))
# print(time.time() - start)