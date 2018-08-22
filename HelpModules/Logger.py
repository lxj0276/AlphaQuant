#coding = utf8
__author__ = 'wangjp'

import os
import sys
import logging
import datetime as dt


class Logger:

    def __init__(self, logPath):
        self._logPath = logPath

    def get_logger(self,loggerName, logName, logDate=None):
        logDate = dt.datetime.today().strftime('%Y%m%d') if logDate is None else logDate
        logFile = os.path.join(self._logPath,'{0}_{1}.log'.format(logName, logDate))
        formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        if not os.path.exists(logFile):
            os.system('type NULL > {0}'.format(logFile))
        logger = logging.getLogger(name=loggerName)
        logger.setLevel(level=logging.DEBUG)
        ch = logging.StreamHandler() # 输出到屏幕
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        if not logger.handlers:
            logger.addHandler(ch)
        fh = logging.FileHandler(logFile, mode='a') # 输出到file
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


class LogMark:
    critical = '[=]'
    error = '[-]'
    warning = '[!]'
    info = '[+]'
    debug = '[*]'
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")