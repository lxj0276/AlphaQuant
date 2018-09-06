#coding=utf8
__author__ = 'wangjp'

import numpy as np
import pandas as pd

from FactorModule.FactorIO import FactorIO

class FactorShow:

    def __init__(self, fctDataPath):
        self.fctDataPath = fctDataPath
        self.fctIO = FactorIO(fctDataPath=self.fctDataPath)