'''
Created on Dec 18, 2012

@author: Yutao
'''
from src.metadata import settings
import pymongo

class Mongo(object):
    def __init__(self):
        self.con = pymongo.Connection(settings.MONGO_HOST)
        self.db = self.con[settings.MONGO_NAME]
        

class Mongo61(object):
    def __init__(self):
        self.con = pymongo.Connection(settings.MONGO_HOST61)
        self.db = self.con[settings.MONGO_NAME]