'''
Created on Dec 18, 2012

@author: Yutao
'''
from src.metadata import settings
import MySQLdb

SQL_GET_PERSON = "SELECT * FROM na_person where id<100"
SQL_GET_PROFILE = "SELECT * FROM person_profile_ext"

class Mysql(object):
    def __init__(self):
        self.db = MySQLdb.connect(host=settings.DB_HOST,
                                       user=settings.DB_USER,
                                       passwd=settings.DB_PASS,
                                       db=settings.DB_NAME)
        self.cur = self.db.cursor()
        
    def fetch_person(self):
        self.cur.execute(SQL_GET_PERSON)
        return self.cur.fetchall()