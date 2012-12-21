'''
Created on Dec 18, 2012

@author: Yutao
'''
from src.metadata import settings
import MySQLdb

SQL_GET_PERSON = "SELECT * FROM na_person"
SQL_GET_PROFILE = "SELECT * FROM person_profile_ext"
SQL_GET_PERSON_RELATION = "SELECT * FROM `na_person_relation` r WHERE r.pid1 = %s or r.pid2 = %s"
SQL_GET_PERSON_RANK = "SELECT rank FROM person_ext WHERE person_id = %s and type = 2 and topic = -1"

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
    
    def get_person_relation(self, pid):
        self.cur.execute(SQL_GET_PERSON_RELATION % (pid, pid))
        return self.cur.fetchall()
    
    def get_person_rank(self, pid):
        self.cur.execute(SQL_GET_PERSON_RANK % pid)
        return self.cur.fetchall()