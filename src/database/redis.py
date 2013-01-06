'''
Created on Dec 26, 2012

@author: Yutao
'''
from src.metadata import settings
import redis

def Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB):
    return redis.StrictRedis(host=host, port=port, db=db)

r_client = Redis("10.1.1.110") # ????????110??????
r_client.sismember('linkedin_crawled', 'in-ipondering') # set ismember, ????in-ipondering??????set???? ????True or False
r_client.sadd('linkedin_crawled', 'in-ipondering') # ??set add, ??in-ipondering??????????set????
