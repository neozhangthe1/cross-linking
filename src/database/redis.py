'''
Created on Dec 26, 2012

@author: Yutao
'''
from src.metadata import settings
import redis

def Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB):
    return redis.StrictRedis(host=host, port=port, db=db)

r_client = Redis("10.1.1.110") # 初始化到110的连接
r_client.sismember('linkedin_crawled', 'in-ipondering') # set ismember, 检查in-ipondering在不在set里， 返回True or False
r_client.sadd('linkedin_crawled', 'in-ipondering') # 把set add, 把in-ipondering加入到这个set里。
