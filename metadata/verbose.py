'''
Created on Dec 19, 2012

@author: Yutao
'''

def debug(out):
    try:
        print "[DEBUG]"+out
    except Exception,e:
        print e