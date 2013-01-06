'''
Created on Dec 23, 2012

@author: Yutao
'''
import matplotlib.pyplot as plt
import pymongo 

def plot_labeled_data_similariy():
    col = pymongo.Connection("10.1.1.110")['scrapy']['labeled_data']
    sim = []
    not_sim = []
    for item in col.find():
        if item['sim']>0:
            sim.append(item['sim'])
        else:
            not_sim.append(item['_id'])
    plt.plot(sim)
    plt.show()
    plt.hist(sim, 100)
    plt.show()