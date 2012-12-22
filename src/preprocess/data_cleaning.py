'''
Created on Dec 21, 2012

@author: Yutao
'''
from src.database import mongo
from src.database.mysql import Mysql
from src.metadata import verbose
from src.metadata import utils
from bs4 import UnicodeDammit
import matplotlib.pyplot as plt

import pickle
    
mongo110 = mongo.Mongo()
mongo61 = mongo.Mongo61()
mysql = Mysql()

def plot_labeled_data_degree():
    col = mongo110.db['labeled_data']
    degree = []
    for item in col.find():
        degree.append(item['rel']['count'])
    plt.hist(degree, 100)
    plt.show()
    

def get_labeled_data_name():
    col = mongo110.db['labeled_data']
    for item in col.find():
        name = mysql.get_person_name(item['aminer'])
        item['aminer_name']=UnicodeDammit(name).markup
        col.save(item)    
    
def gen_labeled_dataset():
    col = mongo110.db["lenin_label_data"]
    labeled_col = mongo110.db["labeled_data"]
    for item in col.find():
        if 'e+' not in item['aminer'] and 'view' not in item['linkedin'] and item['flag']=='1':
            print item['aminer']
            labeled_col.save({'_id':int(item['aminer']),
                              'aminer':int(item['aminer']),
                              'linkedin':utils.get_linkedin_id(item['linkedin']),
                              'url':item['linkedin'],
                              'rank':item['rank'],
                              'rel':item['rel']})

def check_werid_url():
    col = mongo110.db["lenin_label_data"]
    aid = []
    for item in col.find():
        if 'view' in item['linkedin']:
            if item['rank']<40000 and item['flag']=='1':
                verbose.debug(item['aminer'])
                aid.append(item['aminer'])
    

def check_if_lenin_data_exist():
    col = mongo110.db["labeled_data"]
    col61 = mongo110.db['temp_person_profiles']
    lenin_urls = []
    results = []
    not_in_db = []
    index = 0
    count = 0
    for item in col.find():
        if index % 100 == 0:
            verbose.debug(index)
            verbose.debug(len(results))
        index+=1
        lenin_urls.append(item['linkedin'])
        query = col61.find({"_id":item['linkedin']})
        if query.count() == 0:
            verbose.debug("not in")
            verbose.debug(item['linkedin'])
            not_in_db.append(item['url'])
        elif query.count() == 1:
            for res in query:
                verbose.debug(res['_id'])
                results.append(res['url'])
        else:
            verbose.debug("werid")
            verbose.debug(item['linkedin'])
            
def filter_werid_data():
    col = mongo110.db["aminer_linkedin_filtered_1124"]
    count = 0
    index = 0
    for item in col.find():
        print "INDEX "+str(index)
        index+=1
        #print len(item['aminer'].keys())
        if len(item['aminer'].keys())<=2:
            rel = mysql.get_person_relation(item['aminer']['id'])
            print len(rel)
            if len(rel) <5: 
                print item['aminer']['id']
                count+=1
                print count
    print count
    
def check_lenin_data():
    col = mongo110.db["lenin_label_data"]
    count = 0
    index = 0
    data = {"id":[],"rel":[],"rank":[]}
    for item in col.find():
        if index % 100 == 0:
            print "INDEX "+str(index)
        index+=1
        data["id"].append(item['aminer'])
        rel = mysql.get_person_relation(item['aminer'])
        data["rel"].append(rel)
        rank = mysql.get_person_rank(item["aminer"])
        data['rank'].append(rank)
        item['rel']={"count":len(rel), 
                     "rel":[{"pid1":r[1],"pid2":r[2],"similarity":r[3],"rel_type":r[4]} for r in rel],
                     }
        item['rank']=rank[0][0] if len(rank)>0 else -2
        col.save(item)
    
    print count
    
def dump_lenin_data():
    col = mongo110.db['lenin_label_data']
    data = []
    for item in col.find():
        data.append(item)

    dump = open("lenin_data",'w')
    pickle.dump(data, dump)
    dump.close()
    
def dump_mongo(col_name):
    col = mongo110.db[col_name]
    data = []
    for item in col.find():
        data.append(item)

    dump = open(col_name,'w')
    pickle.dump(data, dump)
    dump.close()
    
def compare1():
    col = mongo110.db["aminer_linkedin_labeled_1208"]
    aid = []
    index = 0
    for item in col.find():
        if index % 1000:
            print index
        index+=1
        aid.append(item['aminer']['id'])
    dump = open("E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\preprocess\\lenin_data")
    data = pickle.load(dump)
    lid = []
    for person in data:
        lid.append(person['aminer'])
    
def construct_linkedin_network():
    col = mongo110.db["person_profile"]
#    for col.find({},{'url'})
    
def compare():
    col = mongo110.db["aminer_linkedin_1123"]
    aid = []
    lid = []
    index = 0
    for item in col.find():
        if index % 1000 == 0:
            print index
        index+=1
        if not item.has_key('aminer'):
            verbose.debug('aminer '+item['name'])
        else:
            for a in item['aminer']:
                aid.append(a['id'])
        if not item.has_key('linkedin'):
            verbose.debug('linkedin '+item['name'])
        else:
            for l in item['linkedin']:
                lid.append(l['url'])
    dump_aid = open('aminer_linkedin_1123_aminer_id','w')
    pickle.dump(aid,dump_aid)
    dump_lid = open('aminer_linkedin_1123_linkedin_url','w')
    pickle.dump(lid,dump_lid)
    col = mongo110.db['aminer_linkedin_labeled_1124']
    labeled_aid = []
    labeled_lid = []
    labeled_data = col.find({'$or':[{'labels.homepage_match':True},
                                    {'labels.domain_match':True},
                                    {'labels.aff_match':True}]})
    index = 0
    print "start"
    for item in labeled_data:
        if index % 1000 == 0:
            print index
        index+=1
        verbose.debug(item['name']+' '+str(item['_id']))
        labeled_aid.append(item['aminer']['id'])
        labeled_lid.append(item['linkedin']['url'])
        
    x = []
    for a in labeled_aid:
        x.append(str(a))
        
    dump = open("E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\preprocess\\lenin_data")
    data = pickle.load(dump)
    xaid= []
    xlid = []
    for person in data:
        xaid.append(person['aminer'])
        xlid.append(person['linkedin'])
        
def plot_hindex():
    import numpy as np
    import matplotlib.mlab as mlab
    import matplotlib.pyplot as plt
    dump = open("E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\preprocess\\lenin_data")
    data = pickle.load(dump)
    ranks = []    
    verbose.debug("data loaded")
    for d in data:
        ranks.append(d['rank'])
    
    sranks = sorted(ranks)
    mu, sigma = 100, 15
    #x = mu + sigma*np.random.randn(10000)
    x = range(0,len(sranks))
    
    # the histogram of the data
    n, bins, patches = plt.hist(x, 50, normed=1, facecolor='green', alpha=0.75)
    
    # add a 'best fit' line
    y = sranks#mlab.normpdf( bins, mu, sigma)
    l = plt.plot(bins, y, 'r--', linewidth=1)
    
    plt.xlabel('Smarts')
    plt.ylabel('Probability')
    plt.title(r'$\mathrm{Histogram\ of\ IQ:}\ \mu=100,\ \sigma=15$')
    plt.axis([40, 160, 0, 0.03])
    plt.grid(True)

    verbose.debug("show")
    plt.show()

    
def main():
    get_labeled_data_name()

if __name__ == "__main__":
    main()