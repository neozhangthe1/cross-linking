'''
Created on Dec 21, 2012

@author: Yutao
'''
from src.database.mongo import Mongo
from src.database.mysql import Mysql
from src.metadata import verbose
import pickle
    
mongo = Mongo()
mysql = Mysql()

def filter_werid_data():
    col = mongo.db["aminer_linkedin_filtered_1124"]
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
    col = mongo.db["lenin_label_data"]
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
    col = mongo.db['lenin_label_data']
    data = []
    for item in col.find():
        data.append(item)

    dump = open("lenin_data",'w')
    pickle.dump(data, dump)
    dump.close()
    
def dump_mongo(col_name):
    col = mongo.db[col_name]
    data = []
    for item in col.find():
        data.append(item)

    dump = open(col_name,'w')
    pickle.dump(data, dump)
    dump.close()
    
def compare1():
    col = mongo.db["aminer_linkedin_labeled_1208"]
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
    pass
    
def compare():
    col = mongo.db["aminer_linkedin_1123"]
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
    col = mongo.db['aminer_linkedin_labeled_1208']
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
        verbose.debug(item['name'])
        labeled_aid.append(item['aminer']['id'])
        labeled_lid.append(item['linkedin']['url'])
        
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
    plot_hindex()

if __name__ == "__main__":
    main()