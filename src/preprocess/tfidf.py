'''
Created on Dec 18, 2012

@author: Yutao
'''
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.datasets import load_files
from src.metadata import settings
from src.metadata import verbose
from src.database.mongo import Mongo
from src.database.mysql import Mysql 
import codecs
import numpy as np
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit



def calculate_df():
    counts = pickle.load(open("D:\\Users\\yutao\\eclipse1\\two_counts_dump_"))
    lil_counts = counts.tolil()
    nonzero = counts.nonzero()
    index = 0
    for i in range(len(nonzero[0])):
        if index%10000==0:
            print index
        index+=1
        lil_counts[nonzero[0][i], nonzero[1][i]] = 1
    from sklearn.feature_extraction.text import TfidfTransformer
    tran  = TfidfTransformer()
    idf = tran.fit_transform(lil_counts)

def words_to_dict(words1,words2):
    d1 = {}
    d2 = {}
    for k in range(len(words1)):
        d1[words1[k]['id']] = words1[k]['tfidf']
    for k in range(len(words2)):
        d2[words2[k]['id']] = words2[k]['tfidf']
    return d1, d2

def check_labeled_data_similarity():
    import pymongo
    labeled_data = pymongo.Connection()['scrapy']['labeled_data']
    similarity = pymongo.Connection('10.1.1.110',12345)['scrapy']['similarity']
    index = 0
    for item in labeled_data.find():
        if index%1000==0:
            print index
        index+=1
        
        sim = -1
        try:
            x = similarity.find({"_id":item['linkedin']}).next()
            for y in x['sim']:
                if y['id'] == item['aminer']:
                    sim =  y['sim']
                    break
        except Exception,e:
            print e
        item['sim'] = sim
        labeled_data.save(item)

def pairwise_similarity():
    import pickle
    import numpy as np
    import math
    import heapq
    from sklearn.metrics.pairwise import linear_kernel
    singular = 311363
    tfidf = pickle.load(open("D:\\Users\\yutao\\eclipse1\\two_tfidfs_dump_"))
    x = pickle.load(open("D:\\Users\\yutao\\eclipse1\\profiles"))
    ids = x['ids']
    sim_l = {}
    sim_l_index = {}
    flag = 0
    for i in range(1):
        print i
        v1 = tfidf[i]
        t_v1 = np.transpose(v1)
        sim_a = []
        for j in range(singular,len(ids)):
            if j%1000==0:
                print j
            if flag == 0:
                sim_l[j] = []
                sim_l_index[j] = []
            v2 = tfidf[j]
            t_v2 = np.transpose(v2)            
            sim = np.dot(v1, t_v2)[0,0] / (math.sqrt(np.dot(v1,t_v1)[0,0]) * math.sqrt(np.dot(v2,t_v2)[0,0]))
            if sim > 0:
                if len(sim_a)<=100:
                    heapq.heappush(sim_a,sim)
                else:
                    heapq.heappushpop(sim_a,sim)
                if len(sim_l[j])<=100:
                    heapq.heappush(sim_l[j],sim)
                else:
                    heapq.heappushpop(sim_l[j],sim)
    
    
    lil_tfidf = tfidf.tolil()
    flag = 0
    import pymongo
    col = pymongo.Connection("10.1.1.110",12345)['scrapy']['similarity']
    for i in range(singular+((len(ids)-singular)/6)*4,singular+((len(ids)-singular)/6)*5):
        try:
            print i
            print ids[i]
        except Exception, e:
            print e
        sim = {}
        sim['_id'] = ids[i]
        sim['sim'] = []
        lk = linear_kernel(tfidf[i], lil_tfidf[:singular]).flatten()
        for j in range(len(lk)):
            if lk[j]>0.1:
                sim['sim'].append({'id':ids[j],
                                   'sim':lk[j]})
        print len(sim['sim'])
        col.save(sim)
        
    for i in range(singular/6,singular/4*3):
        print i
        sim = {}
        sim['_id'] = ids[i]
        sim['sim'] = []
        lk = linear_kernel(tfidf[i], csc_tfidf[singular:]).flatten()
        for j in range(len(lk)):
            if lk[j]>0:
                sim['sim'].append({'id':ids[singular+j],
                                   'sim':lk[j]})
        col.save(sim)
        
#    for i in range(singular, len(ids)):
#        print i
#        sim[ids[i]] = linear_kernel(tfidf[i], csc_tfidf[:singular]).flatten().argsort[:-100:-1]                  
            
    pass

def pairwise_similarity_idf():
    import pickle
    idf = pickle.load(open("Z:\\personal\\yutao\\profile_idf"))
    shape = idf.shape
    import pymongo
    singular = 311363
    x = pickle.load(open("D:\\Users\\yutao\\eclipse1\\profiles"))
    ids = x['ids']
    col = pymongo.Connection("10.1.1.110",12345)['scrapy']['idf_similarity']
    from sklearn.metrics.pairwise import linear_kernel
    for i in range(378500,singular+((len(ids)-singular)/6)*6):
        try:
            print i
            print ids[i]
        except Exception, e:
            print e
        sim = {}
        sim['_id'] = ids[i]
        sim['sim'] = []
        lk = linear_kernel(idf[i], idf[:singular]).flatten()
        sim['sim'] = sorted(enumerate(lk),key=lambda x:x[1],reverse=True)[:500]
        col.save(sim)
        
def treating():


def pairwise_similarity1():
    import pymongo
    import heapq
    from collections import OrderedDict
    aminer_col = pymongo.Connection("10.1.1.110")['scrapy']['two_aminer']
    linkedin_col = pymongo.Connection("10.1.1.110")['scrapy']['two_linkedin']
    similarity = {}
    flag = 1
    for i in linkedin_col.find(limit = 10):
        sim = {}
        for j in aminer_col.find():
            if flag == 1:
                similarity[j['_id']] = OrderedDict()
            d1, d2 = words_to_dict(i['words'],j['words'])
            s = cosine_similarity(d1, d2)
            if s>0:
                if len(similarity[j['_id']]) >= 100:
                    try:
                        similarity[j['_id']] = OrderedDict(sorted(similarity[j['_id']].items(), key=lambda k:k[1], reverse=True))
                        if similarity[j['_id']].items()[-1]< s:
                            similarity[j['_id']].popitem()
                            similarity[j['_id']][i['_id']]=s
                            
                        print len(similarity[j['_id']])
                    except Exception,e:
                        print e
                else:
                    similarity[j['_id']][i['_id']] = s
                if len(sim) > 100:
                    try:
                        sim = OrderedDict(sorted(sim.items(), key=lambda k:k[1], reverse=True))
                        if sim.values()[-1]< s:
                            sim.popitem()
                            sim[i['_id']]=s
                            
                        print len(sim[j['_id']])
                    except Exception,e:
                        print e
                else:
                    sim[j['_id']] = s
                
        i['similarity'] = similarity[j['_id']]
        linkedin_col.save(i)
        flag = 0
                        

def cosine_similarity(words1,words2):
    import math
    import numpy as np
    set1 = set(words1.keys())
    set2 = set(words2.keys())
    overlap = set1&set2
    if len(overlap) == 0:
        return 0
    v1 = {}
    v2 = {}
    for k in set1|set2:
        if k in words1:
            v1[k] = words1[k]
        else:
            v1[k] = 0
        if k in words2:
            v2[k] = words2[k]
        else:
            v2[k] = 0
    return np.dot(v1.values(), v2.values()) / (math.sqrt(np.dot(v1.values(),v1.values())) * math.sqrt(np.dot(v2.values(),v2.values())))
    

def sort(items):
    sorted = np.argsort(items)
    print sorted
    
def process_data():
    docs = {'id':[], 'data':[], 'type':[]}
    docs = process_linkedin(docs)
    docs = process_aminer(docs)
    tfidf(docs)
    
def process_aminer(docs):
    mysql = Mysql()
    people = mysql.fetch_person()
    index = 0
    for row in people:
        data = ""
        if index % 10000 == 0:
            print index
        index+=1
#        verbose.debug(row[0])
        docs['id'].append(row[0])
        docs['type'].append(1)
        data+=(row[1]+'\n')
        if row[2]!= -1:
            mysql.cur.execute("SELECT * FROM contact_info c WHERE c.id = '"+str(row[2])+"'")
            contact = mysql.cur.fetchall()
            for c in contact:
                for i in [1,4,5,7,8,14,15,17,18,20,21,22,24]:
                    if c[i]!=None:
                        try:
                            data+=(str(c[i])+'\n')
                        except:
                            try:
                                data+=(str(c[i])+'\n')
                            except Exception, e:
                                print e
        mysql.cur.execute("SELECT * FROM na_person_organization o WHERE o.aid = "+str(row[0]))
        organization = mysql.cur.fetchall()
        for o in organization:
            data+=str(o[4])
        docs['data'].append(UnicodeDammit(data.replace(","," ")).markup)
    return docs
    
def process_linkedin(docs):
    mongo = Mongo()
    col = mongo.db['person_profiles']
    res = col.find()
    index = 0
    for item in res:
        data = ""
        if index % 10000 == 0:
            print index
        index+=1
        docs['id'].append(item['_id'])
        docs['type'].append(0)
        if item.has_key('interests'):
            data+=(item['interests']+'\n')
        if item.has_key('education'):
            for e in item['education']:
                data+=(e['name']+'\n')
                if e.has_key('desc'):
                    data+=(e['desc']+'\n')
        if item.has_key('group'):
            if item['group'].has_key('member'):
                data+=(item['group']['member']+'\n')
            if item['group'].has_key('affilition'):
                for a in item['group']['affilition']:
                    data+=(a+'\n')
        data+=(item['name']['family_name']+' '+item['name']['given_name'])
        if item.has_key('overview_html'):
            soup = BeautifulSoup(item['overview_html'])
            data+=(' '.join(list(soup.strings))+'\n')
        if item.has_key('locality'):
            data+=(item['locality']+'\n')
        if item.has_key('skills'):
            for s in item['skills']:
                data+=(s+'\n')
        if item.has_key('industry'):
            data+=(item['industry']+'\n')
        if item.has_key('experience'):
            for e in item['experience']:
                if e.has_key('org'):
                    data+=(e['org']+'\n')
                if e.has_key('title'):
                    data+=(e['title']+'\n')
        if item.has_key('summary'):
            data+=(item['summary']+'\n')
        data+=('url')
        if item.has_key('specilities'):
            data+=(item['specilities']+'\n')
        if item.has_key('homepage'):
            for k in item['homepage'].keys():
                for h in item['homepage'][k]:
                    data+=(h+'\n')
        if item.has_key('honors'):
            for h in item['honors']:
                data+=(h+'\n')
        docs['data'].append(UnicodeDammit(data).markup)
    return docs


def load_data(aminer, linkedin):
    mysql = Mysql()
    mongo = Mongo()
    import pickle
    aminer = pickle.load(open("D:\\Users\\chenwei\\script\\aminer_two"))
    linkedin = pickle.load(open("D:\\Users\\chenwei\\script\\linkedin_two_filter"))
    print aminer.number_of_nodes()
    print linkedin.number_of_nodes()
    ids = []
    profiles = []
    type = []
    
    index= 0
    for i in aminer.nodes():
        verbose.index(index)
        index+=1
        ids.append(int(i))
        profile = ""
        try:
            profile = mysql.get_person_aminer_profile(i)
        except Exception,e:
            print e
            try:
                print i
            except Exception,e:
                print e
        profiles.append(UnicodeDammit(profile).markup)
        type.append(0)
    index=0
    for i in linkedin.nodes():
        verbose.index(index)
        index+=1
        ids.append(i)
        profile = ""
        try:
            profile = mongo.get_person_linkedin_profile(i)
        except Exception,e:
            print e
            try:
                print i
            except Exception,e:
                print e
        profiles.append(profile)
        type.append(1)
    vectorizer = CountVectorizer(stop_words='english')
    transformer = TfidfTransformer()
    print "count"
    counts = vectorizer.fit_transform(profiles)
    import pickle
    dump = open("two_counts_dump_",'w')
    pickle.dump(counts, dump)
    dump.close()
    
    tfidfs = transformer.fit_transform(counts)
    
    dump = open("two_tfidfs_dump_",'w')
    pickle.dump(tfidfs, dump)
    dump.close()
    
    dump = open("two_vectorizer_dump_",'w')
    pickle.dump(vectorizer, dump)
    dump.close()
    
    dump = open("two_transformer_dump_",'w')
    pickle.dump(transformer, dump)
    dump.close()
    
    x= {'ids':ids,'profiles':profiles,"type":type}
    pickle.dump(x,open("profiles",'w'))
    print "ok"
    
    dump = open("two_counts_dump_")
    counts = pickle.load(dump)
    dump.close()
    
    dump = open("two_tfidfs_dump_")
    tfidfs = pickle.load(dump)
    dump.close()
    
    dump = open("two_vectorizer_dump_")
    vectorizer = pickle.load(dump)
    dump.close()

    x = pickle.load(open("profiles"))
    ids=x['ids']
    profiles=x['profiles']
    type=x['type']
    
    lil_counts = counts.tolil()
    lil_tfidf = tfidfs.tolil()
    feature_names = vectorizer.get_feature_names()
    aminer_col = mongo.db['two_aminer']
    linkedin_col = mongo.db['two_linkedin']
    for i in range(len(profiles)):
        if i%1000==0:
            print i
        item = {'_id':ids[i]}
        words = []
        for j in range(len(feature_names)):
            if lil_counts[i,j] != 0:
                words.append({'id':j,'count':lil_counts[i,j],'tfidf':lil_tfidf[i,j]})
        item['words'] = words
        if type[i]==0:
            aminer_col.save(item)
        else:
            linkedin_col.save(item)
            
    nonzero_count = counts.nonzero()
    nonzero_tfidf = tfidfs.nonzero()
    
    id = nonzero_count[0][0]
    item = {'_id':ids[id],'words':[]}
    for i in range(len(nonzero_count[0])):
        if nonzero_count[0][i]!=id:
            if type[id]==0:
                aminer_col.save(item)
            else:
                linkedin_col.save(item)
            id = nonzero_count[0][i]
            item = {'_id':ids[id], 'words':[]}
        wid = int(nonzero_count[1][i])
        item['words'].append({'id':wid,'count':lil_counts[id,wid],'tfidf':lil_tfidf[id,wid]})
    
                
    
    
    feature_names = vectorizer.get_feature_names()
    out_counts = codecs.open("counts",'w', encoding="utf-8")
    out_tfidfs = codecs.open("tfidfs",'w', encoding="utf-8")
    out_sum_counts = codecs.open("sum_counts",'w', encoding="utf-8")
    out_sum_tfidfs = codecs.open("sum_tfidfs",'w', encoding="utf-8")
    sum_counts = counts.sum(axis=0)
    sum_tfidfs = tfidfs.sum(axis=0)
    nonzero_count = counts.nonzero()
    nonzero_tfidf = tfidfs.nonzero()
    id = nonzero_count[0][0]
    out_counts.write(str(ids[id])+':')
    out_counts.write(feature_names[nonzero_count[1][0]]+','+str(counts.getrow(id)[0,nonzero_count[1][0]])+'#')
    for i in range(1,len(nonzero_count[0])):
        if id!=nonzero_count[0][i]:
            id = nonzero_count[0][i]
            out_counts.write("\n")
            out_counts.write(str(ids[id])+':')
        out_counts.write(feature_names[nonzero_count[1][i]]+','+str(counts.getrow(id)[0,nonzero_count[1][i]])+'#')
    id = nonzero_tfidf[0][0]
    out_tfidfs.write(str(ids[id])+':')
    out_tfidfs.write(feature_names[nonzero_tfidf[1][0]]+','+str(tfidfs.getrow(id)[0,nonzero_tfidf[1][0]])+'#')
    for i in range(1,len(nonzero_tfidf[0])):
        if id!=nonzero_tfidf[0][i]:
            id = nonzero_tfidf[0][i]
            print id
            out_tfidfs.write("\n")
            out_tfidfs.write(str(ids[id])+':')
        out_tfidfs.write(feature_names[nonzero_tfidf[1][i]]+','+str(tfidfs.getrow(id)[0,nonzero_tfidf[1][i]])+'#')
   
    for i in range(sum_counts.shape[1]):
        out_sum_counts.write(feature_names[i]+' '+str(sum_counts[0,i])+'\n')
        out_sum_tfidfs.write(feature_names[i]+' '+str(sum_tfidfs[0,i])+'\n')
    out_counts.close()
    out_tfidfs.close()
        
        

def tfidf(docs):
    vectorizer = CountVectorizer(max_df=0.5,stop_words='english')
    transformer = TfidfTransformer()#subliner_tf stop_words='english'
    print "count"
    counts = vectorizer.fit_transform(docs['data'])
    print "tfidf"
    tfidfs = transformer.fit_transform(counts)
    print "ok"
    feature_names = vectorizer.get_feature_names()
    out_counts = codecs.open(settings.DATA_PATH+"\\counts",'w', encoding="utf-8")
    out_tfidfs = codecs.open(settings.DATA_PATH+"\\tfidfs",'w', encoding="utf-8")
    out_sum_counts = codecs.open(settings.DATA_PATH+"\\sum_counts",'w', encoding="utf-8")
    out_sum_tfidfs = codecs.open(settings.DATA_PATH+"\\sum_tfidfs",'w', encoding="utf-8")
#    arr_counts = counts.toarray()
#    arr_tfidfs = tfidfs.toarray()
    sum_counts = counts.sum(axis=0)
    sum_tfidfs = tfidfs.sum(axis=0)
    nonzero_count = counts.nonzero()
    nonzero_tfidf = tfidfs.nonzero()
    id = nonzero_count[0][0]
    out_counts.write(str(docs['id'][id])+':')
    out_counts.write(feature_names[nonzero_count[1][0]]+','+str(counts.getrow(id)[0,nonzero_count[1][0]])+'#')
    for i in range(1,len(nonzero_count[0])):
        if id!=nonzero_count[0][i]:
            id = nonzero_count[0][i]
            out_counts.write("\n")
            out_counts.write(str(docs['id'][id])+':')
        out_counts.write(feature_names[nonzero_count[1][i]]+','+str(counts.getrow(id)[0,nonzero_count[1][i]])+'#')
    id = nonzero_tfidf[0][0]
    out_tfidfs.write(str(docs['id'][id])+':')
    out_tfidfs.write(feature_names[nonzero_tfidf[1][0]]+','+str(tfidfs.getrow(id)[0,nonzero_tfidf[1][0]])+'#')
    for i in range(1,len(nonzero_tfidf[0])):
        if id!=nonzero_tfidf[0][i]:
            id = nonzero_tfidf[0][i]
            print id
            out_tfidfs.write("\n")
            out_tfidfs.write(str(docs['id'][id])+':')
        out_tfidfs.write(feature_names[nonzero_tfidf[1][i]]+','+str(tfidfs.getrow(id)[0,nonzero_tfidf[1][i]])+'#')
   
    for i in range(sum_counts.shape[1]):
        out_sum_counts.write(feature_names[i]+' '+str(sum_counts[0,i])+'\n')
        out_sum_tfidfs.write(feature_names[i]+' '+str(sum_tfidfs[0,i])+'\n')
    out_counts.close()
    out_tfidfs.close()
#    sort(sum_counts)
#    sort(sum_tfidfs)
    

def main():
    pairwise_similarity()


if __name__ == "__main__":
    main()