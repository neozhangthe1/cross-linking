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
    for row in people:
        data = ""
        verbose.debug(row[0])
        docs['id'].append(row[0])
        docs['type'].append(1)
        data+=row[1]
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
        docs['data'].append(UnicodeDammit(data).markup)
    return docs
    
def process_linkedin(docs):
    mongo = Mongo()
    col = mongo.db['person_profiles']
    res = col.find(limit=10)
    for item in res:
        data = ""
        verbose.debug(item['_id'])
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

def tfidf(docs):
    vectorizer = CountVectorizer(min_df=1,stop_words='english')
    transformer = TfidfTransformer()#subliner_tf stop_words='english'
    counts = vectorizer.fit_transform(docs['data'])
    tfidfs = transformer.fit_transform(counts)
    feature_names = vectorizer.get_feature_names()
    out_counts = codecs.open(settings.DATA_PATH+"\\counts",'w', encoding="utf-8")
    out_tfidfs = codecs.open(settings.DATA_PATH+"\\tfidfs",'w', encoding="utf-8")
    out_sum_counts = codecs.open(settings.DATA_PATH+"\\sum_counts",'w', encoding="utf-8")
    out_sum_tfidfs = codecs.open(settings.DATA_PATH+"\\sum_tfidfs",'w', encoding="utf-8")
    arr_counts = counts.toarray()
    arr_tfidfs = tfidfs.toarray()
    sum_counts = counts.sum(axis=0)
    sum_tfidfs = counts.sum(axis=0)
    for i in range(len(docs['id'])):
        out_counts.write(str(docs['id'][i])+':')
        out_tfidfs.write(str(docs['id'][i])+':')
        verbose.debug(docs['id'][i])
        for j in range(len(feature_names)):
            if arr_counts[i,j]!=0:
                verbose.debug(feature_names[j]+','+str(arr_counts[i,j]))
                out_counts.write(feature_names[j]+','+str(arr_counts[i,j])+'#')
                out_tfidfs.write(feature_names[j]+','+str(arr_tfidfs[i,j])+'#')
        out_counts.write('\n')
        out_tfidfs.write('\n')
    for i in range(len(arr_counts[0])):
        out_sum_counts.write(feature_names[i]+' '+str(sum_counts[0,i])+'\n')
        out_sum_tfidfs.write(feature_names[i]+' '+str(sum_tfidfs[0,i])+'\n')
    out_counts.close()
    out_tfidfs.close()
#    sort(sum_counts)
#    sort(sum_tfidfs)
    

def main():
    process_data()


if __name__ == "__main__":
    main()