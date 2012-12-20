'''
Created on Dec 18, 2012

@author: Yutao
'''
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.datasets import load_files
from metadata import settings
import codecs
import numpy as np

def sort(items):
    sorted = np.argsort(items)
    print sorted

def tfidf():
    files = load_files(settings.DATA_PATH+"\\input")
    vectorizer = CountVectorizer(min_df=1,stop_words='english')
    transformer = TfidfTransformer()#subliner_tf stop_words='english'
    counts = vectorizer.fit_transform(files.data, files.target)
    tfidfs = transformer.fit_transform(counts)
    feature_names = vectorizer.get_feature_names()
    out_counts = codecs.open(settings.DATA_PATH+"\\counts",'w', encoding="utf-8")
    out_tfidfs = codecs.open(settings.DATA_PATH+"\\tfidfs",'w', encoding="utf-8")
    arr_counts = counts.toarray()
    arr_tfidfs = tfidfs.toarray()
    sum_counts = counts.sum(axis=0)
    sum_tfidfs = counts.sum(axis=0)
    for i in range(len(arr_counts[0])):
        out_counts.write(feature_names[i]+' '+str(sum_counts[0,i])+'\n')
        out_tfidfs.write(feature_names[i]+' '+str(sum_tfidfs[0,i])+'\n')
    out_counts.close()
    out_tfidfs.close()
#    sort(sum_counts)
#    sort(sum_tfidfs)
    

def main():
    tfidf()


if __name__ == "__main__":
    main()