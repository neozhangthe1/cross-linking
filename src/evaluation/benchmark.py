import pymongo
import networkx as nx

def get_labeled_data():
    g = nx.Graph()
    col = pymongo.Connection()['scrapy']['labeled_data']
    for item in col.find():
        g.add_edge(str(item['aminer']),item['linkedin'])
    return g
                     

def evaluate(result):
    label = get_labeled_data()
    count = 0
    print "eva"
    for e in result.edges():
        if e in label.edges():
            count+=1

def build_dict():
    a2l = {}
    l2a = {}
    col = pymongo.Connection()['scrapy']['labeled_data']
    for item in col.find():
        a2l[str(item['aminer'])] = item['linkedin']
        l2a[item['linkedin']] = str(item['aminer'])

def evaluate_result(result):
    a2l, l2a = build_dict()
    count = 0
    for l in result:
        if l in l2a:
            if result[l] == l2a[l]:
                count+=1
                try:
                    print 'match'+l+' '+l2a[l]
                except:
                    pass
            else:
                try:
                    print 'miss'+l+' '+l2a[l]+' '+result[l]
                except:
                    pass

def precision(threshold, result):
    sim = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\similarity"))
    a2l, l2a = build_dict()
    correct=0
    fault=0
    total=0
    for l in result:
        if l[0]>'9' or l[0]<'0':
            if l2a.has_key(l) == True:
                if sim[l][result[l]]>threshold:
                    print l+' '+result[l]+' '+str(sim[l][result[l]])
                    total+=1
                    if l2a[l] == result[l]:
                        correct+=1
                    else:
                        fault+=1
    precision = float(correct)/total

def baseline_evaluation(filename_to_read, groundtruth, filename_to_save):
    import pickle
    import networkx as nx
    result = pickle.load(open(filename_to_read))
    match = result['match']
    ids = result['ids']
    gt = nx.Graph()
    correct = 0
    fault = 0
    total = 0
    c = []
    f = []
    t = []
    for pair in groundtruth:
        gt.add_edge(pair[0],pair[1])
        if pair[0] in ids and pair[1] in ids:
            total+=1
            t.append(pair)

    for e in match.edges():
        if gt.has_node(e[0]):
            if gt.has_edge(e[0],e[1]):
                c.append(e)
                correct+=1
            else:
                f.append(e)
                fault+=1

        if gt.has_node(e[1]):
            if gt.has_edge(e[0],e[1]):
                c.append(e)
                correct+=1
            else:
                f.append(e)
                fault+=1
             
    
    precision = float(correct)/(correct+fault)
    print 'precision:'+str(precision)
    recall = float(correct)/(total*2)
    print 'recall:'+str(recall)
    f1 = 2*precision*recall/(recall + precision)
    print 'f1:'+str(f1)
    return precision, recall, f1
      


#if __name__ == '__main__':
#    import pickle
#    gt = 'D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\label_pair_list'
#    for i in range(0, 1):
#        for j in range(10):
#            filename_to_read = 'Z:\\personal\\yutao\\cross linking\\sample-30\\' + str(i) + '\\heuristic_match_0.'+str(j)
#            filename_to_save = 'Z:\\personal\\yutao\\cross linking\\sample-30\\' + str(i) + '\\result_0.'+ str(j)
#            precision, recall, f1 =  baseline_evaluation(filename_to_read = filename_to_read, groundtruth = pickle.load(open(gt)), filename_to_save = filename_to_save)


if __name__ == '__main__':
    import pickle
    gt = 'E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\evaluation\\label_pair_list'
    for i in range(0, 1):
        for j in range(10):
            filename_to_read = 'E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\evaluation\\heuristic_match_result_0.0'
            filename_to_save = 'E:\\My Projects\\Eclipse Workspace\\CrossLinking\\src\\evaluation\\result_0.'
            precision, recall, f1 =  baseline_evaluation(filename_to_read = filename_to_read, groundtruth = pickle.load(open(gt)), filename_to_save = filename_to_save)
