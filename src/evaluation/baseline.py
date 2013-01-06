"""
Baseline evaluation: greedy match + threshold
"""

import pickle
import networkx

def baseline_evaluation(filename_to_read, groundtruth, filename_to_save):
    try:
        g = ''
        with open(filename_to_read) as fr:
            g = pickle.load(fr)['match']
    except Exception, e:
        print e
        return {}
       
    gt = ''
       
    with open(groundtruth) as fr:
        gt = pickle.load(fr)
   
    # gt
    total_pair = 0
    all_id = []
    new_gt = []
    for (id1, id2) in gt:
        if g.has_edge(id1, id2):
            total_pair = total_pair + 1
            new_gt.append((id1, id2))
            all_id.append(id1)
            all_id.append(id2) 
   
    # return the raw match result without threshold
    print new_gt
    if new_gt == []:
        return 'No labeled pair!!'      
   
    match_result = {}
    pairList = {}
    for (id1, id2) in g.edges():
        if id1 == id2:
            #g.remove_edge(id1, id1)
            continue
       # pairList[(id1,id2)] = g.edge[id1][id2][ 'weight']
    #print pairList
    # update g
    #with open(filename_to_read, 'w+') as fr:
    #    pickle.dump(g, fr)
    # get match results
   
    sortedId = sorted(pairList, key = lambda x: x[0 ], reverse = True)
   
    for (id1, id2) in sortedId:
        if g.has_edge(id1, id2):
            match_result[(id1, id2)] = pairList[(id1, id2)]
            g.remove_node(id1)
            g.remove_node(id2)
        else:
            pass
           
    with open(filename_to_save, 'w+') as fw:
        pickle.dump(match_result, fw)
   
   
    """
    # compute detailed accuracy , recall and F1-score for different threshold
    """
       
    sortedKey  =  sorted(match_result, key = lambda x: x[1 ])

    finalResult = {}
    for i in range(0, 11):
        alpha = float(i)/ 1000
       
       
        not_accurate = 0
        recall = 0
        for (id1, id2) in sortedKey:
            if match_result[(id1, id2)] < alpha:
                print match_result[(id1, id2)]
                break
            if ((id1, id2) in new_gt) or ((id2, id1) in new_gt):
                recall = recall + 1
            else:
                if (id1 in all_id):
                    not_accurate= not_accurate + 1
                if (id2 in all_id):
                    not_accurate = not_accurate + 1
        # results
        try:
            accuracy = 1 - float(not_accurate)/(recall*2 + not_accurate)
            recall = float(recall)/total_pair
            F1 = 2*accuracy*recall/(recall + accuracy)
            finalResult[str(alpha)] = { 'accuracy':accuracy, 'recall' : recall, 'F1' : F1}
        except Exception,e:
            print e
       
   
    with open(filename_to_save + 'Final-Result', 'w+') as fw:
        pickle.dump(finalResult, fw)
    return finalResult




if __name__ == '__main__':
    gt = 'D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\label_pair_list'
    for i in range(0, 1):
        filename_to_read = 'Z:\\personal\\yutao\\cross linking\\sample-30\\' + str(i) + '\\heuristic_match_result'
        filename_to_save = 'Z:\\personal\\yutao\\cross linking\\sample-30\\' + str(i) + '\\result'
        print baseline_evaluation(filename_to_read = filename_to_read, groundtruth = gt, filename_to_save = filename_to_save)

