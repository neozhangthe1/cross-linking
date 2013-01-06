'''
Created on Dec 31, 2012

@author: Yutao
'''
import networkx as nx
import pymongo


def gen_network():
    import pickle
    linkedin = pickle.load(open("linkedin_two"))
    aminer = pickle.load(open("aminer_two"))
    sim = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\similarity"))
    network = nx.Graph()
    for n in linkedin.nodes():
        for a in sim[n]:
            network.add_edge(n, a, weight = sim[n][a])
    return network

def gen_network_sample():
    import pickle
    linkedin = pickle.load(open("linkedin_two"))
    aminer = pickle.load(open("aminer_two"))
    sim = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\similarity"))
    network = nx.Graph()
    for n in linkedin.nodes():
        for a in sim[n]:
            network.add_edge(n, a, weight = sim[n][a])
    return network

def max_weight_matching():
    network = gen_network()
    print "matching"
    mate = nx.max_weight_matching(network)

def match():
    import pickle
    sim = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\similarity"))
    g = nx.Graph()
    index = 0
    edges = []
    for key in sim:
        if index%1000==0:
            print index
        index+=1
        for i in sim[key]:
            g.add_edge(key,str(i),weight = sim[key][i])
    mate = nx.max_weight_matching(g)

def heuristic(linkedin, aminer, sim):
    import pickle
    import networkx as nx
    matches = [nx.Graph() for i in range(10)]
    index = 0
    for l in linkedin:
        if index%1000==0:
            print index
        index+=1
        siml = sorted(sim[l].items(), key = lambda x: x[1], reverse=True)
        for i in range(10):
            for s in siml:
                if s[1]<float(i)/10:
                    break
                if aminer.has_node(s[0]):              
                    if matches[i].has_node(s[0]) == False:
                        matches[i].add_edge(l, s[0], weight = s[1])
                        break
    for i in range(10):
        result = {}
        result['match'] = matches[i]
        ids =  list(set(aminer.nodes())|set(linkedin.nodes()))
        result['ids'] = ids
        dump = open("heuristic_match_result_"+str(i),'w')
        pickle.dump(result,dump)
        dump.close()

def heuristic_match():
    import pickle
    sim = pickle.load(open("Z:\\personal\\yutao\\cross linking\\similarity"))
    g = nx.Graph()
    index = 0
    edges = []
    for l in sim:
        if index%1000==0:
            print index
        index+=1
        sim_list = sorted(sim[l].items(), key = lambda x: x[1], reverse = True)
        for s in range(len(sim_list)):
            if not g.has_node(sim_list[s][0]):
                g.add_edge(sim_list[s][0],l)
                break
    dump = open("heurisitic_match_result_all_0",'w')
    pickle.dump(g, dump)
    dump.close()



if __name__ == "__main__":
    import pickle
    linkedin = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\linkedin_two"))
    aminer = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\aminer_two"))
    sim = pickle.load(open("D:\\Users\\chenwei\\sample-experiment-data\\sample\\sample-30\\similarity"))
    import numpy as np
    heuristic(linkedin, aminer, sim)