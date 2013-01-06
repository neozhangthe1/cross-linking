"""
Experiments for using SimRank to match person from Aminer with Linkedin
"""
import networkx as nx
from collections import defaultdict
import copy
import logging
import codecs
from scipy.sparse import coo_matrix, lil_matrix

logging.basicConfig(level=logging.DEBUG)

def load_graph_dec(data_dir):
    from path import path
    data = path(data_dir)
    try:
        with data.files()[0].open() as f:
            G = nx.Graph()
            for line in f:
                edge = line.split(' ')
                if len(edge) == 2:
                    G.add_edge(str(edge[0].strip()), str(edge[1].strip()))
            return G
    except:
        logging.critical("No data file")
        raise

def load_graph(data_dir):
    import pickle
    aminer = pickle.load(open("D:\\Users\\chenwei\\experiment\\aminer_two_3.pickle"))
    linkedin = pickle.load(open("D:\\Users\\chenwei\\experiment\\linkedin_two_filter_3.pickle"))
    id = 0
    for n in aminer.nodes():
        n
    merge = nx.union(linkedin, aminer)
    return merge   

def _get_neighbours(G, node, degree):
    result_old = set([node])
    result = set([node])
    while degree > 0:
        for ni in result_old:
            try:
                ns = G.neighbors(ni)
                for neighbor in ns:
                    result.add(neighbor)
            except Exception, e:
                print "The error node %s " % node
                print e
                raise
            result_old = copy.deepcopy(result)
            degree -= 1
    return result

def sort_similarity():
    import pickle
    from collections import OrderedDict
    similarity = pickle.load(open("D:\\Users\\chenwei\\experiment\\similarity.pickle"))
    index = 0
    for k in similarity:
        if index%100==0:
            print index
            try:
                print k
            except Exception,e:
                print e
        index+=1
        similarity[k] = OrderedDict(sorted(similarity[k].items(),key = lambda x:x[1],reverse=True))
    index = 0
    for k in similarity:
        if index<1100:
            index+=1
            continue
        if index%100==0:
            print index
            try:
                print k
            except Exception,e:
                print e
        index+=1
        xx = {}
        for i in similarity[k].items():
            flag = 0
            if i[1]>0:
                xx[i[0]] = i[1]
            else:
                flag = 1
                break
            if flag == 1:
                print len(similarity[k])
                print "to"
                print len(xx)
                xx = OrderedDict(sorted(xx.items(), key = lambda x:x[1], reverse = True))
                similarity[k] = xx

import pickle
from collections import OrderedDict
label = pickle.load(open("D:\\Users\\chenwei\\experiment\\label_pair_list"))
import json
def dump_snapshot(threshold, sim, iter):
    path = "Z:\\personal\\yutao\\cross linking\\simrank\\snapshot_double_"
    import codecs
    print "snap shot "+str(iter)
    rank_l = {}
    rank_a = {}
    rank_l_x = []
    rank_a_x = []
    index = 0
    cc = 0
    for i in label:
        if index%1000==0:
            print index
        index+=1
        if sim.has_key(i[0]) and sim.has_key(i[1]):
            cc+=1
            try:
                r = OrderedDict(sorted(sim[i[0]].items(),key=lambda x:x[1],reverse=True)).keys().index(i[1])
                rank_a[i[0]] = r
            except Exception,e:
                rank_a_x.append(i[0])
            try:
                r = OrderedDict(sorted(sim[i[1]].items(),key=lambda x:x[1],reverse=True)).keys().index(i[0])
                rank_l[i[1]] = r
            except Exception,e:
                rank_l_x.append(i[0])
    rank_a_file = codecs.open(path+str(threshold)+"\\rank_a_"+str(iter)+".txt",'w','utf-8')
    rank_l_file = codecs.open(path+str(threshold)+"\\rank_l_"+str(iter)+".txt",'w','utf-8')
    for k in rank_a:
        rank_a_file.write(k+' '+str(rank_a[k])+'\n')
    for k in rank_l:
        rank_l_file.write(k+' '+str(rank_l[k])+'\n')
    
    dis_rank_a = {}
    for i in rank_a:
        if not dis_rank_a.has_key(rank_a[i]):
            dis_rank_a[rank_a[i]]=0
        dis_rank_a[rank_a[i]]+=1
    dis_rank_l = {}
    for i in rank_l:
        if not dis_rank_l.has_key(rank_l[i]):
            dis_rank_l[rank_l[i]]=0
        dis_rank_l[rank_l[i]]+=1
    dis_rank_a_file = codecs.open(path+str(threshold)+"\\dis_rank_a_"+str(iter)+".txt",'w','utf-8')
    dis_rank_l_file = codecs.open(path+str(threshold)+"\\dis_rank_l_"+str(iter)+".txt",'w','utf-8')
    json.dump(dis_rank_a, dis_rank_a_file)
    json.dump(dis_rank_l, dis_rank_l_file)

    snap = codecs.open(path+str(threshold)+"\\snap_"+str(iter)+".txt",'w','utf-8')
    for k in sim:
        snap.write(k+' ')
        for s in sim[k]:
            snap.write(s+':'+str(sim[k][s]))
            snap.write(' ')
        snap.write('\n')
    snap.close()
    print "---------------------------dump finished---------------------------------"

def simrank(decay_factor=0.9, distance=5, max_iteration=10):
    from threading import Thread
    aminer = pickle.load(open("D:\\Users\\chenwei\\experiment\\aminer_two_3.pickle"))
    linkedin = pickle.load(open("D:\\Users\\chenwei\\experiment\\linkedin_two_filter_3.pickle"))
    sim = pickle.load(open("D:\\Users\\chenwei\\experiment\\similarity.pickle"))
    #sim_old = copy.deepcopy(sim)
    simi =[{},{}]
    simi[0]=sim
    simi[1]=copy.deepcopy(sim)
    decay_factor = 1
    for cur_iter in range(10):
        logging.info("%sth iterating..." % (cur_iter))
        if cur_iter!=0:
            if _is_converge(simi[0], simi[1]):
                break
        logging.info("not converge")
        #sim_old = copy.deepcopy(sim)
        '''
        if cur_iter is an odd number, cur == 0, old == 1
        if cur_iter is an even number, cur == 1, old == 0
        '''
        old = cur_iter%2  
        cur = (cur_iter+1)%2
        '''
        dump snapshot for each iteration
        '''
        Thread(target = dump_snapshot, args=[decay_factor, simi[old], cur_iter]).start()
        logging.info("algorithm start...")
        index = 0
        for u in simi[old]:
            if index%10==0:
                print "[INDEX]%s" %index
                try:
                    print u
                except Exception,e:
                    print e
            index+=1
            for v in simi[old][u]:
                s_uv = 0.0
                for n_u in linkedin.neighbors(u):
                    for n_v in aminer.neighbors(v):
                        if simi[old][n_u].has_key(n_v):
                            s_uv+=simi[old][n_u][n_v]
                simi[cur][u][v] = (decay_factor * s_uv) / ((linkedin.degree(u))*(aminer.degree(v)))
                simi[cur][v][u] = (decay_factor * s_uv) / ((linkedin.degree(u))*(aminer.degree(v)))
    return sim


        #for u in G.nodes_iter():
        #    for v in _get_neighbours(G, u, distance):
        #        if u == v:
        #            continue
        #        s_uv = 0.0
        #        for n_u in G.neighbors(u):
        #            for n_v in G.neighbors(v):
        #                s_uv += sim_old[n_u][n_v]
        #        ### bug here, what if no neighbor
        #        sim[u][v] = (decay_factor * s_uv / (len(G.neighbors(u)) * len(G.neighbors(v))))
    

def _is_converge(oldg, newg, threhold=1e-10):
    for i in newg.keys():
        for j in newg[i].keys():
            if abs(newg[i][j] - oldg[i][j]) >= threhold:
                return False
    return True

def save(matrix, threhold=1e-5):
    import operator
    with codecs.open("output.txt", 'w', 'utf-8') as f:
        for i in matrix.keys():
            f.write("%s" % i)
            sorted_x = sorted(matrix[i].iteritems(), key=operator.itemgetter(1))
            for j in sorted_x:
                if j[1] > threhold:
                    f.write(" %s(%f)" % (j[0], j[1]))
            f.write("\n")


def main(args):
    logging.debug('Loading graph......')
    G = load_graph(args.data)
    logging.debug("Graph: %s nodes, %s edges." %(G.number_of_nodes(), G.number_of_edges()))
    logging.debug("SimRanking......")
    sim_matrix = simrank(G, args.decay, args.degree, args.max_iteration)
    # record result
    logging.debug("Saving result......")
    save(sim_matrix)
    logging.debug("Done.")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="SimRank algorithm")
    parser.add_argument('-d', '--decay', help='the decay factor, default is 0.9', default=0.9, type=float)
    parser.add_argument('-g', '--degree', help='get outer neighbourhood', default=5, type=int)
    parser.add_argument('-D', '--data', help='directory of data files', default='data', type=str)
    parser.add_argument('-i', '--max_iteration', help='the maximal iteration, default is 10', default=10, type=int)
    args = parser.parse_args()
    main(args)