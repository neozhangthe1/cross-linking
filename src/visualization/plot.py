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

def plot_degree_histogram(G):
    #!/usr/bin/env python
    """
    Random graph from given degree sequence.
    Draw degree histogram with matplotlib.
    #ref: http://networkx.lanl.gov/examples/drawing/degree_histogram.html

    """   

    try:
        import matplotlib.pyplot as plt
        import matplotlib
    except:
        raise

    import networkx as nx

    #z=nx.utils.create_degree_sequence(100,nx.utils.powerlaw_sequence,exponent=2.1)
    #nx.is_valid_degree_sequence(z)

    #print "Configuration model"
    #G=nx.configuration_model(z)  # configuration model

    degree_sequence=sorted(nx.degree(G).values(),reverse=True) # degree sequence
    #print "Degree sequence", degree_sequence
    dmax=max(degree_sequence)

    plt.loglog(degree_sequence,'b-',marker='o')
    plt.title("Aminer Experiment Data Degree")
    plt.ylabel("degree")
    plt.xlabel("rank")

    # draw graph in inset
    #plt.axes([0.45,0.45,0.45,0.45])
    #Gcc=nx.connected_component_subgraphs(G)[0]
    #pos=nx.spring_layout(Gcc)
    #plt.axis('off')
    #nx.draw_networkx_nodes(Gcc,pos,node_size=20)
    #nx.draw_networkx_edges(Gcc,pos,alpha=0.4)

    plt.savefig("Z:\\personal\\yutao\\cross linking\\aminer_degree_histogram.png")
    plt.show()