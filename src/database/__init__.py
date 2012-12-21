    input = open('links.txt')
    output = open('degree.txt','w')
    G = networkx.Graph()
    index = 0
    for line in input:
        if index%1==0:
            print index
        index+=1
        x = line.split(' ')
        G.add_edge(x[0],x[1])