import random
import networkx as nx
import sys

def clique_grid(h, w, k):
    G = nx.DiGraph()
    G.add_edges_from((p+k*i,q+k*i) for i in range(w*h) for p in range(k) for q in range(k) if p!=q)
    for i in range(h):
        for j in range(w - 1):
            clique_num = w*i + j
            u = k*clique_num + k//2
            v = k*(clique_num + 1)
            G.add_edge(u, v)
            G.add_edge(v, u)
    for i in range(h - 1):
        for j in range(w):
            clique_num = w*i + j
            u = k*clique_num + k//4 + k//2
            v = k*(clique_num + w) + k//4
            G.add_edge(u, v)
            G.add_edge(v, u)
    return G

def communities(num_com, com_size, p1=0.9, p2=0.1):
    n = num_com*com_size
    G = nx.DiGraph(nx.empty_graph(n))
    for u in range(n):
        for v in range(n):
            if u == v:
                continue
            r = random.random()
            if (r <= p2) or ((r <= p1) and (u//com_size == v//com_size)):
                G.add_edge(u,v)
                G.add_edge(v,u)
    return G

def save_graph(G, path):
    f = open(path, "w")
    f.write("\n".join(nx.generate_adjlist(G)))
    f.close()

def main():
    type = sys.argv[1]
    path = sys.argv[2]
    if type == 'clique':
        G = clique_grid(int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))
        save_graph(G, path)
    elif type == 'communities':
        G = communities(int(sys.argv[3]), int(sys.argv[4]))
        save_graph(G, path)
main()
