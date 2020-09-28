import networkx as nx 
import numpy as np 
import pandas as pd 





g = nx.read_gpickle('../output/graph.gpickle')

labels = pd.read_pickle('../output/labels_1.pkl')

for v in g.nodes.data():
    try:
        v[1]['pol'] = labels[v[0]]['pol']
        
    except:
        v[1]['pol'] = 'nan'

columns = pd.MultiIndex.from_product([['trump', 'clinton','nan'], ['trump', 'clinton','nan']])

df = pd.DataFrame(columns=columns)

node_attr = nx.get_node_attributes(g,'pol')
i=0
for u,v,attr in g.edges.data():
    i+=1
    polu = node_attr[u]
    polv = node_attr[v]
    subreddit = attr['subreddit']
    
    if subreddit in df.index:
        if df.loc[subreddit,(polu,polv)] != np.nan:
            df.loc[subreddit,(polu,polv)] +=1
        else:
            df.loc[subreddit,(polu,polv)] = 1
    else:
        df.loc[subreddit,(polu,polv)] = 1

df.to_excel('../output/graph_summary.xlsx')
print(i)
