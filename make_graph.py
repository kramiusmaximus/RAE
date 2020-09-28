#!/usr/bin/env python
# coding: utf-8


import numpy as np
import networkx as nx
import pandas as pd
import pickle as p
import random
from time import time

df_network = pd.read_csv('../output/df_network.csv',low_memory = False)

df_network_edge = df_network[(pd.isna(df_network['parent_name']) == False)&(df_network['author'] != '[deleted]')&(df_network['parent_name'] != '[deleted]')] # Remove all edges with deleted user(s)


#Remove obvious bots
bots = p.load(open('../output/bots.pickle','rb'))
df_network_edge.set_index('author',inplace=True)
users = set(df_network_edge.index.values)
bots_present = users.intersection(bots)
df_network_edge.drop(index=bots_present,inplace=True)
df_network_edge.reset_index(level=0, inplace = True)

t1 = time()
G = nx.from_pandas_edgelist(df_network_edge, 'author','parent_name',['id','subreddit','score','submission','created_utc'],nx.MultiDiGraph())
nx.write_gpickle(G,'../output/graph.gpickle')
t2 = time()
print('it took '+str(t2-t1)+' seconds to create the graph and write it')