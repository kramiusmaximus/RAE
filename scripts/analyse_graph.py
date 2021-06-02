#!/usr/bin/env python
# coding: utf-8

import time
import numpy as np
import networkx as nx
import pandas as pd
import pickle as p
import random
from datetime import datetime, timedelta
import pytz
from ast import literal_eval
from dateutil import rrule
from matplotlib import pyplot as plt
from copy import deepcopy

G_1 = nx.read_gpickle('../output/graph.gpickle')
#graph_test = nx.read_gpickle('files/graph_test.gpickle')
#bots = p.load(open('files/bots.pickle','rb'))

labels = pd.read_pickle('../output/labels_1.pkl')

for v in G_1.nodes.data():
    try:
        v[1]['pol'] = labels[v[0]]['pol']
        
    except:
        v[1]['pol'] = 'nan'


# Function used to calculate out/in edge ratio of each user for bot identification purposes

# def out_in(graph,bots):
#     bots_set = bots
#     l = list()
#     b = list()
#     for node in graph.nodes():
#         out_edges = len(list(graph.out_edges(node)))
#         in_edges = len(list(graph.in_edges(node)))
#         if node in bots_set:
#             bots_set.remove(node)
#             if in_edges != 0:
#                 out_in = out_edges/in_edges
#             else:
#                 out_in = 'div_by_0'
#             b.append((node,out_edges,in_edges))
#         else:
#             if in_edges != 0:
#                 out_in = out_edges/in_edges
#             else:
#                 out_in = 'div_by_0'
#             l.append((node,out_edges,in_edges))
#     return l,b         

# l,b = out_in(G,bots)

# x = []
# y = []
# x_bot = []
# y_bot = []
# for node,out,in_ in l:
#     x.append(out)
#     y.append(in_)
# for node,out,in_ in b:
#     x_bot.append(out)
#     y_bot.append(in_)   

# def cool_graph(x,y,binn,size,x_lim=None,y_lim=None):
#     left, width = 0, 0.8
#     bottom, height = 0, 0.8
#     spacing = 0.08

#     fig=plt.figure()
#     #scatter
#     ax=fig.add_axes([left,bottom,width,height])
#     ax.scatter(x, y, color='r', label = 'hoomans',s=size)
#     ax.scatter(x_bot, y_bot, color='b', label = 'bots', s=size)
#     ax.set_xlabel('out edges')
#     ax.set_ylabel('in edges')
#     ax.set_title('out/ in edges of nodes')
#     ax.legend()
#     ax.grid(True)
    
#     if x_lim != None: 
#         plt.xlim([0, x_lim])
#     if y_lim != None:
#         plt.ylim([0, y_lim])


#     #hist
#     ax_histx = fig.add_axes([left,bottom+height+spacing,width,0.3])
#     ax_histx.tick_params(direction='in', labelbottom=False)
#     ax_histy = fig.add_axes([left + width + spacing, bottom, 0.3, height])
#     ax_histy.tick_params(direction='in', labelleft=False)

#     binwidth = 0.25
#     lim = np.ceil(np.abs([x, y]).max() / binwidth) * binwidth
#     ax_scatter.set_xlim((-lim, lim))
#     ax_scatter.set_ylim((-lim, lim))

#     bins = np.arange(-lim, lim + binwidth, binwidth)
#     ax_histx.hist(x, bins=binn)
#     ax_histy.hist(y,bins = binn,orientation='horizontal')

#     ax_histx.set_xlim(ax.get_xlim())
#     ax_histy.set_ylim(ax.get_ylim())

#     plt.show()

# cool_graph(x,y,100,10,x_lim = 1000, y_lim = 5000)


def homophily(G): # (graph, before date, after date) Calculates dictionary of general level of homphily, as well as, levels of homophily for each pol. allignment
    dic_pol = {}
    attr_pol = nx.get_node_attributes(G,'pol')
    count = 0
    for u,v,attr in G.edges.data():
        count +=1
        pol_u = attr_pol[u]
        pol_v = attr_pol[v]
        if pol_u == 'nan' and pol_v == 'nan':
            continue
        
        if pol_u != 'nan':
            if pol_u not in dic_pol:
                dic_pol[pol_u] = {'like':0,'total':0}

            if pol_u == pol_v:
                dic_pol[pol_u]['like']+=1

            dic_pol[pol_u]['total']+=1
        
    

    if len(dic_pol) !=0:
        
        def dumb_ass(x = dic_pol):
            dic_out = {}
            for key, dic in x.iteritems():
                a = dic['like']
                b = dic['total']

                value = float(a)/b
                dic_out[key+'_hom'] =  value
            
            return dic_out

        dic_out = dumb_ass()

        u = 0
        l = 0
        for key,dic in dic_pol.iteritems():
            u += dic['like']
            l += dic['total']

        dic_out['general'] = float(u)/l
        return dic_out

    return None


# In[330]:


def homophily_trend(graph,str_after = '01.08.2016',str_before = '28.02.2017', period_length = 'full',sampling_size = 10):
    
    timezone = pytz.timezone("America/Los_Angeles")
    
    date_after = datetime.strptime(str_after, '%d.%m.%Y')
    date_after = timezone.localize(date_after)
    utc_after = time.mktime(date_after.timetuple())
    
    date_before = datetime.strptime(str_before, '%d.%m.%Y')
    date_before = timezone.localize(date_before)
    utc_before = time.mktime(date_before.timetuple())
    
    G = nx.MultiDiGraph()
    
    for (source, target, attr) in graph.edges.data():
        if attr['created_utc'] >= utc_after and attr['created_utc'] <= utc_before:
            G.add_edge(source, target,id = attr['id'], subreddit = attr['subreddit'],score =attr['score'], submission = attr['submission'], created_utc =attr['created_utc'])
    
    attributes = nx.get_node_attributes(graph, 'pol')
    for v in G.nodes.data():
                try:
                    v[1]['pol'] = attributes[v[0]]
        
                except:
                    v[1]['pol'] = 'nan'

    if period_length == 'full':
        return {'raw':homophily(G),'z_scores':homophily_random(G,sampling_size)}
    
    else:
        assert type(period_length) == int
        dates_list = []
        for dt in rrule.rrule(rrule.DAILY,interval=period_length,dtstart=date_after, until=date_before):
            dates_list.append(time.mktime(dt.timetuple()))
        dates_list_collated = []
        for i in range(len(dates_list)-1):
            dates_list_collated.append((dates_list[i],dates_list[i+1]))
        dic_out = {}
        for i,(utc_after0,utc_before0) in enumerate(dates_list_collated):
            
            Gx = nx.MultiDiGraph()
    
            for (source, target, attr) in G.edges.data():
                if attr['created_utc'] >= utc_after0 and attr['created_utc'] <= utc_before0:
                    Gx.add_edge(source, target,id = attr['id'], subreddit = attr['subreddit'],score =attr['score'], submission = attr['submission'], created_utc =attr['created_utc'])
                        
            for v in Gx.nodes.data():
                try:
                    v[1]['pol'] = attributes[v[0]]
        
                except:
                    v[1]['pol'] = 'nan'
                    
            if len(Gx) == 0:
                break
                
            value = homophily_random(Gx,sampling_size)
            
            if value == None:
                break
                
            dic_out[str(i)] = value
            
        return dic_out


# In[331]:


# Function which tests the significance of the results of homophily of given network

def homophily_random(graph,sampling_size):
    node_attr = nx.get_node_attributes(graph,'pol')
    # create lists of users and values
    users = []
    pol_ali = []
    for key, item in node_attr.iteritems():
        users.append(key)
        pol_ali.append(item)
    
    # generate n different dictionaries
    label_dictionaries = [] 
    for i in range(sampling_size):
        random_labels = random.sample(pol_ali,len(pol_ali))
        dictionary = {key:{'pol':value} for key,value in zip(users,random_labels)}
        label_dictionaries.append(dictionary)
    
    graph_score = homophily(graph)
    scores = []
    for i in range(sampling_size):
        #G = graph.copy()
        for v in graph.nodes.data():
            try:
                v[1]['pol'] = label_dictionaries[i][v[0]]['pol']
            except:
                v[1]['pol'] = 'nan'
        to_append = homophily(graph)
        scores.append(to_append)
    scores_dic = {}
    for dic in scores:
        for key, value in dic.iteritems():
            if key in scores_dic:
                scores_dic[key].append(value)
                
            else:
                scores_dic[key] = [value]

    mean_std = {}
    for pol, value in scores_dic.iteritems():
            mean_std[pol] = {}
            mean_std[pol]['mean'] = np.mean(value)
            mean_std[pol]['std'] = np.std(value)

    z_scores = {}
    for pol,value in graph_score.iteritems():
        z_scores[pol] = (value - mean_std[pol]['mean'])/mean_std[pol]['std']
    
    trump_mean = np.mean(scores_dic['trump_hom'])
    hillary_mean = np.mean(scores_dic['clinton_hom'])
    difference_values = [a-b for a,b in zip(scores_dic['trump_hom'],scores_dic['clinton_hom'])]
    std = np.std(difference_values)
    z_scores['difference_z_score'] = ((graph_score['trump_hom'] - graph_score['clinton_hom'])-(trump_mean - hillary_mean))/std
    
    return z_scores


# In[332]:


# results from graph with full period
major_sauce = homophily_trend(G_1,sampling_size = 100)


# In[1]:


p.dump(major_sauce,open('../output/z_scores_full_l1_1.pickle', 'wb'))


# In[18]:


# looking at trends now with 7day intervals

#trend_data = homophily_trend(G,str_after = '01.08.2016',str_before = '28.02.2017', period_length = 32)


# In[21]:


# In[22]:

# In[ ]:




