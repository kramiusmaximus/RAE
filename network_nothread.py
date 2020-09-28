from __future__ import division
import os
#os.environ["OMP_NUM_THREADS"] = "1"
import numpy as np
import pandas as pd
import networkx as nx
import time
import multiprocessing
import itertools
import pickle as p
import sys
from datetime import datetime

#start_date = 1475193600 # 8 Oct 2016

#finish_date = 1481932799 # 8 Dec 2016

# load comment and submission dataframes

df_c = pd.read_csv('../output/formatted/comment_scrape_08.04.20_08.16_02.17_v2.csv', encoding = 'utf-8',low_memory = False)
#df_c = (df_c['created_utc'] >= start_date) & (df_c['created_utc'] <= finish_date)
df_c['submission'] = False
# Make the id's of comments searchable. Id prefexed with t1 -- is submission and with t3 -- is comment.
df_c['parent_id_search'] = df_c['parent_id'].apply(lambda x: x.replace('t1_','').replace('t3_',''))

df_s = pd.read_csv('../output/formatted/submission_scrape_09.04.20_08.16_02.17_v1.csv', encoding = 'utf-8',low_memory = False)
#df_s = df_s[(df_s['created_utc'] >= start_date) & (df_s['created_utc'] <= finish_date)]
df_s['parent_id'] = np.nan
df_s['parent_id_search'] = np.nan
df_s = df_s.rename(columns={'selftext':'body'})
df_s['submission'] = True

# Concatenate comment and submission dataframes
columns = ['author','id','body','parent_id','parent_id_search','created_utc','subreddit','score','submission']
df_network = pd.concat([df_c[columns],df_s[columns]],sort=True,ignore_index = True)

# test df
#n = 100000


df_network_test = df_network#.iloc[:n]

df_network_test = df_network_test.set_index('id')

# add date column

#df_network_test['created_date'] = df_network_test['created_utc'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%d.%m.%Y'))




# Function which finds username of the comment/submission which the user commented on
t1 = time.time()

l = []
for i in df_network_test.index.values:

    parent_id = df_network_test['parent_id_search'][i]
    try:
        
        parent_name = df_network_test['author'][parent_id]
        
    except:
        
        parent_name = np.nan
        
    l.append(parent_name)

t2 = time.time()


# execution 



df_network_test['parent_name'] = l
df_network_test = df_network_test.drop(columns = ['parent_id','parent_id_search'])
'''
t3 = time.time()

df_author_parents = pd.DataFrame.from_dict(L11, orient = 'index', columns = ['parent_author'])

df_network_test = pd.merge(df_network_test,df_author_parents,left_index = True, right_index = True)


t4 = time.time()

df_network_test.to_csv('../output/network_oct_0_dec_16.csv', encoding = 'utf-8', index=False)
'''
print("pool operation took "+str(t2-t1)+" seconds")

df_network_test.to_csv('../output/df_network.csv', encoding = 'utf-8')

#print("merge operation took "+str(t4-t2)+" seconds")

#edge_list = [edge for edge in L1]

#print(edge_list)

'''
# trimming edges which are not in user_labels file
t3 = time.time()

user_labels = p.load(open('../files/user_labels.pickle','rb'))

users_present = [key for key,value in user_labels.iteritems()]

def remove_edge_if_not_present((edge,edge_list_output), users = users_present):

	(a,b) = edge

	if (a not in users) | (b not in users):

		edge_list_output.remove((edge))


L2 = multiprocessing.Manager().list(edge_list)

pool2 = multiprocessing.Pool()

output = pool2.map(remove_edge_if_not_present,itertools.izip(edge_list,itertools.repeat(L2)))
###
for i, _ in enumerate(pool2.imap(remove_edge_if_not_present,itertools.izip(edge_list,itertools.repeat(L2)),chunksize=1000),1):
    sys.stderr.write('\rPart 2 done {0:%}'.format(i/len(df_main_test)))
###
pool2.close()

pool2.join()

t4 = time.time()

print('trimming took {} seconds'.format(t4-t3))
'''
#Create Network

#g = nx.MultiDiGraph(edge_list)

#nx.write_graphml(g,"../output/network_all_test.graphml")

