from __future__ import division
import os
os.environ["OMP_NUM_THREADS"] = "1"
import numpy as np
import pandas as pd
import networkx as nx
import time
import multiprocessing
import itertools
import pickle as p
import sys

start_date = 1475193600 # 8 Oct 2016

start_date= 1481932799 # 8 Dec 2016

# load comment and submission dataframes

df_c = pd.read_csv('../output/formatted/comment_scrape_08.04.20_08.16_02.17_v2.csv', encoding = 'utf-8')
df_c = (df_c['created_utc'] >= start_date) & (df_c['created_utc'] <= finish_date)
df_c['submission'] = False
# Make the id's of comments searchable. Id prefexed with t1 -- is submission and with t3 -- is comment.
df_c['parent_id_search'] = df_c['parent_id'].apply(lambda x: x.replace('t1_','').replace('t3_',''))

df_s = pd.read_csv('../output/formatted/submission_scrape_09.04.20_08.16_02.17_v1.csv', encoding = 'utf-8')
df_s = df_s[(df_s['created_utc'] >= start_date) & (df_s['created_utc'] <= finish_date)]
df_s['parent_id'] = np.nan
df_s['parent_id_search'] = np.nan
df_s['submission'] = True

# Concatenate comment and submission dataframes
columns = ['author','id','parent_id','parent_id_search','created_utc','subreddit','score','submission']
df_network = pd.concat([df_c[columns],df_s[columns]],sort=True,ignore_index = True)

# test df
#n = 100000

df_network_test = df_network#.iloc[:n]





# Function which finds username of the comment/submission which the user commented on

def find_parent(x,dataframe = df_network_test):
    
    #(edge_list,author_user,search_id,created_utc,subreddit,score,submission) = x
    (dictionary,index,author_user,search_id,created_utc,subreddit,score,submission) = x
    if  submission == True:
        
        pass
        
    else:
        
        parent = dataframe[dataframe['id'] == search_id]['author'].values.tolist()
        
        if len(parent)==0:
            
            pass
            
        else:
            
                    #edge_list.append((author_user,parent[0],{'created_utc':created_utc,'subreddit':subreddit,'score':score}))
                    dictionary[index] = parent[0]
# Multiprocessing Execution
t1 = time.time()

#L1 = multiprocessing.Manager().list()
L11 = multiprocessing.Manager().dict()

pool11 = multiprocessing.Pool()

#output = pool1.map(find_parent,itertools.izip(itertools.repeat(L1),df_main_test['author'],df_main_test['parent_id_search']))

for i, _ in enumerate(pool11.imap_unordered(find_parent,itertools.izip(itertools.repeat(L11),df_network_test.index.values,df_network_test['author'],df_network_test['parent_id_search'],df_network_test['created_utc'],df_network_test['subreddit'],df_network_test['score'],df_network_test['submission']),chunksize=500),1):
    sys.stderr.write('\rPart 1 done {0:%}'.format(i/len(df_network_test)))

pool11.close()

pool11.join()

print(len(L11))

t2 = time.time()

t3 = time.time()

df_author_parents = pd.DataFrame.from_dict(L11, orient = 'index', columns = ['parent_author'])

df_network_test = pd.merge(df_network_test,df_author_parents,left_index = True, right_index = True)


t4 = time.time()

df_network_test.to_csv('../output/network_oct_0_dec_16.csv', encoding = 'utf-8', index=False)

print("pool operation took "+str(t2-t1)+" seconds")

print("merge operation took "+str(t4-t2)+" seconds")

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

