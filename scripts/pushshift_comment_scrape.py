#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import requests as req
from bs4 import BeautifulSoup as bs
import time

#from matplotlib import pyplot as plt


# In[30]:


# Relevant Time Frame

after_date = 1477958400 # 1 Nov 2016

before_date = 1479254399 # 15 Nov 2016


after_date_nnew = 1475280000 # 1 oct 2016

before_date_nnew = 1475884799 # 7 oct 2016


after_date_new = 1470009601  # 1 August 2016

before_date_new = 1488326399 # 28  February 2017




# In[31]:


# Comment Characteristics to retrieve

comment_characteristics = ['author','body','created_utc','subreddit',"score"]



# In[32]:


#from https://old.reddit.com/r/ListOfSubreddits/wiki/politics50k


subreddit_list = pd.read_csv('../output/subreddit_list.csv')
subreddits_list = subreddit_list['subreddit'].values.tolist()


master_subreddits_list =  ['cispa', 'the_donald', 'truelibertarian', 'jindal', 'targetedshirts', 'leninism', 'the_mueller', 'trumpcriticizestrump', 'keep_track', 'libertarianwomen', 'crimethinccollective', 'enlightenedcentrism', 'ricksantorum', 'peoplesparty', 'chrischristie']#subreddits_list # place list of subreddits to scrape here

# In[38]:


# Fuction to get comments (child)

def comments_child(subreddit,after_date_child,before_date_child):
    
    before_date = str(before_date_child+1)
    
    after_date = str(after_date_child-1)
    
    url_comment = 'https://api.pushshift.io/reddit/search/comment/?subreddit='+subreddit+'&before='+before_date+'&after='+after_date+'&sort=asc&sort_type=created_utc&size=500'
    
    #print(url_comment)
    try:
        comments = req.get(url_comment).json()
    
        return comments

    except:
        print('none')
        return None



# In[40]:


# Function to get ALL comments (parent)

def comments_parent(subreddit,after_date,before_date,comment_characteristics):
    
    after_date_child = after_date
    
    before_date_child = before_date
    
    comment_count = 500 
    
    comment_counter = 0
    
    dictionary = {}
    
    while comment_count == 500:
        
        comments = comments_child(subreddit,after_date_child,before_date_child)
        
        if comments != None:
            
            comment_count = len(comments['data'])
            print(comment_count)
            for comment in comments['data']:
                
                comment_id = comment['id']
                
                dictionary[comment_id] = {}
                
                
                for characteristic in comment_characteristics:
                    
                    try:
                        
                        sauce = comment[characteristic]
                        
                        if type(sauce)==str:
                            
                            sauce = sauce.replace('\r','\n')
                            sauce = sauce.replace('\n\n\n',' ')
                            sauce = sauce.replace('\n\n',' ')
                            sauce = sauce.replace('\n',' ') 
                            sauce = sauce.replace('&gt;',' ')
                            
                        dictionary[comment_id][characteristic] = sauce
                        
                    except:
                        
                        continue
                
        comment_counter +=500
        
        print(comment_counter)

        if comments !=None:
            
            if len(comments['data']) != 0:

                after_date_child = comments['data'][-1]['created_utc']
        
    return dictionary


# In[ ]:





# In[41]:


# final comment el scrappo

def scrape_reddit_comment(subreddit_list,after_date,before_date,comment_characteristics):
    
    master_dic = {}
    
    for subreddit in subreddit_list:
        
        print('Starting to scrape '+subreddit)
        
        master_dic[subreddit] = comments_parent(subreddit,after_date,before_date,comment_characteristics)
                   
    output = {}
    
    for key,item in master_dic.iteritems():
        
        output.update(master_dic[key])
            
        
    return output


# In[42]:

train = scrape_reddit_comment(['politics'],after_date_nnew,before_date_nnew,comment_characteristics)
comment_scrape1 = scrape_reddit_comment(['The_Donald','democrats','Conservative','hillaryclinton','PoliticalDiscussion','SandersForPresident'],after_date_nnew,before_date_nnew,comment_characteristics)
comment_scrape2 = scrape_reddit_comment(['news','askscience','sports','philosophy','memes','Music'],after_date_nnew,before_date_nnew,comment_characteristics)

# In[51]:

dft = pd.DataFrame(train).T
df1 = pd.DataFrame(comment_scrape1).T
df2 = pd.DataFrame(comment_scrape2).T

# In[44]:

dft.to_csv('../output/comment_scrape/train.csv', index=False, encoding = 'utf-8')
df1.to_csv('../output/comment_scrape/political.csv', index=False, encoding = 'utf-8')
df2.to_csv('../output/comment_scrape/nonpolitical.csv', index=False, encoding = 'utf-8')


