#!/usr/bin/env python
# coding: utf-8

# In[27]:


import pandas as pd
import numpy as np
import requests as req
from bs4 import BeautifulSoup as bs
import time


# Relevant Time Frame

after_date = 1477958400 # 1 Nov 2016

before_date = 1479254399 # 15 Nov 2016

after_date_new = 1470009601  # 1 August 2016

before_date_new = 1488326399 # 28  February 2017


# Submission charctersitics to retrieve

sub_characteristics = ['author','author_fullname','created_utc','domain','full_link','id','is_original_content','is_reddit_media_domain','locked','media','num_crossposts','subreddit','title','url','selftext',"num_comments","num_crossposts",'score']





master_subreddits_list =  ... # place list of subreddits to scrape here


# In[37]:


# Fuction to get submissions (child)

def submissions_child(subreddit,after_date_child,before_date_child,submission_characteristics):
    
    before_date = str(before_date_child+1)
    
    after_date = str(after_date_child-1)
    
    url_sub = 'https://api.pushshift.io/reddit/search/submission/?subreddit='+subreddit+'&before='+before_date+'&after='+after_date+'&sort=asc&sort_type=created_utc&size=500'
    #print(url_sub)
    submissions = req.get(url_sub).json()
    
    
    
    
    
    return submissions


# In[38]:


# Function to get ALL submissions (parent)

def submissions_parent(subreddit,after_date,before_date,submission_characteristics):
    
    after_date_child = after_date
    
    before_date_child = before_date
    
    submission_count = 500 
    
    sub_counter = 0
    
    dictionary = {}
    
    while submission_count == 500:
        
        submissions = submissions_child(subreddit,after_date_child,before_date_child,submission_characteristics)
        
        submission_count = len(submissions['data'])
        
        for sub in submissions['data']:
            
            sub_id = sub['id']
            
            dictionary[sub_id] = {}
            
            #print(sub_id)
            
            for characteristic in submission_characteristics:
                
                try:
                    
                    sauce = sub[characteristic]
                    
                    if type(sauce)==str:
                        
                        sauce = sauce.replace('\r','\n')
                        sauce = sauce.replace('\n\n\n',' ')
                        sauce = sauce.replace('\n\n',' ')
                        sauce = sauce.replace('\n',' ')
                        
                    dictionary[sub_id][characteristic] = sauce
                    
                except:
                    
                    continue
                
        sub_counter +=500
        
        print(sub_counter)
      
        after_date_child = submissions['data'][-1]['created_utc']
    
    return dictionary
        
        
    
    


# In[39]:


# Final function which takes (dictionary,subreddit,query,before_date,after_date,submission_characteristics,comment_characteristics) and returns an updated version of the dictionary containing all submissions and comments for given subreddits and time-frame

def scrape_reddit_sub(subreddit_list,after_date,before_date,submission_characteristics):
    
    master_dic = {}
    
    for subreddit in subreddit_list:
        
        print('Starting to scrape '+subreddit)
        
        try:
            
            master_dic[subreddit] = submissions_parent(subreddit,after_date,before_date,submission_characteristics)
            
        except:
            
            continue
            
    output = {}
    
    for key,item in master_dic.items():
        
        try:
            
            output.update(master_dic[key])
            
        except:
            
            continue
        
    return output


# In[40]:


# HERE WE GOOOO!


sub_scrape = scrape_reddit_sub(master_subreddits_list,1470009601,1488326399,sub_characteristics)



# In[41]:


df = pd.DataFrame(sub_scrape).T

try:
    df.to_csv('../output/submission_scrape/submission_scrape__08.04.20_08.16_02.17.csv', index=False, encoding = 'utf-8')
    #df.to_pickle('../output/comment_scrape/pickle/submission_scrape_08.04.20_08.16_02.17.pkl')
except:
    print('failed')


