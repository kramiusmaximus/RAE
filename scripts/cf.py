import csv
import pandas as pd


# Clean submissions
"""
subs = open('../output/submission_scrape/submission_scrape_09.04.20_08.16_02.17.csv','rU') #.read().replace('\n \n',' ').replace('\n\n',' ')
    
reader = csv.reader(subs)

csv_file = open('../output/formatted/submission_scrape_09.04.20_08.16_02.17_v1.csv', 'w')

writer = csv.writer(csv_file)

for row in reader:
    
    if len(row) == 14:
        
        writer.writerow(row)

# csv_file.close()
# """
# # Clean comments
# print('hi')
# com = open('../output/comment_scrape/comment_scrape_11.04.20_08.16_02.17_missing.csv','rU') #.read().replace('\n \n',' ').replace('\n\n',' ')
    
# reader = csv.reader(com)

# csv_file = open('../output/formatted/comment_scrape_11.04.20_08.16_02.17_missing.csv', 'w')

# writer = csv.writer(csv_file)

# for row in reader:
    
#     if len(row) == 10:
#         row_1 = row
#         row_1[2] = row_1[2].replace('\n',' ')
        
#         writer.writerow(row_1)

# csv_file.close()

# # append coments tro existing comments

# df1 = pd.read_csv('../output/formatted/comment_scrape_08.04.20_08.16_02.17_v1.csv', encoding = 'utf-8',low_memory = False)
# df2 = pd.read_csv('../output/formatted/comment_scrape_11.04.20_08.16_02.17_missing.csv', encoding = 'utf-8',low_memory = False)

# df_main = pd.concat([df1,df2], ignore_index = True)

df = pd.read_csv('../output/formatted/comment_scrape_08.04.20_08.16_02.17_v2.csv', encoding = 'utf-8')
df = df[(df['created_utc']>1478577600)&(df['created_utc']<1478664000)]
dfd = df[df['subreddit']=='The_Donald'] 
dfp = df[df['subreddit']=='politics']
dfpd = df[df['subreddit'] == 'PoliticalDiscussion']
dfh = df[df['subreddit']=='hillaryclinton']
dfd = dfd[['created_utc','score','body','subreddit']]
dfp = dfp[['created_utc','score','body','subreddit']]
dfpd = dfpd[['created_utc','score','body','subreddit']]
dfh = dfh[['created_utc','score','body','subreddit']]
dfd.to_csv('dfd.csv', index = False, encoding = 'utf-8')
dfp.to_csv('dfp.csv', index = False, encoding = 'utf-8')
dfpd.to_csv('dfpd.csv', index = False, encoding = 'utf-8')
dfh.to_csv('dfh.csv', index = False, encoding = 'utf-8')





