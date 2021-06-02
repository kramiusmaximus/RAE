import pandas as pd
import numpy as np
import networkx as nx
from datetime import datetime
from datetime import date
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import matplotlib.dates as mdates

df_c = pd.read_csv('../output/formatted/comment_scrape_08.04.20_08.16_02.17_v2.csv',encoding = 'utf-8',low_memory = False)
df_s = pd.read_csv('../output/formatted/submission_scrape_09.04.20_08.16_02.17_v1.csv',encoding = 'utf-8',low_memory = False)
df_main = pd.read_csv('../output/df_network.csv', low_memory=False,encoding = 'utf-8')


# daily comments and submissions
df_main['created_date'] = df_c['created_utc'].apply(lambda x: datetime.utcfromtimestamp(x).date())
comment_freq = df_main.groupby('created_date').count().sort_values('created_date')[['author']]

fig1 = plt.figure()
ax1 = fig1.add_axes([0,0,1,1])
ax1.plot(comment_freq, c='0',lw = 2)
locator = mdates.AutoDateLocator()
formatter = mdates.ConciseDateFormatter(locator)
ax1.xaxis.set_major_locator(locator)
ax1.xaxis.set_major_formatter(formatter)
ax1.set_xlim([min(list(comment_freq.index)),max(list(comment_freq.index))])
ax1.set_ylim([min(list(comment_freq.values)),max(list(comment_freq.values))+10000])
ax1.set_title('# New Comments')
ax1.vlines(date(2016,11,8),0,max(comment_freq.values),colors='r')
plt.savefig('../output/comment_freq.pdf',bbox_inches='tight')

# daily 'The_Donald' comments and submissions





# daily 'hillaryclinton' comments and submissions























