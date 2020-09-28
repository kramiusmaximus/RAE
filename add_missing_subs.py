import pandas as pd

df_main = pd.read_csv('../../output/formatted/comment_scrape_08.04.20_08.16_02.17_v1.csv')

df_other = pd.read_csv('...')

df_out = pd.concat([df_main,df_other])

df_out.to_csv('../../output/formatted/comment_scrape_08.16_02.17_final.csv')