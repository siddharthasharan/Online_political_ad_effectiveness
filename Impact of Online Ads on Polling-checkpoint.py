


import math
import json

import facebook # finally facebook is imported ... facebook graph api


# start exploratory data analysis of the complete political ad dataset from Facebook
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
pd.show_versions()

# Use url https://www.facebook.com/ads/library/report/
# download All data ( Actual data from May 2018 to AUg 17 2019 !!)
from pandas import read_csv
df = pd.read_csv(r"C:\python\data\fb_ll_advertisers.csv")


df.head()
df.rename(columns={'Page ID':'ID',
                   'Page Name':'page_name',
                   'Amount Spent (USD)':'amt_spent',
                   'Number of Ads in Library':'num_ads_lib'}, 
                 inplace=True)
print(df.columns)

df.dtypes
df.describe()

df.count()


df['amt_spent'].describe() # Gives values as <= 100 will need to remove those rows as it's very low value
df = df[df.amt_spent != '≤100']
df['amt_spent'].describe() 

df['page_name'].describe() # Drop rows where all data is the same as there are too many page names which are non sensical
df = df.drop_duplicates()
df = df.dropna(axis = 0, how ='any') 
df.describe() # reduces the count by half after removing duplicate page names
df['amt_spent'].astype(str).astype(int)

df.describe()
df['amt_spent'] = df['amt_spent'].str.replace('$','').str.replace(',','')
df['amt_spent'] = pd.to_numeric(df['amt_spent'])
num_bins = 5
plt.hist(df['amt_spent'], num_bins, normed=1, facecolor='blue', alpha=0.5)
plt.show()
#distribution of amount spent shows most of the ads very low spend which means that the dataset needs further filtering

#1. Kamala Harris
kh = df[df['page_name'].str.contains('Kamala Harris')]
# 2. Pete Buttigieg
pb = df[df['page_name'].str.contains('Pete Buttigieg')]
# 3. Beto O'Rourke
bor = df[df['page_name'].str.contains("Beto O'Rourke")]
#4. Cory Booker
cb=  df[df['page_name'].str.contains("Cory Booker")]
dem_candidates = pd.concat([kh, pb, bor, cb])

dem_candidates.head(10)


# plotting the spend and ads for these candidates- KH
num_bins = 10
plt.hist(kh['amt_spent'], num_bins, normed=1, facecolor='blue', alpha=0.5)
plt.show()

num_bins = 10
plt.hist(kh['num_ads_lib'], num_bins, normed=1, facecolor='green', alpha=0.5)
plt.show()
print(kh)
# plotting the spend and ads for these candidates- PB
num_bins = 10
plt.hist(pb['amt_spent'], num_bins, normed=1, facecolor='blue', alpha=0.5)
plt.show()

num_bins = 10
plt.hist(pb['num_ads_lib'], num_bins, normed=1, facecolor='green', alpha=0.5)
plt.show()
print(pb)
num_bins = 10
plt.hist(bor['amt_spent'], num_bins, normed=1, facecolor='blue', alpha=0.5)
plt.show()

num_bins = 10
plt.hist(bor['num_ads_lib'], num_bins, normed=1, facecolor='green', alpha=0.5)
plt.show()
print(bor)
num_bins = 10
plt.hist(cb['amt_spent'], num_bins, normed=1, facecolor='blue', alpha=0.5)
plt.show()

num_bins = 10
plt.hist(cb['num_ads_lib'], num_bins, normed=1, facecolor='green', alpha=0.5)
plt.show()
print(cb)
# Data in all advertisers matches closely to the full report pull
#page level data is not tied to spend
# page level data is also not tied to impressions or demographics 
# page level data is not tied to timeline of spend
# run regularly to see if there are any changes
import pandas as pd
import numpy as np
import requests
from io import BytesIO
#for 538 data
url = 'https://projects.fivethirtyeight.com/polls-page/president_primary_polls.csv'
response = requests.get(url, verify =False)
content = BytesIO(response.content)
df = pd.read_csv(content)

#filtering for candidates
sel_polls = df.loc[df['candidate_name'].isin(['Kamala D. Harris','Pete Buttigieg',"Beto O'Rourke", 'Cory A. Booker'])]
# filter for A grade polls only
sel_polls = sel_polls.loc[sel_polls['fte_grade'].isin(['A','A-','A+'])]
# remove additional columns which are not necessary for this analysis
temp_polls= sel_polls.drop(['question_id','poll_id','state','pollster_id',
                            'sponsor_ids','display_name','pollster_rating_id', 'sponsor_candidate',
                            'internal','partisan','tracking','nationwide_batch','notes','url'], axis = 1)
poll_pivot = pd.pivot_table(temp_polls, values='pct', index=['candidate_name'], columns=['end_date'], aggfunc=np.average)
# pivoting data for all A grade polls by end date by average
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (30,20)
# polling chart
end_date = poll_pivot.columns
kh = poll_pivot.loc['Kamala D. Harris']
cb = poll_pivot.loc['Cory A. Booker']
br = poll_pivot.loc["Beto O'Rourke"]
pb = poll_pivot.loc['Pete Buttigieg']
plt.plot(end_date, kh, 'g--', label='KH')
plt.plot(end_date, cb, 'b--', label='CB')
plt.plot(end_date, br, 'r--', label='BR')
plt.plot(end_date, pb, 'o--', label='PB')
poll_pivot.T.plot()
plt.ylabel('percent per candidate') # alternate


get_ipython().run_line_magic('load_ext', 'google.cloud.bigquery')
import os 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/livingly/Downloads/My Project 78759-01227524add5.json"

# putting the results in dataframe
# ad weekly spend data
from google.cloud import bigquery
client = bigquery.Client()
sql = """
select EXTRACT(YEAR FROM week_start_date) year, 
       EXTRACT(MONTH FROM week_start_date) month, 
       advertiser_name, 
       (sum(spend_usd)+ sum(spend_inr))/70 as spend 
       from `bigquery-public-data.google_political_ads.advertiser_weekly_spend` 
        where advertiser_id in (select advertiser_id
                                       from `bigquery-public-data.google_political_ads.advertiser_stats`  
                                       where elections = "US-Federal") 
        and week_start_date between '2019-01-01' and '2019-09-01'
        group by 1, 2, 3 
        order by 1 desc, 2 desc, 4 desc
"""
ad_spend_g_ads = client.query(sql).to_dataframe()
ad_spend_g_ads.head()
# campaign details by month data
sql1 = """
SELECT advertiser_name, count(ads_list) as ad_counts, extract(month from start_date) month 
      FROM `bigquery-public-data.google_political_ads.campaign_targeting` 
       where advertiser_id in 
       (select advertiser_id from `bigquery-public-data.google_political_ads.advertiser_stats`  where elections = "US-Federal") 
       and start_date between '2019-01-01' and '2019-09-01'
       group by 1, 3
order by 3 desc, 2 desc
"""
campaign_details = client.query(sql1).to_dataframe()
campaign_details.head()

# creative stats
sql2 = """
select advertiser_name, impressions,extract(month from date_range_start) month, sum(num_of_days) as days_run 
from `bigquery-public-data.google_political_ads.creative_stats` 
where advertiser_id in 
       (select advertiser_id from `bigquery-public-data.google_political_ads.advertiser_stats`  where elections = "US-Federal") 
and date_range_start between '2019-01-01' and '2019-09-01'
and date_range_end between '2019-01-01' and '2019-09-01'
group by 1,2, 3 order by 4 desc limit 100
"""
creative_stats = client.query(sql2).to_dataframe()
creative_stats.head()
