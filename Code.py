
# coding: utf-8

# In[35]:


## python version 3.6.5


# In[36]:


## import libraries
import pandas as pd
import numpy as np
import datetime
import sys


# In[37]:


## import data and get summary statistics
dtypes = {'uuid': np.str, 'ts': 'float', 'useragent': np.str, 'hashed_ip': np.str}
parse_dates = ['ts']
data=pd.read_csv('logs.csv', sep=',', dtype = dtypes , parse_dates=parse_dates)
data.describe()


# In[38]:


## explore wether missing values may be a problem to solve the task
missing = data[data.isnull().any(axis=1)]
missing
# as missing values only occur in column useragent, they should not matter for this purpose


# In[39]:


## Calculate day (without time), weekday and houre from timestamp
# Day is calculate including year and month (although not needed here) to be able to use the code for datasets with more than one month of data as well
data['day'] = data.ts.map(lambda x: x.strftime('%Y-%m-%d'))
data['weekday'] = data['ts'].dt.weekday_name
data['weekday_no'] = data['ts'].dt.weekday
data['hour'] = data['ts'].dt.hour


# In[40]:


## Calculate if event occured during business hours (True/False); assumption: Monday to Friday, 9 to 5
data['business_hours'] = False
for i, row in data.iterrows():    
    if row['weekday_no'] <= 4 and row['hour'] >= 9 and row['hour'] <= 16:    
        data.at[i, 'business_hours'] = True


# In[41]:


## Look at the distribution of the count of events per uuid
ts_per_user = data.groupby(['uuid'])['ts'].agg(['count', 'first', 'last'])
ts_per_user.describe(percentiles=[.25,.5,.75,.9,.95])


# In[42]:


# show distribution of count of timestamps; the last bin is limited to 8+ numbers of timestamps
histogram = ts_per_user
for i, row in histogram.iterrows():
    if row['count'] >= 8:
        histogram.at[i, 'count'] = 8
histogram.hist(column='count', bins = 8)


# In[43]:


## Define highly_active
# To define whether a user is highly active, I would use a relative approach. 
# The distribution shows that the top 5% of the users have visited the site at least five times in the last month. That is on average approx. twice per week.
# These shall be defined as highly_active here. A less restrictive definition would be to limit it to at least 5 visits (top 5%)
# I choose to make the loop dynamic. If the 95%-percentile changes in another dataset, the threshold for highly_active changes adequately
ts_per_user['95_percentile_threshold'] = np.percentile(ts_per_user['count'], 95)

ts_per_user['highly_active'] = False
for i, row in ts_per_user.iterrows():
    if row['count'] >= row['95_percentile_threshold']:
        ts_per_user.at[i, 'highly_active'] = True

# calculate distribution of highly_active
highly_active_true = ts_per_user['highly_active'].agg('sum')
highly_active_false = ts_per_user['highly_active'].agg('count') - highly_active_true
ha_true_false = [highly_active_true, highly_active_false]
print("Highly active: {} \n Not highly active: {}".format(*ha_true_false))


# In[44]:


## Define multiple_days
days_per_user = data.groupby(['uuid'])['day'].agg(['nunique', 'first',  'last'])
days_per_user['multiple_days'] = False

for i, row in days_per_user.iterrows():
    if row['nunique'] > 1:
        days_per_user.at[i, 'multiple_days'] = True

# calculate distribution of multiple_days
multiple_days_true = days_per_user['multiple_days'].agg('sum')
multiple_days_false = days_per_user['multiple_days'].agg('count') - multiple_days_true
md_true_false = [multiple_days_true, multiple_days_false]
print("Visited site on multiple days: {} \n Visited site on only one day: {}".format(*md_true_false))


# In[45]:


## Define weekday_biz
# I define the following: A user tends to visit the site during business ours (assumption: 9 to 5) if it has more events during these hours than during other hours. 
# It is further assumed that there aren't any public or personal holidays during the observed period

business_hours = data.groupby(['uuid'])['business_hours'].agg(['count', 'sum'])
business_hours['weekday_biz'] = False
for i, row in business_hours.iterrows():
    if row['sum'] > row['count']/2:
        business_hours.at[i, 'weekday_biz'] = True

# calculate distribution of weekday_biz
weekday_biz_true = business_hours['weekday_biz'].agg('sum')
weekday_biz_false = business_hours['weekday_biz'].agg('count') - weekday_biz_true
wb_true_false = [weekday_biz_true, weekday_biz_false]
print("Visited site preferably during business hours: {} \n Visited site preferably during recreational hours: {}".format(*wb_true_false))


# In[46]:


## Define 4th component 'days_since_last_activity'
# Assumption: The analysis is made at the last day of the each month (In this case 2017-07-31)
# The longer a user hasn't visited the site, the more unlikely is it that he will not book anything

latest_date = data.day.max()
latest_date = pd.to_datetime(latest_date)
days_per_user['last'] = pd.to_datetime(days_per_user['last'], format='%Y-%m-%d')

days_per_user['days_since_last_activity'] = latest_date - days_per_user['last'] 

# convert days_since_last_activity to int
days_per_user['days_since_last_activity'] = days_per_user['days_since_last_activity'].astype(datetime.timedelta).map(lambda x: np.nan if pd.isnull(x) else x.days)

# calculate distribution of days_since_last_activity
days_per_user.hist(column='days_since_last_activity', bins=30)


# In[47]:


# off topic: if needed in future analysis: the first day is also converted to datetime here
days_per_user['first'] = pd.to_datetime(days_per_user['first'], format='%Y-%m-%d')


# In[49]:


## Join the KPIs in one table and drop all information that is not required in the output
kpis = ts_per_user.merge(days_per_user, left_index=True, right_index=True)
kpis = kpis.merge(business_hours, left_index=True, right_index=True)
kpis = kpis.drop(columns=['count_x', 'first_x', 'last_x', '95_percentile_threshold', 'nunique', 'first_y', 'last_y', 'count_y', 'sum'])


# In[50]:


## Write output to stdout
print(kpis)
save_kpis = sys.stdout


# In[51]:


## Write output to csv
kpis.to_csv('output.csv')

