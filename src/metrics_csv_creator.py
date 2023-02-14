import requests as rq
import json
import pandas as pd
import re
import numpy as np
from datetime import datetime

# defining changable variables
start_date='2023-01-31T12:10:30.781Z'
end_date='2023-02-01T13:11:00.781Z'
step_time='15s'

# define csv file to pull data
name_and_query = pd.read_csv('commands.csv')

# use for execute first by a different rule
boole = True

# get queries from csv file
for query in name_and_query.iloc[:,1]:
    
    # get request
    data = rq.get(f'http://localhost:9090/api/v1/query_range?query={query}&start={start_date}&end={end_date}&step={step_time}')
    
    # turn request str into json
    data = data.json()

    # get data values by parsing
    all_data = np.array(data['data']['result'][0]['values'])
    
    # get metric data
    metric  = all_data[:,1][np.newaxis]
    
    # get time stamp data
    time_stamp = all_data[:,0][np.newaxis]
    #metric = metric.apply(lambda x: GiB(float(x)), axis=1)
    # for executing just once
    if boole:
        temp_data = np.concatenate((time_stamp.T,metric.T),axis=1)
        boole=False

    # merge data collectively
    else:
    
        temp_data = np.concatenate((temp_data, metric.T), axis=1)
        

# start title list with one title which is the same for all
titles = ['time_stamp']

# loop title list for all
for names in name_and_query.iloc[:,0]:
    titles.append(names)

# a function to convert byte into megabyte
def div(x):
    x = x/1048576
    y="{:.3f}".format(x)
    y=y+"Mb"
    return y

# get data into dataframe object
dataframe = pd.DataFrame(temp_data,columns=titles)

# turn time stamp data into clear data
dataframe["DateTime"] = dataframe.apply(lambda x: datetime.fromtimestamp(float(x["time_stamp"])), axis=1)

# turn byte data into mb data
for cols in dataframe.columns:
    if (cols !=  "time_stamp") and (cols != "DateTime"):
        # usage of created div function
        dataframe[cols] = dataframe.apply(lambda y: div(float(y[cols])), axis = 1)

# save dataframe into new created csv
dataframe.to_csv('last_state.csv')


