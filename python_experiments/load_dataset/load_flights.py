# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All"
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session

import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

def load_flights(path = "../datasets/flight/", nulls={'Delay':0.2, 'CANCELLED':0.2, 'DIVERTED':0.2, 'SCHEDULED_DEPARTURE':0.2, 'AIR_TIME':0.2, 'DISTANCE':0.2}, max_rows = 400000, models = {'Delay':'classification', 'CANCELLED':'classification', 'DIVERTED':'classification', 'SCHEDULED_DEPARTURE':'regression', 'AIR_TIME':'regression', 'DISTANCE':'regression'}):#'snow':0.1
#0,3,4,7,8,9
    low_memory=False
    flights_path = path + "flights.csv"

    # Load the data
    flights_data = pd.read_csv(flights_path, dtype={'ORIGIN_AIRPORT': str, 'DESTINATION_AIRPORT': str})

    if max_rows is not None:
        flights_data = flights_data[0:max_rows].copy()

    flights_data=flights_data.drop(['YEAR','FLIGHT_NUMBER','AIRLINE','TAIL_NUMBER','SCHEDULED_TIME','DEPARTURE_TIME','WHEELS_OFF','ELAPSED_TIME','WHEELS_ON','CANCELLATION_REASON', 'ARRIVAL_TIME', 'AIR_SYSTEM_DELAY',  'SECURITY_DELAY'  ,'AIRLINE_DELAY',  'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY'],
                                                 axis=1)

    #print(len(pd.unique(flights_seg['FLIGHT_NUMBER'])))
    #print(len(pd.unique(flights_seg['ORIGIN_AIRPORT'])))

    #year column is unneccesary since the data is bounded to 2015 but day and month are important


    Flight_data_delay =[]
    for row in flights_data['ARRIVAL_DELAY']:
        if row > 60:
            Flight_data_delay.append(3)
        elif row > 30:
            Flight_data_delay.append(2)
        elif row > 15:
            Flight_data_delay.append(1)
        else:
            Flight_data_delay.append(0)

    print("dropping")
    flights_data.drop('ARRIVAL_DELAY', axis=1, inplace=True)

    #

    flights_data['Delay'] = Flight_data_delay

    print("fillna")

    flights_data=flights_data.fillna(flights_data.mean())

    print("cols: ", flights_data.columns)
    one_hot = pd.get_dummies(flights_data[["ORIGIN_AIRPORT", "DESTINATION_AIRPORT", 'MONTH', 'DAY', 'DAY_OF_WEEK']])
    flights_data = flights_data.drop(["ORIGIN_AIRPORT", "DESTINATION_AIRPORT", 'MONTH', 'DAY', 'DAY_OF_WEEK'],axis = 1)
    # Join the encoded df
    flights_data = flights_data.join(one_hot)


    #one_hot = pd.get_dummies(flights_data[["ORIGIN_AIRPORT", "DESTINATION_AIRPORT"]])
    # Drop column B as it is now encoded
    #pd.set_option('display.max_columns', None)
    #print(flights_data.head())

    print("scaling")
    num_cols = ['SCHEDULED_DEPARTURE', 'DEPARTURE_DELAY', 'TAXI_OUT', 'AIR_TIME', 'DISTANCE', 'TAXI_IN', 'SCHEDULED_ARRIVAL']
    flights_data[num_cols] = StandardScaler().fit_transform(flights_data[num_cols])


    true_vals = {}
    cols_models = {}

    for k, v in nulls.items():
        print(k)
        ix = np.sort(np.random.choice(len(flights_data), size=int(v * len(flights_data)), replace=False))
        index_no = flights_data.columns.get_loc(k)
        true_vals[index_no] = flights_data.loc[ix, k].copy()
        flights_data.loc[ix, k] = np.nan
        cols_models[index_no] = models[k]

    return flights_data, true_vals, cols_models

