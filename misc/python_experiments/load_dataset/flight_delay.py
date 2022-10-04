# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load
import time

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os

from sklearn.linear_model import LinearRegression
from sklearn.metrics import accuracy_score, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All"
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session

# Store the path in variables

def load_flights(nulls={'DEPARTURE_DELAY': 0.2}):

    airlines_path = "../../datasets/next_try/2015_flight_delays/airlines.csv"
    airport_path = "../../datasets/next_try/2015_flight_delays/airports.csv"
    flights_path = "../../datasets/next_try/2015_flight_delays/flights.csv"

    # Load the data
    airlines_data = pd.read_csv(airlines_path)
    airport_data = pd.read_csv(airport_path)
    flights_data = pd.read_csv(flights_path)

    #lets take a segment of this data for now
    flights_seg = flights_data[:150000].copy()

    #year column is unneccesary since the data is bounded to 2015 but day and month are important
    delay =[]
    for row in flights_seg['ARRIVAL_DELAY']:
        if row > 60:
            delay.append(3)
        elif row > 30:
            delay.append(2)
        elif row > 15:
            delay.append(1)
        else:
            delay.append(0)
    flights_seg['delay'] = delay


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

    flights_data['Delay'] = Flight_data_delay

    flights_data=flights_data.drop(['YEAR','FLIGHT_NUMBER','AIRLINE','DISTANCE','TAIL_NUMBER','TAXI_OUT','SCHEDULED_TIME','DEPARTURE_TIME','WHEELS_OFF','ELAPSED_TIME','AIR_TIME','WHEELS_ON','DAY_OF_WEEK','TAXI_IN','CANCELLATION_REASON','ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', "CANCELLED"],
                                                 axis=1)

    flights_data=flights_data.fillna(flights_data.mean())

    print(flights_data.columns)

    scaler = StandardScaler()
    flights_data[['MONTH', 'DAY', 'DEPARTURE_DELAY','SCHEDULED_DEPARTURE',
       'SCHEDULED_ARRIVAL', 'DIVERTED', 'AIR_SYSTEM_DELAY', 'SECURITY_DELAY',
       'AIRLINE_DELAY', 'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY']] = scaler.fit_transform(flights_data[['MONTH', 'DAY', 'DEPARTURE_DELAY','SCHEDULED_DEPARTURE',
       'SCHEDULED_ARRIVAL', 'DIVERTED', 'AIR_SYSTEM_DELAY', 'SECURITY_DELAY',
       'AIRLINE_DELAY', 'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY']])

    true_vals = {}

    for k, v in nulls.items():
        print(k)
        ix = np.sort(np.random.choice(len(flights_data), size=int(v * len(flights_data)), replace=False))
        true_vals[k] = flights_data.loc[ix, k].copy()
        flights_data.loc[ix, k] = np.nan
    return flights_data, true_vals

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
#clf = LinearDiscriminantAnalysis()
clf = LinearRegression()
X, Y = load_flights()

train = X[~X['DEPARTURE_DELAY'].isna()]


t1 = time.time()
t1 = time.time()
clf.fit(train.drop(['DEPARTURE_DELAY'], axis=1).values, train['DEPARTURE_DELAY'].values)
print(time.time() - t1)

t2 = time.time()
print('train size: ', train.shape, 'time: ', t2-t1)

new_matches_test = clf.predict(X[X['DEPARTURE_DELAY'].isna()].drop(['DEPARTURE_DELAY'], axis=1).values)

print(new_matches_test)
print(Y)

acc = mean_squared_error(Y['DEPARTURE_DELAY'], new_matches_test)
print(acc)

from sklearn.metrics import f1_score

print(f1_score(Y['SCHEDULED_DEPARTURE'], new_matches_test, average='micro'))
