import pandas as pd
import numpy as np
import sklearn
from pandas._libs.tslibs.offsets import CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler


def load_sf_bike(nulls={'trips': 0.2, 'mean_visibility_miles': 0.1, 'mean_temperature_f':0.1}):

    df = pd.read_csv("../../datasets/sf_bike_sharing/trip.csv")
    weather = pd.read_csv("../../datasets/sf_bike_sharing/weather.csv")
    stations = pd.read_csv("../../datasets/sf_bike_sharing/station.csv")

    print(len(df))

    df.duration /= 60

    #remove outlier
    df['duration'].quantile(0.995)
    df = df[df.duration <= 360]
    df.start_date = pd.to_datetime(df.start_date, format='%m/%d/%Y %H:%M')
    df['date'] = df.start_date.dt.date

    dates = {}
    for d in df.date:
        if d not in dates:
            dates[d] = 1
        else:
            dates[d] += 1

    df2 = pd.DataFrame.from_dict(dates, orient = "index")
    df2['date'] = df2.index
    df2['trips'] = df2.iloc[:,0]
    train = df2.iloc[:,1:3]
    train.reset_index(drop = True, inplace = True)

    train = train.sort_values('date')
    train.reset_index(drop=True, inplace=True)

    weather.date = pd.to_datetime(weather.date, format='%m/%d/%Y')

    weather = weather[weather.zip_code == 94107]
    weather.loc[weather.events == 'rain', 'events'] = "Rain"
    weather.loc[weather.events.isnull(), 'events'] = "Normal"
    events = pd.get_dummies(weather.events)
    weather = weather.merge(events, left_index = True, right_index = True)
    #Remove features we don't need
    weather = weather.drop(['events','zip_code'], axis=1)
    weather.loc[weather.max_gust_speed_mph.isnull(), 'max_gust_speed_mph'] = weather.groupby('max_wind_Speed_mph').max_gust_speed_mph.apply(lambda x: x.fillna(x.median()))
    weather.precipitation_inches = pd.to_numeric(weather.precipitation_inches, errors = 'coerce')
    weather.loc[weather.precipitation_inches.isnull(),
                'precipitation_inches'] = weather[weather.precipitation_inches.notnull()].precipitation_inches.median()
    train = train.merge(weather, on = train.date)
    train['date'] = train['date_x']
    train.drop(['date_y','date_x'],1, inplace= True)
    stations.installation_date = pd.to_datetime(stations.installation_date, format = "%m/%d/%Y").dt.date
    total_docks = []
    for day in train.date:
        total_docks.append(sum(stations[stations.installation_date <= day].dock_count))
    train['total_docks'] = total_docks
    #Find all of the holidays during our time span
    calendar = USFederalHolidayCalendar()
    holidays = calendar.holidays(start=train.date.min(), end=train.date.max())
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    business_days = pd.date_range(start=train.date.min(), end=train.date.max(), freq=us_bd)
    business_days = pd.to_datetime(business_days, format='%Y/%m/%d').date
    #print(business_days)
    holidays = pd.to_datetime(holidays, format='%Y/%m/%d').date
    #A 'business_day' or 'holiday' is a date within either of the respected lists.
    train['business_day'] = train.date.isin(business_days)
    train['holiday'] = train.date.isin(holidays)
    #Convert True to 1 and False to 0
    train.business_day = train.business_day.map(lambda x: 1 if x == True else 0)
    train.holiday = train.holiday.map(lambda x: 1 if x == True else 0)
    #Convert date to the important features, year, month, weekday (0 = Monday, 1 = Tuesday...)
    #We don't need day because what it represents changes every year.
    train['year'] = pd.to_datetime(train['date']).dt.year
    train['month'] = pd.to_datetime(train['date']).dt.month
    train['weekday'] = pd.to_datetime(train['date']).dt.weekday

    print(train.dtypes)

    #labels = train.trips
    train = train.drop(['date', 'key_0'], axis=1)
    scaler = StandardScaler()
    train[train.columns] = scaler.fit_transform(train[train.columns])
    
    true_vals = {}

    for k,v in nulls.items():
        print(v)
        ix = np.random.choice(len(train), size=int(v * len(train)), replace=False)
        true_vals[k] = train.loc[ix, k].copy()
        train.loc[ix, k] = np.nan

    return train, true_vals

'''
def inventory_dataset(nulls=0.2):
    table = pd.read_csv(r"/home/mperini/inventory.csv")

    vals = table.values

    scaler = StandardScaler()
    scaler.fit(vals)
    vals = scaler.transform(vals)
    #vals = vals[:30000, [x for x in range(30)]]
    shape = vals.shape
    print("min: ", np.amin(vals), " max: ", np.amax(vals))

    vals = vals.flatten().copy().astype(float)

    n = len(vals)
    np.random.seed(1)
    ix = np.random.choice(n, size=int(nulls * n), replace=False)
    vals[ix] = np.nan
    vals = vals.reshape(shape)

    return vals, None #pd.DataFrame(vals, columns=table.columns).values, None
'''

'''
X_train, X_test, y_train, y_test = train_test_split(train, labels, test_size=0.2, random_state = 2)
#15 fold cross validation. Multiply by -1 to make values positive.
#Used median absolute error to learn how many trips my predictions are off by.

def scoring(clf):
    scores = cross_val_score(clf, X_train, y_train, cv=15, n_jobs=1, scoring = 'neg_mean_squared_error')
    print (np.median(scores) * -1)
rfr = LinearRegression()
scoring(rfr)
'''
