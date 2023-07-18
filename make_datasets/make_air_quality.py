from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

import pandas as pd
import numpy as np
import time

from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

imputeDB = False
systemds = False

num_cols = ['AQI', 'CO', 'O3', 'PM10', 'PM2.5', 'NO2', 'NOx', 'NO', 'WindSpeed', 'WindDirec', 'SO2_AVG']
def read_data():
    df = pd.read_csv("taiwan.csv")
    df = df.dropna(subset=['AQI'])
    #df = pd.get_dummies(df, columns = ["City"])
    df["DataCreationDate"] = pd.to_datetime(df["DataCreationDate"], format="%Y-%m-%d %H:%M")
    #df = df[(df['DataCreationDate'] < '2019-01-01') & (df['DataCreationDate'] >= '2017-01-01')]

    df.drop(['County', 'Pollutant', 'Status', 'Unit', 'Longitude', 'Latitude', 'SiteId', 'CO_8hr', "SiteName", "DataCreationDate"], axis=1, inplace=True)

    cast = ["CO", "O3", "O3_8hr", "PM10","PM2.5","WindSpeed","WindDirec","PM10_AVG"]

    df[cast] = df[cast].apply(pd.to_numeric, errors='coerce', downcast='float')

    df.drop(["PM10_AVG", "SO2", 'PM2.5_AVG', 'O3_8hr'], axis=1, inplace=True)
    #num_cols = ['CO', 'O3', 'PM10', 'PM2.5', 'NO2', 'NOx', 'NO', 'WindSpeed', 'WindDirec', 'CO_8hr', 'SO2_AVG']

    scaler3 = StandardScaler()
    scaler3.fit(df[["WindSpeed", "NO"]])

    scaler2 = StandardScaler()
    df[num_cols] = scaler2.fit_transform(df[num_cols])
    df = df.reset_index(drop=True)
    return df, scaler3

def get_float_cols(df):
  return df.columns[(df.dtypes == float)].tolist()

def float_fixed_precision(df, prec):
  df = df.copy()
  for col in get_float_cols(df):
    # check if the column actually uses decimal points
    vals = df[col]
    not_missing = vals[~np.isnan(vals)]
    if (not_missing != np.floor(not_missing)).any():
      df[col] = np.floor(np.round(df[col], prec) * 10 ** prec)
  return df
  
data, scaler = read_data()
print(data.columns)

data = float_fixed_precision(data, 3)
if imputeDB:
    data.to_csv('air_quality_imputedb.csv', index=False, header=True, na_rep='nan',float_format='%.0f')
else:
    data.to_csv('air_quality_postgres.csv', index=False, header=False)
if systemds:
    cat_numerical = (((data.nunique()<=2).values).astype(int)).reshape(1,-1)
    np.savetxt("cat_numerical.csv", cat_numerical, delimiter=",")
