import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn
from math import pi
from sklearn.preprocessing import LabelEncoder

NULL = 0

def load_flights(path = "flights/", nulls={'DIVERTED':NULL, 'WHEELS_ON_HOUR':NULL, 'WHEELS_OFF_HOUR':NULL, 'TAXI_OUT':NULL, 'TAXI_IN':NULL, 'ARR_DELAY':NULL, 'DEP_DELAY':NULL}, max_rows = None):#'snow':0.1

    schedule = pd.read_csv(path+"schedule.csv")
    distances = pd.read_csv(path+"distances.csv")
    airlines_data = pd.read_csv(path+"airlines_data.csv")

    if max_rows is not None:
        airlines_data = airlines_data[0:max_rows].copy()

    airlines_data = airlines_data.reset_index(drop=True).reset_index(drop=False)
    print(airlines_data.head())


    # scale
    num_cols = ["CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "CRS_ELAPSED_TIME"]
    scaler1 = StandardScaler()
    schedule[num_cols] = scaler1.fit_transform(schedule[num_cols])

    scaler2 = StandardScaler()
    distances["DISTANCE"] = scaler2.fit_transform(distances["DISTANCE"].values.reshape(-1, 1))
    scaler3 = StandardScaler()
    num_cols = list(set(airlines_data.columns) - {"FL_DATE", "DIVERTED", "flight","index"})
    airlines_data[num_cols] = scaler3.fit_transform(airlines_data[num_cols])

    c2 = schedule.columns.get_loc("CRS_DEP_HOUR")
    c1 = airlines_data.columns.get_loc("DEP_TIME_HOUR")
    c3 = airlines_data.columns.get_loc("DEP_DELAY")

    print(airlines_data.head())

    # process date
    airlines_data["FL_DATE"] = pd.to_datetime(airlines_data["FL_DATE"], format="%Y-%m-%d")
    months_orig = airlines_data["FL_DATE"].dt.month - 1
    days_orig = airlines_data["FL_DATE"].dt.day - 1
    days_in_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])

    theta = 2 * pi * (months_orig / 12)
    month_sin = np.sin(theta)
    month_cos = np.cos(theta)

    theta = 2 * pi * (days_orig / days_in_month[months_orig])
    day_sin = np.sin(theta)
    day_cos = np.cos(theta)

    theta = 2 * pi * (airlines_data["FL_DATE"].dt.weekday / 7)
    weekday_sin = np.sin(theta)
    weekday_cos = np.cos(theta)

    airlines_data["MONTH_SIN"] = month_sin
    airlines_data["MONTH_COS"] = month_cos

    airlines_data["DAY_SIN"] = day_sin
    airlines_data["DAY_COS"] = day_cos

    airlines_data["WEEKDAY_SIN"] = weekday_sin
    airlines_data["WEEKDAY_COS"] = weekday_cos

    airlines_data.drop("FL_DATE", axis=1, inplace=True)
    schedule.drop(["OP_CARRIER_FL_NUM", "CRS_ELAPSED_TIME"], axis=1, inplace=True)  # drop elapsed time (derived col)

    # add nulls
    true_values = {}
    # insert nan in distances and schedule
    columns_nulls = set(nulls.keys())
    distances_columns = set(distances.columns)
    distances_columns_null = columns_nulls & distances_columns

    # join
    airlines_data = (schedule.set_index("airports").join(distances.set_index("airports"), how="inner")).reset_index().set_index("flight").join(airlines_data.set_index("flight"), how="inner").reset_index()
    airlines_data["EXTRA_DAY_ARR"] = ((airlines_data["ARR_TIME_HOUR"] < airlines_data["CRS_ARR_HOUR"]) & (airlines_data["ARR_DELAY"] >= 0)).astype(int)  # the flight arrived the next day
    airlines_data["EXTRA_DAY_DEP"] = ((((airlines_data["DEP_TIME_HOUR"] / scaler3.scale_[c1]) + scaler3.mean_[c1]) < ((airlines_data["CRS_DEP_HOUR"] / scaler1.scale_[c2]) + scaler1.mean_[c2])) & (((airlines_data["DEP_DELAY"] / scaler3.scale_[c3]) + scaler3.mean_[c3] >= 0))).astype(int)

    #add nulls in fact table

    for col in columns_nulls:
        idx_nan = np.sort(np.random.choice(len(airlines_data), int(len(airlines_data) * nulls[col]), replace=False))
        airlines_data.iloc[idx_nan, airlines_data.columns.get_loc(col)] = np.nan

    distances_dataset = airlines_data.drop_duplicates(subset=["airports"], keep='first', inplace=False)[["airports","ORIGIN","DEST","DISTANCE"]] #,"ORIGIN","DEST"
    schedule_dataset = airlines_data.drop_duplicates(subset=["flight"], keep='first', inplace=False)[["flight","OP_CARRIER","CRS_DEP_HOUR","CRS_DEP_MIN","CRS_ARR_HOUR","CRS_ARR_MIN","airports"]] #OP_CARRIER

    print("COLS: ", list(airlines_data.columns))

    airlines_data = airlines_data[["index","DEP_DELAY","TAXI_OUT","TAXI_IN","ARR_DELAY","DIVERTED","ACTUAL_ELAPSED_TIME","AIR_TIME","DEP_TIME_HOUR","DEP_TIME_MIN","WHEELS_OFF_HOUR","WHEELS_OFF_MIN","WHEELS_ON_HOUR","WHEELS_ON_MIN","ARR_TIME_HOUR","ARR_TIME_MIN","MONTH_SIN","MONTH_COS","DAY_SIN","DAY_COS","WEEKDAY_SIN","WEEKDAY_COS","EXTRA_DAY_ARR","EXTRA_DAY_DEP","flight"]]
    airlines_data = airlines_data.drop_duplicates(subset=["index"],keep='first', inplace=False)
    print(airlines_data.head())
    label_encoder = LabelEncoder()
    distances_dataset["ORIGIN"] = label_encoder.fit_transform(distances_dataset["ORIGIN"])
    label_encoder = LabelEncoder()
    distances_dataset["DEST"] = label_encoder.fit_transform(distances_dataset["DEST"])
    label_encoder = LabelEncoder()
    schedule_dataset["OP_CARRIER"] = label_encoder.fit_transform(schedule_dataset["OP_CARRIER"])

    return airlines_data, distances_dataset, schedule_dataset

a,b,c = load_flights()
print(a.columns)
print(b.columns)
print(c.columns)
c["airports"] = c["airports"].astype(int)
b["airports"] = b["airports"].astype(int)
a["DIVERTED"] = a["DIVERTED"].astype('Int64')

print(a.shape)

a.to_csv('airlines_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
b.to_csv('route_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
c.to_csv('schedule_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')

