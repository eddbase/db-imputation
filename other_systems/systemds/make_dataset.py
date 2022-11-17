import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn
from math import pi

def load_flights(path = "data/flights/", nulls={'CRS_DEP_HOUR':0.25, 'DISTANCE':0.3, 'TAXI_OUT':0.15, 'TAXI_IN':0.1, 'DIVERTED':0.1, 'ARR_DELAY':0.2, 'DEP_DELAY':0.1}, max_rows = 50000, models = 
{'CRS_DEP_HOUR':'regression', 'DISTANCE':'regression', 'TAXI_OUT':'regression', 'TAXI_IN':'regression', 'DIVERTED':'classification', 'ARR_DELAY':'regression', 'DEP_DELAY':'regression'}):#'snow':0.1

    schedule = pd.read_csv(path+"schedule.csv")
    distances = pd.read_csv(path+"distances.csv")
    airlines_data = pd.read_csv(path+"airlines_data.csv")

    if max_rows is not None:
        airlines_data = airlines_data[0:max_rows].copy()


    # scale
    num_cols = ["CRS_DEP_HOUR", "CRS_DEP_MIN", "CRS_ARR_HOUR", "CRS_ARR_MIN", "CRS_ELAPSED_TIME"]  # list(set(airlines_data.columns)-{"ORIGIN", "DEST", "OP_CARRIER", "OP_CARRIER_FL_NUM", "FL_DATE"})
    scaler1 = StandardScaler()
    schedule[num_cols] = scaler1.fit_transform(schedule[num_cols])

    scaler2 = StandardScaler()
    distances["DISTANCE"] = scaler2.fit_transform(distances["DISTANCE"].values.reshape(-1, 1))
    scaler3 = StandardScaler()
    num_cols = list(set(airlines_data.columns) - {"FL_DATE", "DIVERTED", "flight"})
    airlines_data[num_cols] = scaler3.fit_transform(airlines_data[num_cols])

    c2 = schedule.columns.get_loc("CRS_DEP_HOUR")
    c1 = airlines_data.columns.get_loc("DEP_TIME_HOUR")
    c3 = airlines_data.columns.get_loc("DEP_DELAY")

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

        # process classes
    distances = pd.get_dummies(distances, columns=["ORIGIN", "DEST"])
    schedule = pd.get_dummies(schedule, columns=["OP_CARRIER"])  # OP_CARRIER_FL_NUM
    schedule.drop(["OP_CARRIER_FL_NUM", "CRS_ELAPSED_TIME"], axis=1, inplace=True)  # drop elapsed time (derived col)

    # add nulls
    true_values = {}
    # insert nan in distances and schedule
    columns_nulls = set(nulls.keys())
    distances_columns = set(distances.columns)
    distances_columns_null = columns_nulls & distances_columns

    idxs_nan = {}
    for col in distances_columns_null:
        distances[col+"_true_label"] = distances[col]
        np.random.seed(0)
        idx_nan = np.sort(np.random.choice(len(distances), int(len(distances)*nulls[col]), replace=False))
        true_values[col] = distances.iloc[idx_nan, distances.columns.get_loc(col)].copy()
        distances.iloc[idx_nan, distances.columns.get_loc(col)] = np.nan

    schedule_columns = set(schedule.columns)
    schedule_columns_null = columns_nulls & schedule_columns

    for col in schedule_columns_null:
        schedule[col+"_true_label"] = schedule[col]
        np.random.seed(0)
        idx_nan = np.sort(np.random.choice(len(schedule), int(len(schedule) * nulls[col]), replace=False))
        true_values[col] = schedule.iloc[idx_nan, schedule.columns.get_loc(col)].copy()
        schedule.iloc[idx_nan, schedule.columns.get_loc(col)] = np.nan

    # join
    airlines_data = (schedule.set_index("airports").join(distances.set_index("airports"), how="inner")).reset_index().set_index("flight").join(airlines_data.set_index("flight"), how="inner").reset_index()
    # print("COLS: ", list(airlines_data.columns))
    # airlines_data.loc[((airlines_data["ARR_TIME"] < airlines_data["CRS_ARR_TIME"]) & (airlines_data["ARR_DELAY"] >= 0)), "ARR_TIME"] += 24*60#if flight arrives next day, count time starting from prev. day
    print(list(airlines_data.columns))
    no_agg_true_labels = {}

    for col in distances_columns_null:
        no_agg_true_labels[col] = airlines_data.loc[pd.isna(airlines_data[col]), col+"_true_label"].values
        airlines_data.drop(col+"_true_label", axis=1, inplace=True)
    for col in schedule_columns_null:
        no_agg_true_labels[col] = airlines_data.loc[pd.isna(airlines_data[col]), col+"_true_label"].values
        airlines_data.drop(col+"_true_label", axis=1, inplace=True)

    # print(list(airlines_data.columns))
    #add 2 features

    airlines_data["EXTRA_DAY_ARR"] = ((airlines_data["ARR_TIME_HOUR"] < airlines_data["CRS_ARR_HOUR"]) & (airlines_data["ARR_DELAY"] >= 0)).astype(int)  # the flight arrived the next day
    airlines_data["EXTRA_DAY_DEP"] = ((((airlines_data["DEP_TIME_HOUR"] / scaler3.scale_[c1]) + scaler3.mean_[c1]) < ((airlines_data["CRS_DEP_HOUR"] / scaler1.scale_[c2]) + scaler1.mean_[c2])) & 
(((airlines_data["DEP_DELAY"] / scaler3.scale_[c3]) + scaler3.mean_[c3] >= 0))).astype(int)

    # print(scaler3.scale_[c1], scaler3.data_min_[c1])
    # print(scaler1.scale_[c2], scaler1.data_min_[c2])

    # print(airlines_data[((((airlines_data["DEP_TIME_HOUR"]/scaler3.scale_[c1]) + scaler3.data_min_[c1]) < ((airlines_data["CRS_DEP_HOUR"]/scaler1.scale_[c2])+scaler1.data_min_[c2])) & 
(((airlines_data["DEP_DELAY"]/scaler3.scale_[c3])+scaler3.data_min_[c3] >= 0)))][["DEP_TIME_HOUR", "DEP_TIME_MIN", "CRS_DEP_HOUR", "CRS_DEP_MIN", "DEP_DELAY", "EXTRA_DAY_DEP"]])

    #add nulls last table

    airlines_data_col_null = (columns_nulls - schedule_columns) - distances_columns
    i = 1
    for col in airlines_data_col_null:
        np.random.seed(i)
        idx_nan = np.sort(np.random.choice(len(airlines_data), int(len(airlines_data) * nulls[col]), replace=False))
        true_values[col] = airlines_data.iloc[idx_nan, airlines_data.columns.get_loc(col)].copy()
        airlines_data.iloc[idx_nan, airlines_data.columns.get_loc(col)] = np.nan
        #self.idxs_nan[airlines_data.columns.get_loc(col)] = idx_nan
        i += 1

    flight = airlines_data["flight"]
    airports = airlines_data["airports"]


    print("COLS: ", list(airlines_data.columns))

    airlines_data.drop(["flight", "airports"], axis=1, inplace=True)
    cols_models = {}
    col_to_idx = {}
    idx_to_col = {}
    for col in nulls.keys():
        index_no = airlines_data.columns.get_loc(col)
        cols_models[index_no] = models[col]
        col_to_idx[col] = index_no
        idx_to_col[index_no] = col
        idxs_nan[airlines_data.columns.get_loc(col)] = airlines_data.loc[pd.isna(airlines_data[col]), :].index.values
        #print("IDX NAN col ", airlines_data.columns.get_loc(col), " : ", self.idxs_nan[airlines_data.columns.get_loc(col)])
        #print(self.idxs_nan[airlines_data.columns.get_loc(col)])

    true_values = true_values
    col_to_idx = col_to_idx
    col_models = cols_models
    idx_to_col = idx_to_col
    print("Diverted: ", airlines_data["DIVERTED"])
    return airlines_data, cols_models, idxs_nan

a,b,c = load_flights()
a.to_csv('input_data.csv', header=False, index=False, na_rep='')
cat_numerical = (((a.nunique()<=2).values).astype(int)).reshape(1,-1)

np.savetxt("cat_numerical.csv", cat_numerical, delimiter=",")

