from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

from autoimpute.analysis import MiLinearRegression
from autoimpute.imputations import MiceImputer

import miceforest as mf
from sklearn.datasets import load_iris
import pandas as pd
import numpy as np
import time

from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn
from math import pi
from sklearn.preprocessing import LabelEncoder
import numpy as np

import scipy

import numpy as np

def sample_rows(data, sampling = "MCAR", fraction = 0.2, column = "col"):
    # Completely At Random
    if sampling == 'MCAR':
        rows = np.random.permutation(data.index)[:int(len(data)*fraction)]
    elif sampling == ('MNAR') or sampling == 'MAR':
        n_values_to_discard = int(len(data) * min(fraction, 1.0))
        perc_lower_start = np.random.randint(0, len(data) - n_values_to_discard)
        perc_idx = range(perc_lower_start, perc_lower_start + n_values_to_discard)
        # Not At Random
        if sampling == 'MNAR':
            # pick a random percentile of values in this column
            rows = data[column].sort_values().iloc[perc_idx].index
        # At Random
        elif sampling == 'MAR':
            depends_on_col = np.random.choice(list(set(data.columns) - {column}))
            # pick a random percentile of values in other column
            rows = data[depends_on_col].sort_values().iloc[perc_idx].index
    return rows

def corrupt_data(data, sampling, fraction, columns):    
    corrupted_data = data.copy(deep=True)
    for col in columns:
        rows = sample_rows(corrupted_data, sampling, fraction, col)
        #print(rows)
        corrupted_data.loc[rows, [col]] = np.nan
    return corrupted_data

def compute_statistics(imputed_regression, scaler):
    means = []
    sums = []
    sums_vector_1 = []
    sums_vector_2 = []
    r2s = []
    mse = []
    coeff_lr = []

    for dataset in imputed_regression:
        d = dataset.copy()
        d[["DEST","ORIGIN", "OP_CARRIER_FL_NUM", "OP_CARRIER"]] = data[["DEST","ORIGIN", "OP_CARRIER_FL_NUM", "OP_CARRIER"]]
        d["DISTANCE_SCALED"] = scaler.inverse_transform(d[["DISTANCE"]])
        means += [d["DISTANCE_SCALED"].mean()]
        sums += [d["DISTANCE_SCALED"].sum()]
        sums_vector_1 += [d[d["OP_CARRIER"] == "VX"]["DISTANCE_SCALED"].mean()]
        sums_vector_2 += [d[d["OP_CARRIER"] == "AS"]["DISTANCE_SCALED"].mean()]
        
        #TRAIN MODEL
        
        train_data, test_data = train_test_split(dataset, train_size=0.8, random_state=0)
        reg = LinearRegression().fit(train_data.drop(["ACTUAL_ELAPSED_TIME"], axis=1), train_data["ACTUAL_ELAPSED_TIME"])
        r2s += [r2_score(test_data["ACTUAL_ELAPSED_TIME"], reg.predict(test_data.drop("ACTUAL_ELAPSED_TIME", axis=1)))]
        mse += [mean_squared_error(test_data["ACTUAL_ELAPSED_TIME"], reg.predict(test_data.drop("ACTUAL_ELAPSED_TIME",  axis=1)))]
        print("R2: ", r2_score(test_data["ACTUAL_ELAPSED_TIME"], reg.predict(test_data.drop("ACTUAL_ELAPSED_TIME", axis=1))))
        coeff_lr += [reg.coef_[list(data.columns).index("DISTANCE")]]
        
        #print([d[d["OP_CARRIER"] == "9E"]["DISTANCE"].mean()])

    def mean_confidence_interval(data, confidence=0.95):
        a = 1.0 * np.array(data)
        n = len(a)
        m, se = np.mean(a), scipy.stats.sem(a)
        h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
        return [m, m-h, m+h]

    print("CI ", mean_confidence_interval(means))
    print("CI ", mean_confidence_interval(sums))
    print("CI ", mean_confidence_interval(sums_vector_1))
    print("CI ", mean_confidence_interval(sums_vector_2))
    
    print("CI LR PARAM", mean_confidence_interval(sums_vector_2))
    d = {'r2': r2s, 'mse': mse, 'mean': means, 'sum': sums, 'q_1': sums_vector_1, 'q_2': sums_vector_2}
    df = pd.DataFrame(data=d)
    
    return df

def read_data():
    df = pd.read_csv("flights_2015.csv")
    df.drop(['WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY', 'CARRIER_DELAY', 'Unnamed: 27'], axis=1, inplace=True)
    df = df[df["CANCELLED"] == 0]
    df = df[df["DIVERTED"] == 0]

    df["FL_DATE"] = pd.to_datetime(df["FL_DATE"], format="%Y-%m-%d")
    df = df[(df['FL_DATE'] < '2015-04-01')]

    months_orig = df["FL_DATE"].dt.month - 1
    days_orig = df["FL_DATE"].dt.day - 1
    days_in_month = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])

    theta = 2 * pi * (months_orig / 12)
    month_sin = np.sin(theta)
    month_cos = np.cos(theta)

    theta = 2 * pi * (days_orig / days_in_month[months_orig])
    day_sin = np.sin(theta)
    day_cos = np.cos(theta)

    theta = 2 * pi * (df["FL_DATE"].dt.weekday / 7)
    weekday_sin = np.sin(theta)
    weekday_cos = np.cos(theta)

    df["MONTH_SIN"] = month_sin
    df["MONTH_COS"] = month_cos

    df["DAY_SIN"] = day_sin
    df["DAY_COS"] = day_cos

    df["WEEKDAY_SIN"] = weekday_sin
    df["WEEKDAY_COS"] = weekday_cos

    df.drop("FL_DATE", axis=1, inplace=True)
    df.drop(["CRS_ELAPSED_TIME"], axis=1, inplace=True)
    df.drop(["AIR_TIME"], axis=1, inplace=True)

    df["DEP_TIME_HOUR"] = df["DEP_TIME"] //100
    df["DEP_TIME_MIN"] = df["DEP_TIME"] % 100

    df["CRS_DEP_HOUR"] = df["CRS_DEP_TIME"] //100
    df["CRS_DEP_MIN"] = df["CRS_DEP_TIME"] % 100

    df["CRS_ARR_HOUR"] = df["CRS_ARR_TIME"] //100
    df["CRS_ARR_MIN"] = df["CRS_ARR_TIME"] % 100

    df["WHEELS_OFF_HOUR"] = df["WHEELS_OFF"] //100
    df["WHEELS_OFF_MIN"] = df["WHEELS_OFF"] % 100

    df["WHEELS_ON_HOUR"] = df["WHEELS_ON"] //100
    df["WHEELS_ON_MIN"] = df["WHEELS_ON"] % 100

    df["ARR_TIME_HOUR"] = df["ARR_TIME"] //100
    df["ARR_TIME_MIN"] = df["ARR_TIME"] % 100

    df.drop(['ARR_TIME', 'WHEELS_ON', 'WHEELS_OFF', 'CRS_ARR_TIME', 'CRS_DEP_TIME', 'DEP_TIME', 'CANCELLATION_CODE'], axis=1, inplace=True)

    df["EXTRA_DAY_ARR"] = ((df["ARR_TIME_HOUR"] < df["CRS_ARR_HOUR"]) & (df["ARR_DELAY"] >= 0)).astype(int)
    df["EXTRA_DAY_DEP"] = ((df["DEP_TIME_HOUR"] < df["CRS_DEP_HOUR"]) & (df["DEP_DELAY"] >= 0)).astype(int)
    
    num_cols = ["DEP_DELAY", 'TAXI_OUT', 'TAXI_IN', 'ARR_DELAY', 'ACTUAL_ELAPSED_TIME', 'DISTANCE', 'DEP_TIME_HOUR', 'DEP_TIME_MIN', 'CRS_DEP_HOUR', 'CRS_DEP_MIN', 'CRS_ARR_HOUR', 'CRS_ARR_MIN', 'WHEELS_OFF_HOUR', 'WHEELS_OFF_MIN', 'WHEELS_ON_HOUR','WHEELS_ON_MIN', 'ARR_TIME_HOUR', 'ARR_TIME_MIN',]  # list(set(airlines_data.columns)-{"ORIGIN", "DEST", "OP_CARRIER", "OP_CARRIER_FL_NUM", "FL_DAT>
    scaler1 = StandardScaler()
    scaler2 = StandardScaler()
    scaler2.fit(df[["DISTANCE"]])
    df[num_cols] = scaler1.fit_transform(df[num_cols])
    df = df.reset_index(drop=True)
    
    return df, scaler2

data, scaler = read_data()

import MIDASpy as md
from sklearn.model_selection import train_test_split

import warnings
warnings.filterwarnings('ignore')

DATASETS = 1
NULLS = [0.01, 0.025, 0.1, 0.4]
train_size = 0.8

print("statistics FULL DATASET: ")
df_res = compute_statistics([data.drop(["DEST","ORIGIN", "OP_CARRIER_FL_NUM", "OP_CARRIER"], axis=1)], scaler)
df_res["nulls"] = 0
df_res["alg"] = "full"
df_res["time"] = 0


for null in NULLS:
    missing_data = corrupt_data(data, "MCAR", null, ["DIVERTED", "WHEELS_ON_HOUR", "WHEELS_OFF_HOUR", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "DEP_DELAY", "DISTANCE"])
    missing_data.drop(["DEST","ORIGIN", "OP_CARRIER_FL_NUM", "OP_CARRIER"], axis=1, inplace=True)
        
    imp = MiceImputer(n=DATASETS, k=6, strategy={"DIVERTED":"stochastic", "WHEELS_ON_HOUR": "stochastic", 'WHEELS_OFF_HOUR': "stochastic", 'TAXI_OUT': "stochastic", 'TAXI_IN': "stochastic", 'ARR_DELAY': "stochastic",'DEP_DELAY':'stochastic', 'DISTANCE':'stochastic'}
                  , return_list=True, seed=101)

    t1 = time.time()
    imputed_regression = imp.fit_transform(missing_data)
    print("MICE")
    t_ = time.time()-t1
    print("time ", t_)
    print("statistics MICE: ")
    
    df_res1 = compute_statistics([k[1] for k in imputed_regression], scaler)
    df_res1["nulls"] = null
    df_res1["alg"] = "mice"
    df_res1["time"] = t_
    
    df_res = pd.concat([df_res, df_res1], ignore_index=True)
    
    ## RANDOM FOREST
    
    from missingpy import MissForest
    imputer = MissForest(max_iter=6, n_estimators=5, n_jobs=-1)
    cols_names = list(missing_data.columns)
    t1 = time.time()
    rf_complete = imputer.fit_transform(missing_data)
    rf_complete = pd.DataFrame(rf_complete, columns = cols_names)
    t_ = time.time()-t1
    print("time ", t_)
    
    # Return the completed dataset.
    rf_complete = [rf_complete]
    #print(rf_complete)
    print("statistics RF: ")
    df_res1 = compute_statistics(rf_complete, scaler)
    df_res1["nulls"] = null
    df_res1["alg"] = "RF"
    df_res1["time"] = t_
    df_res = pd.concat([df_res, df_res1], ignore_index=True)

    
    ## DEEP LEARNING
    imputer = md.Midas(layer_structure = [256,256], vae_layer = False, seed = 89, input_drop = 0.75)
    t1 = time.time()
    imputer.build_model(missing_data)#, softmax_columns = cat_cols_list
    imputer.train_model(training_epochs = 8)
    t_ = time.time()-t1
    print("time ", t_)
    imputations_dl = imputer.generate_samples(m=DATASETS).output_list
    #print(imputations_dl)
    print("statistics DL: ")
    df_res1 = compute_statistics(imputations_dl, scaler)
    df_res1["nulls"] = null
    df_res1["alg"] = "DL"
    df_res1["time"] = t_
    df_res = pd.concat([df_res, df_res1], ignore_index=True)
    
    
    ##MEAN
    imp = MiceImputer(n=DATASETS, k=1, strategy={"DIVERTED":"mean", "WHEELS_ON_HOUR": "mean", 'WHEELS_OFF_HOUR': "mean", 'TAXI_OUT': "mean", 'TAXI_IN': "mean", 'ARR_DELAY': "mean",'DEP_DELAY':'mean', 'DISTANCE':'mean'} , return_list=True)
    t1 = time.time()
    imputed_regression = imp.fit_transform(missing_data)
    t_ = time.time()-t1
    print("time ", time.time()-t_)
    #print("MEAN")
    print("statistics MEAN: ", compute_statistics([k[1] for k in imputed_regression], scaler))
    df_res1 = compute_statistics([k[1] for k in imputed_regression], scaler)
    df_res1["nulls"] = null
    df_res1["alg"] = "mean"
    df_res1["time"] = t_
    df_res = pd.concat([df_res, df_res1], ignore_index=True)
    

df_res.to_csv("results_flight.csv", index=False)
