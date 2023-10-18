from hyperimpute.plugins.utils.simulate import simulate_nan
import pandas as pd
import numpy as np


def read_data_flights():
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

def read_data_retailer():
    df = pd.read_csv("retailer_limited.csv")
    df = df.drop(['locn', 'dateid', 'zip', 'rgn_cd', 'clim_zn_nbr', 'ksn', 'subcategory','category', 'categoryCluster'], axis=1)
    
    return df, None



from hyperimpute.plugins.imputers import Imputers
from hyperimpute.plugins.utils.metrics import RMSE
from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn
from math import pi
from sklearn.preprocessing import LabelEncoder
import numpy as np
from sklearn.metrics import mean_squared_error
import time

NULLS = [0.05, 0.1, 0.2, 0.4, 0.6, 0.8] #[0.01, 0.025, 0.1, 0.4]
PATTERNS = ['MCAR','MAR','MNAR']
DATASET = 'retailer'

train_midas=False
train_mice=True#
train_gain=True#
train_miracle=False#
train_miwae=False#
train_missforest=True#
train_diffusion_model=False
train_mean = True#
#
train_sinkhorn = False
train_softimpute = False

from hyperimpute.utils.distributions import enable_reproducible_results
seed = 0

if DATASET == 'retailer':
    data, scaler = read_data_retailer()
else:
    data, scaler = read_data_flights()


def compute_statistics(dataset, data_set="flight"):
    means = []
    sums = []
    sums_vector_1 = []
    sums_vector_2 = []
    r2s = []
    mse = []
    coeff_lr = []
    MSE = []
    
    if data_set=="flight":
        train_data, test_data = train_test_split(dataset, train_size=0.8)
        reg = LinearRegression().fit(train_data.drop(["ACTUAL_ELAPSED_TIME"], axis=1), train_data["ACTUAL_ELAPSED_TIME"])
        r2s += [r2_score(test_data["ACTUAL_ELAPSED_TIME"], reg.predict(test_data.drop("ACTUAL_ELAPSED_TIME", axis=1)))]
        mse += [mean_squared_error(test_data["ACTUAL_ELAPSED_TIME"], reg.predict(test_data.drop("ACTUAL_ELAPSED_TIME",  axis=1)), squared=False)]
    elif data_set == "retailer":
        train_data, test_data = train_test_split(dataset, train_size=0.8)
        reg = LinearRegression().fit(train_data.drop(["targetdistance"], axis=1), train_data["targetdistance"])
        r2s += [r2_score(test_data["targetdistance"], reg.predict(test_data.drop("targetdistance", axis=1)))]
        mse += [mean_squared_error(test_data["targetdistance"], reg.predict(test_data.drop("targetdistance",  axis=1)), squared=False)]
        
    df = pd.DataFrame(data={'r2': r2s, 'rmse': mse, 'dataset': data_set})
    df["r2"] = r2s
    df["mse"] = mse
    df["dataset"] = data_set
    return df

df_res = compute_statistics(data, data_set=DATASET)
df_res["null"] = 0
df_res["pattern"] = 'NA'
df_res["rmse_vals"] = 0
df_res["time"] = 0
df_res["alg"] = ""
print(df_res)

for null in NULLS:
    for pattern in PATTERNS:
        col_nan = []
        
        enable_reproducible_results(seed)
        seed+=1
        
        if DATASET == 'retailer':
            if pattern == 'MAR':
                col_nan = ['inventoryunits']
            col_nan = ['males', 'females', 'targetdrivetime', 'population', 'occupiedhouseunits']
        else:
            if pattern == 'MAR':
                col_nan = ['ACTUAL_ELAPSED_TIME']
            col_nan += ["WHEELS_ON_HOUR", "WHEELS_OFF_HOUR", "TAXI_OUT", "TAXI_IN", "ARR_DELAY", "DEP_DELAY", "DISTANCE"]
        
        d = data.copy()
        data_not_nan = data.drop(col_nan, axis=1)
        print("introducing nulls...")
        if pattern == 'MAR':
            res = simulate_nan(data[col_nan].values, null, "MAR", p_obs = 1/len(col_nan), sample_columns=False)
        else:
            res = simulate_nan(data[col_nan].values, null, pattern)
        missing = pd.DataFrame(res['X_incomp'], columns = col_nan)
        print("done")
        d = pd.concat([data_not_nan, missing], axis=1, verify_integrity=True)
        
        #----
        if train_gain:
            print("gain")
            t1 = time.time()
            plugin = Imputers().get("gain", n_epochs = 100000)
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "gain"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("gain", loss, t2)
        
        if train_miracle:
            plugin = Imputers().get("miracle", max_steps=30)
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "miracle"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("miracle", loss, t2)
        
        if train_miwae:
            plugin = Imputers().get("miwae", n_epochs=5)
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "miwae"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("miwae", loss, t2)
        
        if train_mice:
            print("start mice")
            plugin = Imputers().get("mice", n_imputations = 1, max_iter=5)
            print(plugin.n_imputations)
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "mice"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("mice", loss, t2)
        
        if train_missforest:
            plugin = Imputers().get("missforest", max_iter = 20, n_estimators=2)
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "missforest"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("missforest", loss, t2)
        
        if train_sinkhorn:
            plugin = Imputers().get("sinkhorn", n_epochs=5)
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "sinkhorn"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("sinkhorn", loss, t2)
        
        if train_softimpute:
            plugin = Imputers().get("softimpute", maxit=5)
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "softimpute"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("softimpute", loss, t2)
        
        if train_mean:
            plugin = Imputers().get("mean")
            t1 = time.time()
            out = plugin.fit_transform(d.copy())
            t2 = time.time() - t1
            out = pd.DataFrame(data=out.values, columns = d.columns)
            loss = RMSE(out[col_nan].values, data[col_nan].values, res['mask'])
            metrics = compute_statistics(out, data_set=DATASET)
            metrics["null"] = null
            metrics["pattern"] = pattern
            metrics["rmse_vals"] = loss
            metrics["time"] = t2
            metrics["alg"] = "mean"
            df_res = pd.concat([df_res, metrics], ignore_index=True)
            print("mean", loss)
df_res.to_csv('out.csv', index=False)

