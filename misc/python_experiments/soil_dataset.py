import numpy as np
import pandas as pd
import json
import os
import matplotlib.pyplot as plt
import seaborn as sns
import time

from sklearn.multioutput import MultiOutputRegressor
from tqdm.auto import tqdm
from datetime import datetime
from scipy.interpolate import interp1d
from sklearn.preprocessing import RobustScaler

sns.set_style('white')

files = {}

for dirname, _, filenames in os.walk('../datasets/soil'):
    for filename in filenames:
        if 'train' in filename:
            files['train'] = os.path.join(dirname, filename)
        if 'valid' in filename:
            files['valid'] = os.path.join(dirname, filename)
        if 'test' in filename:
            files['test'] = os.path.join(dirname, filename)

print(files)

class2id = {
    'None': 0,
    'D0': 1,
    'D1': 2,
    'D2': 3,
    'D3': 4,
    'D4': 5,
}
id2class = {v: k for k, v in class2id.items()}

index2feature = {
    0: 'PRECTOT',
    1: 'PS',
    2: 'QV2M',
    3: 'T2M',
    4: 'T2MDEW',
    5: 'T2MWET',
    6: 'T2M_MAX',
    7: 'T2M_index2feature = {',
    0: 'PRECTOT',
    1: 'PS',
    2: 'QV2M',
    3: 'T2M',
    4: 'T2MDEW',
    5: 'T2MWET',
    6: 'T2M_MAX',
    7: 'T2M_MIN',
    8: 'T2M_RANGE',
    9: 'TS',
    10: 'WS10M',
    11: 'WS10M_MAX',
    12: 'WS10M_MIN',
    13: 'WS10M_RANGE',
    14: 'WS50M',
    15: 'WS50M_MAX',
    16: 'WS50M_MIN',
    17: 'WS50M_RANGE',
    18: 'SCORE',
    19: 'DATE_SIN',
    20: 'DATE_COS'
}
feature2index = {v: k for k, v in index2feature.items()}
list(feature2index)

dfs = {
    k: pd.read_csv(files[k]).set_index(['fips', 'date'])
    for k in files.keys()
}

def interpolate_nans(padata, pkind='linear'):
    """
    see: https://stackoverflow.com/a/53050216/2167159
    """
    aindexes = np.arange(padata.shape[0])
    agood_indexes, = np.where(np.isfinite(padata))
    f = interp1d(agood_indexes
               , padata[agood_indexes]
               , bounds_error=False
               , copy=False
               , fill_value="extrapolate"
               , kind=pkind)
    return f(aindexes)

def date_encode(date):
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")
    return (
        np.sin(2 * np.pi * date.timetuple().tm_yday / 366),
        np.cos(2 * np.pi * date.timetuple().tm_yday / 366)
    )

def loadXY(
    df,
    random_state=42, # keep this at 42
    window_size=60, # how many days in the past (default/competition: 180)
    target_size=6, # how many weeks into the future (default/competition: 6)
    fuse_past=True, # add the past drought observations? (default: True)
    return_fips=False, # return the county identifier (do not use for predictions)
    encode_season=True, # encode the season using the function above (default: True)
    use_prev_year=False, # add observations from 1 year prior?
):
    df = dfs[df]
    soil_df = pd.read_csv("../datasets/soil/soil_data.csv")
    time_data_cols = sorted(
        [c for c in df.columns if c not in ["fips", "date", "score"]]
    )
    static_data_cols = sorted(
        [c for c in soil_df.columns if c not in ["soil", "lat", "lon"]]
    )
    count = 0
    score_df = df.dropna(subset=["score"])
    X_static = np.empty((len(df) // window_size, len(static_data_cols)))
    X_fips_date = []
    add_dim = 0
    if use_prev_year:
        add_dim += len(time_data_cols)
    if fuse_past:
        add_dim += 1
        if use_prev_year:
            add_dim += 1
    if encode_season:
        add_dim += 2
    X_time = np.empty(
        (len(df) // window_size, window_size, len(time_data_cols) + add_dim)
    )
    y_past = np.empty((len(df) // window_size, window_size))
    y_target = np.empty((len(df) // window_size, target_size))
    if random_state is not None:
        np.random.seed(random_state)
    for fips in tqdm(score_df.index.get_level_values(0).unique()):
        if random_state is not None:
            start_i = np.random.randint(1, window_size)
        else:
            start_i = 1
        fips_df = df[(df.index.get_level_values(0) == fips)]
        X = fips_df[time_data_cols].values
        y = fips_df["score"].values
        X_s = soil_df[soil_df["fips"] == fips][static_data_cols].values[0]
        for i in range(start_i, len(y) - (window_size + target_size * 7), window_size):
            X_fips_date.append((fips, fips_df.index[i : i + window_size][-1]))
            X_time[count, :, : len(time_data_cols)] = X[i : i + window_size]
            if use_prev_year:
                if i < 365 or len(X[i - 365 : i + window_size - 365]) < window_size:
                    continue
                X_time[count, :, -len(time_data_cols) :] = X[
                    i - 365 : i + window_size - 365
                ]
            if not fuse_past:
                y_past[count] = interpolate_nans(y[i : i + window_size])
            else:
                X_time[count, :, len(time_data_cols)] = interpolate_nans(
                    y[i : i + window_size]
                )
            if encode_season:
                enc_dates = [
                    date_encode(d) for f, d in fips_df.index[i : i + window_size].values
                ]
                d_sin, d_cos = [s for s, c in enc_dates], [c for s, c in enc_dates]
                X_time[count, :, len(time_data_cols) + (add_dim - 2)] = d_sin
                X_time[count, :, len(time_data_cols) + (add_dim - 2) + 1] = d_cos
            temp_y = y[i + window_size : i + window_size + target_size * 7]
            y_target[count] = np.array(temp_y[~np.isnan(temp_y)][:target_size])
            X_static[count] = X_s
            count += 1
    print(f"loaded {count} samples")
    results = [X_static[:count], X_time[:count], y_target[:count]]
    if not fuse_past:
        results.append(y_past[:count])
    if return_fips:
        results.append(X_fips_date)
    return results

scaler_dict = {}
scaler_dict_static = {}
scaler_dict_past = {}


def normalize(X_static, X_time, y_past=None, fit=False):
    for index in tqdm(range(X_time.shape[-1])):
        if fit:
            scaler_dict[index] = RobustScaler().fit(X_time[:, :, index].reshape(-1, 1))
        X_time[:, :, index] = (
            scaler_dict[index]
            .transform(X_time[:, :, index].reshape(-1, 1))
            .reshape(-1, X_time.shape[-2])
        )
    for index in tqdm(range(X_static.shape[-1])):
        if fit:
            scaler_dict_static[index] = RobustScaler().fit(
                X_static[:, index].reshape(-1, 1)
            )
        X_static[:, index] = (
            scaler_dict_static[index]
            .transform(X_static[:, index].reshape(-1, 1))
            .reshape(1, -1)
        )
    index = 0
    if y_past is not None:
        if fit:
            scaler_dict_past[index] = RobustScaler().fit(y_past.reshape(-1, 1))
        y_past[:, :] = (
            scaler_dict_past[index]
            .transform(y_past.reshape(-1, 1))
            .reshape(-1, y_past.shape[-1])
        )
        return X_static, X_time, y_past
    return X_static, X_time

X_static_train, X_time_train, y_target_train = loadXY("train")
print("train shape", X_time_train.shape)
X_static_train, X_time_train = normalize(X_static_train, X_time_train, fit=True)

print('First line of above data frame:')
print(X_time_train[0][0])
print('...')
print('Last line of above data frame:')
print(X_time_train[0][-1])
print()
print('Labels to be predicted:')
print(y_target_train[0])

print(X_time_train.shape)
X_train = np.array(list(map(lambda x: x.flatten(), X_time_train)))
print(X_train.shape)
print(X_static_train.shape)
X_train = np.concatenate((X_train, X_static_train), axis=1)
print(X_train.shape)

import seaborn as sns
import matplotlib.pyplot as plt
from sklearn import metrics
import re
import warnings

from sklearn.linear_model import LinearRegression

tic = time.perf_counter()
linearRegressor = MultiOutputRegressor(LinearRegression(normalize=True))
linearRegressor.fit(X_train, np.squeeze(y_target_train))

X_static_valid, X_time_valid, y_target_valid = loadXY("valid")
print("validation shape", X_time_valid.shape)
X_static_valid, X_time_valid = normalize(X_static_valid, X_time_valid)
print(X_time_valid.shape)
X_valid = np.array(list(map(lambda x: x.flatten(), X_time_valid)))
print(X_valid.shape)
print(X_static_valid.shape)
X_valid = np.concatenate((X_valid, X_static_valid), axis=1)
print(X_valid.shape)

def round_and_intify(y):
    return np.clip(np.squeeze(y).round().astype('int'), 0, 5)

def summarize(y_true, y_pred):
    weeks_true = np.split(y_true, 6, 1)
    weeks_pred = np.split(y_pred, 6, 1)
    matrices = []
    macro_f1s = []
    maes = []
    for y_true, y_pred in zip(weeks_true, weeks_pred):
        y_true = round_and_intify(y_true.flatten())
        y_pred = round_and_intify(y_pred.flatten())
        report = metrics.classification_report(y_true, y_pred, digits=3)
        print(report)
        maes += [np.mean(abs(y_true - y_pred))]
    print(maes)


y_pred_valid = linearRegressor.predict(X_valid)
summarize(y_target_valid, y_pred_valid)
toc = time.perf_counter()
duration = toc - tic
minutes = int(duration/60)
print(f"Elapsed time: {str(minutes) + ' minutes, ' if minutes else ''}{int(duration % 60)} seconds")