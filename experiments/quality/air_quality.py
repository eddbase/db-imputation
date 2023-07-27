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
from sklearn.preprocessing import StandardScaler

def compute_statistics(imputed_regression, scaler):
    r2s = []
    mse = []
    coeff_lr = []

    for dataset in imputed_regression:
        d = dataset.copy()        
        train_data, test_data = train_test_split(dataset, train_size=0.8, random_state=2)
        reg = LinearRegression().fit(train_data.drop(["AQI"], axis=1), train_data["AQI"])
        r2s += [r2_score(test_data["AQI"], reg.predict(test_data.drop("AQI", axis=1)))]
        mse += [mean_squared_error(test_data["AQI"], reg.predict(test_data.drop("AQI", axis=1)))]
        print("R2: ", r2_score(test_data["AQI"], reg.predict(test_data.drop("AQI", axis=1))))        
        #print([d[d["OP_CARRIER"] == "9E"]["DISTANCE"].mean()])

    d = {'r2': r2s, 'mse': mse}
    df = pd.DataFrame(data=d)
    
    return df

#CO_8hr
num_cols = ['CO', 'O3', 'PM10', 'PM2.5', 'NO2', 'NOx', 'NO', 'WindSpeed', 'WindDirec', 'SO2_AVG']

def read_data():
    df = pd.read_csv("../../make_datasets/taiwan.csv")
    df = df.dropna(subset=['AQI'])
    #df = pd.get_dummies(df, columns = ["City"])
    df["DataCreationDate"] = pd.to_datetime(df["DataCreationDate"], format="%Y-%m-%d %H:%M")
    #df = df[(df['DataCreationDate'] < '2019-01-01') & (df['DataCreationDate'] >= '2017-01-01')]

    df.drop(['County', 'Pollutant', 'Status', 'Unit', 'Longitude', 'Latitude', 'SiteId', 'CO_8hr'], axis=1, inplace=True)

    cast = ["CO", "O3", "O3_8hr", "PM10","PM2.5","WindSpeed","WindDirec","PM10_AVG"]

    df[cast] = df[cast].apply(pd.to_numeric, errors='coerce', downcast='float')

    df.drop(["PM10_AVG", "SO2", 'PM2.5_AVG', 'O3_8hr'], axis=1, inplace=True)
    #num_cols = ['CO', 'O3', 'PM10', 'PM2.5', 'NO2', 'NOx', 'NO', 'WindSpeed', 'WindDirec', 'CO_8hr', 'SO2_AVG']
    
    scaler3 = StandardScaler()
    scaler3.fit(df[["WindSpeed", "NO"]])
    
    scaler2 = StandardScaler()
    df[["AQI"]+num_cols] = scaler2.fit_transform(df[["AQI"]+num_cols])
    df = df.reset_index(drop=True)
    return df, scaler3

data, scaler = read_data()

import MIDASpy as md
from sklearn.model_selection import train_test_split

DATASETS = 1
#print("statistics FULL DATASET: ")

from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import IterativeImputer

#PREDICT AQI
from sklearn.model_selection import train_test_split
from autoimpute.imputations import MiceImputer

train_size = 0.8

missing_data = data.drop(['SiteName', 'DataCreationDate'], axis=1)

cols_names = list(missing_data.columns)
imp = IterativeImputer(random_state=0, sample_posterior=True)
t1 = time.time()
imputed_regression = imp.fit_transform(missing_data)
imputed_regression = pd.DataFrame(imputed_regression, columns = cols_names)
t_ = time.time()-t1
print("time sklearn", t_)
df_res1 = compute_statistics([imputed_regression], scaler)

#print(rf_complete)



col_imp = {}
for x in num_cols:
    col_imp[x] = "stochastic"

imp = MiceImputer(n=DATASETS, k=5, strategy="stochastic", return_list=True)

t1 = time.time()
imputed_regression = imp.fit_transform(missing_data)
print("MICE")
t_ = time.time()-t1
print("time lib", t_)
print("statistics MICE: ")

df_res = compute_statistics([k[1] for k in imputed_regression], scaler)
df_res["alg"] = "mice"
df_res["time"] = t_
    
## RANDOM FOREST

#import sys
#import sklearn.neighbors._base
#sys.modules['sklearn.neighbors.base'] = sklearn.neighbors._base
#import sys
#import sklearn.neighbors._base
#sys.modules['sklearn.neighbors.base'] = sklearn.neighbors._base
print("MissForest")
from missingpy import MissForest
imputer = MissForest(max_iter=5, n_estimators=10, n_jobs=-1)
#kds = mf.ImputationKernel(missing_data, save_all_iterations=False, datasets=DATASETS, random_state=1991)
    
t1 = time.time()
# Run the MICE algorithm for x iterations
#kds.mice(5)
cols_names = list(missing_data.columns)
rf_complete = imputer.fit_transform(missing_data)
rf_complete = pd.DataFrame(rf_complete, columns = cols_names)

#print(rf_complete)
t_ = time.time()-t1
print("time ", t_)
    
# Return the completed dataset.
#rf_complete = [kds.complete_data(dataset=x) for x in range(DATASETS)]
print("statistics RF: ")
df_res1 = compute_statistics([rf_complete], scaler)
df_res1["alg"] = "RF"
df_res1["time"] = t_
df_res = pd.concat([df_res, df_res1], ignore_index=True)

    
## DEEP LEARNING
imputer = md.Midas(layer_structure = [256,256], vae_layer = False, seed = 89, input_drop = 0.75)
t1 = time.time()
imputer.build_model(missing_data)#, softmax_columns = cat_cols_list
imputer.train_model(training_epochs = 5)
t_ = time.time()-t1
print("time ", t_)
imputations_dl = imputer.generate_samples(m=DATASETS).output_list
print("statistics DL: ")
df_res1 = compute_statistics(imputations_dl, scaler)
df_res1["alg"] = "DL"
df_res1["time"] = t_
df_res = pd.concat([df_res, df_res1], ignore_index=True)
    
col_imp = {}
for x in num_cols:
    col_imp[x] = "mean"
    
##MEAN
imp = MiceImputer(n=DATASETS, k=1, strategy=col_imp, return_list=True, seed=101)
t1 = time.time()
imputed_regression = imp.fit_transform(missing_data)
t_ = time.time()-t1
print("time ", time.time()-t_)
#print("MEAN")
print("statistics MEAN: ", compute_statistics([k[1] for k in imputed_regression], scaler))
df_res1 = compute_statistics([k[1] for k in imputed_regression], scaler)
df_res1["alg"] = "mean"
df_res1["time"] = t_
df_res = pd.concat([df_res, df_res1], ignore_index=True)
df_res.to_csv("results_air.csv", index=False)
