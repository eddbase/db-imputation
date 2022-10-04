import numpy as np
import pandas as pd
from sklearn.datasets import make_regression
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_classification

col_drop = ["OHX02CTC","OHX03CTC","OHX04CTC","OHX05CTC","OHX06CTC","OHX07CTC","OHX08CTC","OHX09CTC","OHX10CTC","OHX11CTC","OHX12CTC","OHX13CTC","OHX14CTC","OHX15CTC","OHX18CTC","OHX19CTC","OHX20CTC","OHX21CTC","OHX22CTC","OHX23CTC","OHX24CTC","OHX25CTC","OHX26CTC","OHX27CTC","OHX28CTC","OHX29CTC","OHX30CTC","OHX31CTC","CSXTSEQ"]

def acs_dataset(nulls=0.2):
    table = pd.read_csv(r"../../datasets/acs_no_header.csv", header=None)

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
    print(vals.shape)

    return vals, None #pd.DataFrame(vals, columns=table.columns).values, None

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

def cdc_dataset():
    demographic = pd.read_csv(r"datasets/cdc/demographic.csv")
    labs = pd.read_csv(r"datasets/cdc/labs.csv")
    exams = pd.read_csv(r"datasets/cdc/examination.csv")
    exams.drop(col_drop, inplace=True, axis=1)
    df = demographic.merge(labs, left_on="SEQN", right_on="SEQN")
    df = df.merge(exams, left_on="SEQN", right_on="SEQN")

    table = df.values

    table = np.delete(table, np.where(np.isnan(table).all(axis=0))[0], 1)

    scaler = StandardScaler()
    scaler.fit(table)
    vals = scaler.transform(table)
    print("min: ", np.amin(vals), " max: ", np.amax(vals))

    vals = vals[:, :500]

    return vals, None

def synth_dataset(n_items, feats, nan_cols, nulls=0.2):
    X,y = make_regression(n_samples=n_items, n_features=feats, noise=10.0, bias=100.0)
    X *= 1000

    scaler = StandardScaler()
    scaler.fit(X)
    X = scaler.transform(X)
    print("min: ", np.amin(X)," max: ", np.amax(X))

    X_nan = np.copy(X)
    #nan_cols = [0, 2, 4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,46,38,40,42,44,46,48]
    for col in nan_cols:
        rows = np.random.choice(X.shape[0], int(X.shape[0] * nulls), replace=False)
        X_nan[rows, col] = np.nan

    return X_nan, None

def synth_dataset_classification(nan_cols):

    X,y = make_classification(n_samples=100, n_features=8, n_redundant=4, n_informative=4, n_classes=3, )

    np.concatenate([y for x in range(8)], axis=1)
    np.random.choice(range(100), size=None, replace=True, p=None)

    scaler = StandardScaler()
    scaler.fit(X)
    X = scaler.transform(X)
    print("min: ", np.amin(X)," max: ", np.amax(X))

    X_nan = np.copy(X)
    nulls = 0.2
    for col in nan_cols:
        rows = np.random.choice(X.shape[0], int(X.shape[0] * nulls), replace=False)
        X_nan[rows, col] = np.nan

    return X_nan, None
