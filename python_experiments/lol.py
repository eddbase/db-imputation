import pandas as pd
from sklearn import tree
from sklearn.metrics import accuracy_score
import matplotlib.pyplot as plt
import numpy as np

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory
from sklearn.preprocessing import StandardScaler


def load_lol(nulls={'win': 0.2}):

    winner_data = pd.read_csv("../datasets/next_try/lol/match_winner_data_version1.csv")
    loser_data = pd.read_csv("../datasets/next_try/lol/match_loser_data_version1.csv")

    def clean_data(data):
        #Eliminating rows with NaN and null values
        if data.isnull().values.any():
            data.dropna(subset = ["win", "firstBlood", "firstTower", "firstInhibitor",
            "firstBaron", "firstDragon", "firstRiftHerald"], inplace=True)
        return data

    if (winner_data.empty or loser_data.empty):
        print("Problem reading files! Please check files path!")
    else:
        print("Files read successfully!")
        print("Winner dataset shape: ", winner_data.shape)
        print("Loser dataset shape: ", loser_data.shape)

    #Replace "Win" and "Fail" with True and False
    winner_data["win"].replace({"Win": True}, inplace=True)
    loser_data["win"].replace({"Fail": False}, inplace=True)

    #Separate training set
    winner_training_data = winner_data.iloc[:, 2:9] #I'm taking just a part of dataset to train our model
    loser_training_data = loser_data.iloc[:, 2:9]

    #Separate test dataset
    #winner_test_data = winner_data.iloc[50000:108828, 2:9]
    #loser_test_data = loser_data.iloc[50000:108828, 2:9]

    #preparing test data
    #test_data = pd.concat([winner_test_data.iloc[:, 1:7], loser_test_data.iloc[:, 1:7]], ignore_index=True)
    #test_set_labels = pd.concat([winner_test_data["win"], loser_test_data["win"]], ignore_index=True)

    #Preparing training data and define the dependent and independent variables
    #Defining dependent variable
    Y = pd.concat([winner_training_data["win"], loser_training_data["win"]], ignore_index=True)

    #Defining independent variable
    X = pd.concat([winner_training_data.iloc[:, 1:7], loser_training_data.iloc[:, 1:7]], ignore_index=True)

    scaler = StandardScaler()
    #X[X.columns] = scaler.fit_transform(X[X.columns])

    array_sum = np.sum(Y)
    print(np.isnan(array_sum))

    print(X[X.isna()])

    X['win'] = Y.astype(int)

    true_vals = {}

    for k, v in nulls.items():
        print(v)
        ix = np.random.choice(len(X), size=int(v * len(X)), replace=False)
        true_vals[k] = X.loc[ix, k].copy()
        X.loc[ix, k] = np.nan

    return X, true_vals



from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
clf = LinearDiscriminantAnalysis()
X, Y = load_lol()

train = X[~X['win'].isna()]

print(train.drop(['win'], axis=1))
print(train['win'])

clf.fit(train.drop(['win'], axis=1).values, train['win'])
new_matches_test = clf.predict(X[X['win'].isna()].drop(['win'], axis=1))
acc = accuracy_score(Y, new_matches_test)
print(acc)