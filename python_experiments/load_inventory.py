import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis

def load_inventory(nulls={'maxtemp': 0.2, 'mintemp': 0.2, 'snow':0.2, 'rain':0.2, 'supertargetdistance': 0.2, 'walmartdistance':0.2}):#'snow':0.1

    #inventory = pd.read_csv('inventory.tbl', sep='|', names=['locn', 'dateid', 'ksn', 'inventoryunits'])
    #item = pd.read_csv('item.tbl', sep='|', names=['ksn', 'subcategory', 'category', 'categoryCluster', 'prize'])
    weather = pd.read_csv('../datasets/retailer/weather.tbl', sep='|', names=['locn', 'dateid', 'rain', 'snow', 'maxtemp', 'mintemp', 'meanwind', 'thunder'])
    location = pd.read_csv('../datasets/retailer/location.tbl', sep='|', names=['locn', 'zip', 'rgn_cd', 'clim_zn_nbr', 'tot_area', 'sell_area', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime'])
    #census = pd.read_csv('census.tbl', sep='|', names=['zip', 'population', 'white', 'asian', 'pacific', 'black', 'mediange', 'occupiedhouseunits', 'houseunits', 'family', 'households', 'husbwife', 'males', 'females', 'householdschildren', 'hispanic'])
    #inventory = inventory.drop_duplicates(subset=['locn'])#CHANGE HERE IF COL IS CHANGED

    df = weather.merge(location, on=['locn'])
    #df = df.merge(item, on=['ksn'])
    #df = df.merge(location, on=['locn'])
    #df = df.merge(census, on=['zip'])
    #df.head()

    result = df

    result.drop(['zip', 'dateid', 'locn'], axis=1, inplace=True)

    print("normalizing...")
    numerical = ['maxtemp', 'mintemp', 'meanwind', 'tot_area', 'sell_area', 'avghhi', 'supertargetdistance', 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime', 'walmartsupercenterdistance', 'walmartsupercenterdrivetime']

    scaler = StandardScaler()
    result[numerical] = scaler.fit_transform(result[numerical])

    #X_train, X_test, y_train, y_test = train_test_split(result.drop([col], axis=1), result[col], test_size=0.2, random_state=1)

    true_vals = {}

    for k, v in nulls.items():
        print(k)
        ix = np.sort(np.random.choice(len(result), size=int(v * len(result)), replace=False))
        true_vals[result.columns.get_loc(k)] = result.loc[ix, k].copy()
        result.loc[ix, k] = np.nan

    return result, true_vals

'''
model = LinearRegression(normalize=True)
#model = LinearDiscriminantAnalysis()


print('training...')
model = model.fit(X_train, y_train)
print('done')

y_predicted = model.predict(X_test)
fig, ax = plt.subplots()
ax.scatter(y_predicted, y_test, edgecolors=(0, 0, 1))
ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=3)
ax.set_xlabel('Predicted')
ax.set_ylabel('Actual')


mse_test = mean_squared_error(y_test, y_predicted)
print(mse_test)
print('SCORE ', model.score(X_test, y_test))
'''