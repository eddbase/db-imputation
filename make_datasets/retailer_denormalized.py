import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn
from math import pi

NULLS = 0
SYSTEMDS = False
IMPUTEDB = False


def load_flights(path = "retailer/", nulls={'inventoryunits':NULLS, 'maxtemp':NULLS, 'mintemp':NULLS, 'supertargetdistance':NULLS, 'walmartdistance':NULLS, 'snow':NULLS, 'rain':NULLS}, max_rows = None, models ={'inventoryunits':'regression', 'maxtemp':'regression', 'mintemp':'regression', 'supertargetdistance':'regression', 'walmartdistance':'regression'}):#'snow':0.1

    weather = pd.read_csv(path + 'Weather.tbl', sep='|',
                          names=['locn', 'dateid', 'rain', 'snow', 'maxtemp', 'mintemp', 'meanwind', 'thunder'])
    location = pd.read_csv(path + 'Location.tbl', sep='|',
                           names=['locn', 'zip', 'rgn_cd', 'clim_zn_nbr', 'tot_area', 'sell_area', 'avghhi',
                                  'supertargetdistance', 'supertargetdrivetime', 'targetdistance',
                                  'targetdrivetime', 'walmartdistance', 'walmartdrivetime',
                                  'walmartsupercenterdistance', 'walmartsupercenterdrivetime'])
    inventory = pd.read_csv(path + 'Inventory.tbl', sep='|', names=['locn', 'dateid', 'ksn', 'inventoryunits'])
    item = pd.read_csv(path + 'Item.tbl', sep='|',
                       names=['ksn', 'subcategory', 'category', 'categoryCluster', 'prize'])
    census = pd.read_csv(path + 'Census.tbl', sep='|',
                         names=['zip', 'population', 'white', 'asian', 'pacific', 'black', 'mediange',
                                'occupiedhouseunits', 'houseunits', 'families', 'households', 'husbwife', 'males', 'females',
                                'householdschildren', 'hispanic'])

    scaler = StandardScaler()
    weather[['maxtemp', 'mintemp', 'meanwind']] = scaler.fit_transform(weather[['maxtemp', 'mintemp', 'meanwind']])

    numerical = ['rgn_cd', 'clim_zn_nbr', 'tot_area', 'sell_area', 'avghhi', 'supertargetdistance',
                 'supertargetdrivetime', 'targetdistance', 'targetdrivetime', 'walmartdistance', 'walmartdrivetime',
                 'walmartsupercenterdistance', 'walmartsupercenterdrivetime']
    scaler = StandardScaler()
    location[numerical] = scaler.fit_transform(location[numerical])

    scaler = StandardScaler()
    inventory["inventoryunits"] = scaler.fit_transform(inventory["inventoryunits"].values.reshape(-1, 1))

    scaler = StandardScaler()
    item["prize"] = scaler.fit_transform(item["prize"].values.reshape(-1, 1))

    numerical = ['population', 'white', 'asian', 'pacific', 'black', 'mediange', 'occupiedhouseunits', 'houseunits',
                 'husbwife', 'males', 'females', 'householdschildren', 'hispanic','houseunits', 'families']
    scaler = StandardScaler()
    census[numerical] = scaler.fit_transform(census[numerical])

    prize = np.log(np.absolute(np.log(np.absolute(item["prize"]) + 0.01)) + 0.01)
    item["feat_1"] = np.cos(prize) + prize

    if max_rows is not None:
        inventory = inventory[0:max_rows].copy()
    
    # merge
    df = weather.merge(location, on=['locn'])
    df = df.merge(inventory, on=['locn', 'dateid'])

    df = df.merge(item, on=['ksn'])
    df = df.merge(census, on=['zip'])

    result = df

    for col in nulls:
        idx_nan = np.sort(np.random.choice(len(result), int(len(result) * nulls[col]), replace=False))
        result.iloc[idx_nan, result.columns.get_loc(col)] = np.nan

    result["snow"] = result["snow"].astype('Int64')#categorical nulls
    result["rain"] = result["rain"].astype('Int64')
    return result #inventory, weather, location, item, census

result = load_flights()


if IMPUTEDB:
    result = float_fixed_precision(result, 3)
    result.to_csv('join_table_flights.csv', header=True, index=False, na_rep='nan', float_format='%.0f')
else:
    result.to_csv('join_table_flights.csv', header=False, index=False, na_rep='', float_format='%.3f')

if SYSTEMDS:
    cat_numerical = (((result.nunique()<=2).values).astype(int)).reshape(1,-1)
    cat_numerical[0, result.columns.get_loc("subcategory")] = 1
    cat_numerical[0, result.columns.get_loc("category")] = 1
    cat_numerical[0, a.columns.get_loc("categoryCluster")] = 1
    np.savetxt("cat_numerical.csv", cat_numerical, delimiter=",")
