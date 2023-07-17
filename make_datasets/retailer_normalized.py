import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn
from math import pi

NULLS = 0

def load_retailer(path = "retailer/", nulls={'inventoryunits':NULLS}, max_rows = None):#'snow':0.1

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
                 'husbwife', 'males', 'females', 'householdschildren', 'hispanic','houseunits', 'families', 'households']
    scaler = StandardScaler()
    census[numerical] = scaler.fit_transform(census[numerical])

    prize = np.log(np.absolute(np.log(np.absolute(item["prize"]) + 0.01)) + 0.01)
    item["feat_1"] = np.cos(prize) + prize

    if max_rows is not None:
        inventory = inventory[0:max_rows].copy()
    
    idx_nan = np.sort(np.random.choice(len(inventory), int(len(inventory) * nulls['inventoryunits']), replace=False))
    inventory.iloc[idx_nan, inventory.columns.get_loc('inventoryunits')] = np.nan
    
    inventory = inventory.reset_index(drop=True).reset_index(drop=False)
    return inventory, weather, location, item, census

inventory, weather, location, item, census = load_retailer()

inventory.to_csv('inventory_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
weather.to_csv('weather_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
location.to_csv('location_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
item.to_csv('item_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
census.to_csv('census_dataset.csv', header=False, index=False, na_rep='', float_format='%.3f')
#result.to_csv('join_table.csv', header=False, index=False, na_rep='', float_format='%.3f')
