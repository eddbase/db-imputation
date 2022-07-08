import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import pandas as pd
import datetime as dt
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

#2: 'regression', 3:'classifier', 4:'classifier',6:'classifier', 7:'regression'

#'store_nbr', 'item_nbr', 'unit_sales', 'onpromotion', 'cluster', 'class', 'perishable', 'transactions'

def load_favorita(path = "../datasets/favorita/", nulls={'unit_sales':0.1, 'transactions':0.1, 'perishable':0.1, 'onpromotion':0.1}, max_rows = None, models = {'unit_sales':'regression', 'transactions':'regression', 'perishable':'classification', 'onpromotion':'classification'}):#'snow':0.1

    items = pd.read_csv(path+"items.csv")
    holiday_events = pd.read_csv(path+"holidays_events.csv", parse_dates=['date'])
    stores = pd.read_csv(path+"stores.csv")
    oil = pd.read_csv(path+"oil.csv", parse_dates=['date'])  # parse_dates=['date']
    transactions = pd.read_csv(path+"transactions.csv", parse_dates=['date'])
    # the full training data's output: "125,497,040 rows | 6 columns"
    # 5% of the data
    # train = pd.read_csv("train.csv", nrows=6000000  , parse_dates=['date'])
    train_large = pd.read_csv(path+'train.csv', nrows=307041, parse_dates=['date'])

    X = [train_large, stores, oil, items, transactions, holiday_events]

    print('size before merging: ', len(X[0]))
    train_stores = X[0].merge(X[1], right_on='store_nbr', left_on='store_nbr', how='left')
    print(len(train_stores))
    train_stores_oil = train_stores.merge(X[2], right_on='date', left_on='date', how='left')
    print(len(train_stores_oil))
    train_stores_oil_items = train_stores_oil.merge(X[3], right_on='item_nbr', left_on='item_nbr', how='left')
    print(len(train_stores_oil_items))
    train_stores_oil_items_transactions = train_stores_oil_items.merge(X[4], right_on=['date', 'store_nbr'],
                                                                       left_on=['date', 'store_nbr'], how='left')
    print(len(train_stores_oil_items_transactions))
    train_stores_oil_items_transactions_hol = train_stores_oil_items_transactions.merge(X[5], right_on='date',
                                                                                        left_on='date', how='left')
    print(train_stores_oil_items_transactions_hol.columns)
    # end merging - handle missing onpromotion
    train_stores_oil_items_transactions_hol['onpromotion'] = train_stores_oil_items_transactions_hol[
        'onpromotion'].fillna("Not Mentioned")
    train_stores_oil_items_transactions_hol.loc[
        train_stores_oil_items_transactions_hol["onpromotion"] == True, "onpromotion"] = "Yes"
    train_stores_oil_items_transactions_hol.loc[
        train_stores_oil_items_transactions_hol["onpromotion"] == False, "onpromotion"] = "No"
    print('size after merging: ', len(train_stores_oil_items_transactions_hol))
    data_df = train_stores_oil_items_transactions_hol  # .copy(deep = True)
    del train_stores_oil_items_transactions_hol
    data_df = data_df.dropna(axis=1)
    # data_df['onpromotion'] = data_df['onpromotion'].astype(int)
    data_df.rename(columns={'type_x': 'st_type', 'type_y': 'hol_type'}, inplace=True)
    data_df.drop(['id'], axis=1, inplace=True)
    print(data_df.head())
    data_df['date'] = pd.to_datetime(data_df['date'])
    data_df['date'] = data_df['date'].map(dt.datetime.toordinal)
    data_df = data_df.dropna(axis=1)
    print(data_df.head())
    print('LEN ', len(data_df))
    print('COLS: ', data_df.columns)

    X = data_df
    df_ = X.drop(['date'], axis=1)
    cols = df_.columns
    num_cols = df_._get_numeric_data().columns
    cat_cols = list(set(cols) - set(num_cols))

    data_num_df = X[num_cols]
    data_cat_df = X[cat_cols]
    data_date_df = X['date']

    X = [data_num_df, data_cat_df, data_date_df]

    imputer = SimpleImputer(strategy="mean", copy="true")
    num_imp = imputer.fit_transform(X[0])
    data_num_df = pd.DataFrame(num_imp, columns=X[0].columns, index=X[0].index)

    scaler = StandardScaler()
    scaler.fit(data_num_df)
    num_scaled = scaler.transform(data_num_df)
    data_num_df = pd.DataFrame(num_scaled, columns=X[0].columns, index=X[0].index)

    cat_encoder = OneHotEncoder(sparse=False)
    data_cat_1hot = cat_encoder.fit_transform(X[1])

    # convert it to datafram with n*99 where n number of rows and 99 is no. of categories
    data_cat_df = pd.DataFrame(data_cat_1hot, columns=cat_encoder.get_feature_names())  # , index=X[1].index)

    print('Size after 1-hot encoding: ', len(data_cat_df), ' rows. Cols: ', len(data_cat_df.columns))

    X = [data_num_df, data_cat_df, X[2]]

    data_df = X[0].join(X[1])
    data_df = data_df.join(X[2])

    # split it according to our feature engineering
    #X = data_df.drop(['unit_sales'], axis=1)
    #Y = data_df[['unit_sales']]

    true_vals = {}
    cols_models = {}

    for k, v in nulls.items():
        print(k)
        index_no = data_df.columns.get_loc(k)
        ix = np.sort(np.random.choice(len(data_df), size=int(v * len(data_df)), replace=False))
        true_vals[index_no] = data_df.loc[ix, k].copy()
        data_df.loc[ix, k] = np.nan
        cols_models[index_no] = models[k]


    return data_df, true_vals, cols_models

    #unit_sales, transactions
    #perishable, onpromotion, cluster