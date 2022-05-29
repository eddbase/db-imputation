import datetime as dt
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
import pandas as pd
import numpy as np

#2: 'regression', 3:'classifier', 4:'classifier',6:'classifier', 7:'regression'

#'store_nbr', 'item_nbr', 'unit_sales', 'onpromotion', 'cluster', 'class', 'perishable', 'transactions'

def load_favorita(nulls={'unit_sales':0.1, 'transactions':0.1, 'perishable':0.1, 'onpromotion':0.1}, max_rows = None, models = {'unit_sales':'regression', 'transactions':'regression', 'perishable':'classification', 'onpromotion':'classification'}):#'snow':0.1

    items = pd.read_csv("../../datasets/favorita/items.csv")
    holiday_events = pd.read_csv("../../datasets/favorita/holidays_events.csv", parse_dates=['date'])
    stores = pd.read_csv("../../datasets/favorita/stores.csv")
    oil = pd.read_csv("../../datasets/favorita/oil.csv", parse_dates=['date'])
    transactions = pd.read_csv("../../datasets/favorita/transactions.csv", parse_dates=['date'])
    # the full training data's output: "125,497,040 rows | 6 columns"
    # 5% of the data
    train = pd.read_csv("../../datasets/favorita/train.csv", nrows=6000000, parse_dates=['date'])
    if max_rows is not None:
        train_large = pd.read_csv('../../datasets/favorita/train.csv', nrows = max_rows, names = train.columns, parse_dates = ['date'])
    else:
        train_large = pd.read_csv('../../datasets/favorita/train.csv', names = train.columns, parse_dates = ['date'])

    len(train_large)

    class prepare_data(BaseEstimator, TransformerMixin):
        def __init__(self):
            print("prepare_data -> init")

        def fit(self, X, y=None):
            return self

        def transform(self, X, y=None):
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
            # train_stores_oil_items_transactions_hol['day_type'] = train_stores_oil_items_transactions_hol['day_type'].fillna("Work Day")
            # train_stores_oil_items_transactions_hol['oil_price'] = train_stores_oil_items_transactions_hol["oil_price"].fillna(axis = 0,method = 'ffill')
            train_stores_oil_items_transactions_hol['onpromotion'] = train_stores_oil_items_transactions_hol[
                'onpromotion'].fillna("Not Mentioned")

            print('size after merging: ', len(train_stores_oil_items_transactions_hol))

            data_df = train_stores_oil_items_transactions_hol.copy(deep=True)

            # change the bool to int

            data_df = data_df.dropna(axis=1)

            data_df['onpromotion'] = data_df['onpromotion'].astype(int)
            # data_df['transferred'] = data_df['transferred'].astype(int)

            # change the names
            data_df.rename(columns={'type_x': 'st_type', 'type_y': 'hol_type'}, inplace=True)

            # drop the id
            data_df.drop(['id'], axis=1, inplace=True)

            print(data_df.head())

            # handle date
            data_df['date'] = pd.to_datetime(data_df['date'])
            data_df['date'] = data_df['date'].map(dt.datetime.toordinal)

            data_df = data_df.dropna(axis=1)

            print(data_df.head())
            print('LEN ', len(data_df))
            print('COLS: ', data_df.columns)

            return data_df


    # split dataframe into numerical values, categorical values and date
    class split_data(BaseEstimator, TransformerMixin):
        def __init__(self):
            print("split_data -> init")

        def fit(self, X, y=None):
            return self

        def transform(self, X, y=None):
            # Get columns for each type
            df_ = X.drop(['date'], axis=1)
            cols = df_.columns
            num_cols = df_._get_numeric_data().columns

            #num_cols = set(num_cols)
            #num_cols.remove('onpromotion')
            #num_cols.remove('perishable')
            #num_cols = list(num_cols)
            print("nn ", num_cols)

            print("check ", pd.unique(df_["onpromotion"]))
            print("check ", pd.unique(df_["cluster"]))
            print("check ", pd.unique(df_["perishable"]))

            cat_cols = list(set(cols) - set(num_cols))

            data_num_df = X[num_cols]
            data_cat_df = X[cat_cols]
            data_date_df = X['date']

            return data_num_df, data_cat_df, data_date_df


    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.preprocessing import StandardScaler


    class process_data(BaseEstimator, TransformerMixin):
        def __init__(self):
            print("process_data -> init")

        def fit(self, X, y=None):
            return self

        def transform(self, X, y=None):
            ### numerical data
            # impute nulls in numerical attributes

            imputer = SimpleImputer(strategy="mean", copy="true")
            num_imp = imputer.fit_transform(X[0])
            print('numerical cols: ', X[0].columns)
            data_num_df = pd.DataFrame(num_imp, columns=X[0].columns, index=X[0].index)

            l_imputed = set(X[0].columns)
            l_imputed.remove('onpromotion')
            l_imputed.remove('perishable')
            l_imputed = list(l_imputed)

            # apply standard scaling
            scaler = StandardScaler()
            scaler.fit(data_num_df[l_imputed])

            print('standardize: ', l_imputed)

            data_num_df[l_imputed] = scaler.transform(data_num_df[l_imputed])
            data_num_df = pd.DataFrame(data_num_df, columns=X[0].columns, index=X[0].index)


            ### categorical data
            # one hot encoder
            cat_encoder = OneHotEncoder(sparse=False)
            data_cat_1hot = cat_encoder.fit_transform(X[1])

            # convert it to datafram with n*99 where n number of rows and 99 is no. of categories
            data_cat_df = pd.DataFrame(data_cat_1hot, columns=cat_encoder.get_feature_names())  # , index=X[1].index)

            print('Size after 1-hot encoding: ', len(data_cat_df), ' rows. Cols: ', len(data_cat_df.columns))

            return data_num_df, data_cat_df, X[2]


    class join_df(BaseEstimator, TransformerMixin):
        def __init__(self):
            print("join_df -> init")

        def fit(self, X, y=None):
            return self

        def transform(self, X, y=None):
            ### numerical data
            print("merging...")
            data_df = X[0].join(X[1])
            data_df = data_df.join(X[2])

            return data_df

    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler

    pipe_processing = Pipeline([
            ('prepare_data', prepare_data()),
            ('split_data', split_data()),
            ('process_data', process_data()),
            ('join_data', join_df())
        ])

    # our prepared data
    data_df = pipe_processing.fit_transform([train_large, stores, oil, items, transactions, holiday_events])
    print(data_df.columns)
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