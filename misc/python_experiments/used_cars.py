import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from feature_engine.encoding import RareLabelEncoder,OrdinalEncoder,OneHotEncoder



data=pd.read_csv("../datasets/next_try/vehicles.csv")
df=data.copy()
df.head()

df=df.drop(columns=["id","url","region_url","image_url","description","VIN","county"],axis=1)
df.head()

df=df.dropna()
df.shape

df.odometer=df.odometer.astype(int)
df.year=df.year.astype(int)
df.dtypes

df=df.drop(df[df["price"]==0].index)

df.drop(df[(df.price<500 )|( df.price>28000)].index)["price"].describe()

df_cleaned=df.copy()
df_cleaned=df.drop(df[(df.price<500 )|( df.price>28000)].index)
df_cleaned.select_dtypes(include="object").columns[1:]
#nominal.associations(df_cleaned,figsize=(20,10),mark_columns=True); # for nominal and categorical (Cramer's V)
df_cleaned=df_cleaned.drop(columns=["lat","long","model"],axis=1)
df_cleaned.head()
df_cleaned=df_cleaned.drop(columns=["region"],axis=1)
def find_skewed_boundaries(df, variable, distance):

    IQR = df[variable].quantile(0.75) - df[variable].quantile(0.25)
    #stats.iqr(df[variable])
    print("IQR Value :",IQR)
    lower_boundary = df[variable].quantile(0.25) - (IQR * distance)
    upper_boundary = df[variable].quantile(0.75) + (IQR * distance)

    return upper_boundary, lower_boundary



find_skewed_boundaries(df_cleaned,"price",1.5)

upper_odo,lower_odo=find_skewed_boundaries(df_cleaned,"odometer",1.5)
upper_odo,lower_odo
df_cleaned[(df_cleaned.odometer>upper_odo)].shape,df_cleaned[~(df_cleaned.odometer>upper_odo)].shape
df_cleaned[df_cleaned.odometer<0]
df_cleaned=df_cleaned[~(df_cleaned.odometer>upper_odo)]
df_cleaned.head()

X=df_cleaned.drop(columns=["price"])
y=df_cleaned["price"]
X.head()

X_train,X_test,y_train,y_test=train_test_split(X,y,
                                               test_size=0.20,
                                                random_state=42)

multi_cat_cols = []

for col in X_train.columns:

    if X_train[col].dtypes == 'O':  # if variable  is categorical

        if X_train[col].nunique() > 10:  # and has more than 10 categories

            multi_cat_cols.append(col)  # add to the list

            print(X_train.groupby(col)[col].count() / len(
                X_train))  # and print the percentage of observations within each category

            print()

X_train.manufacturer.value_counts().index[-10:]
X_train = X_train.copy()
print(X_train.shape)
X_train["year"]=2020-X_train["year"]
X_test["year"]=2020-X_test["year"]
X_train.head()

rare_encoder = RareLabelEncoder(
    tol=0.0125,  # minimal percentage to be considered non-rare
    n_categories=10, # minimal number of categories the variable should have to re-cgroup rare categories
    variables=["manufacturer","state","title_status"] # variables to re-group
)

rare_encoder.fit(X_train)

X_train = rare_encoder.transform(X_train)
X_test = rare_encoder.transform(X_test)

rare_encoder = RareLabelEncoder(
    tol=0.05,  # minimal percentage to be considered non-rare
    n_categories=3, # minimal number of categories the variable should have to re-cgroup rare categories
    variables=["title_status"],
    replace_with='NotClean' # variables to re-group
)

rare_encoder.fit(X_train)
X_train = rare_encoder.transform(X_train)
X_test = rare_encoder.transform(X_test)

X_train.condition=X_train.condition.replace({"salvage":1,"new":2,"fair":3,"like new":4,"good":5,"excellent":6})
X_test.condition=X_test.condition.replace({"salvage":1,"new":2,"fair":3,"like new":4,"good":5,"excellent":6})


X_train_encoded=pd.get_dummies(X_train,drop_first=True)
X_test_encoded=pd.get_dummies(X_test,drop_first=True)
X_train_encoded.head()

models = []

models.append(LinearRegression())

r2_values_test = []
r2_values_train = []
rmse_values_test = []
mse_values_test = []
for model in models:
    model_ = model.fit(X_train_encoded, y_train)
    y_pred = model_.predict(X_test_encoded)

    r2_train = model_.score(X_train_encoded, y_train)
    r2_values_train.append(r2_train)

    r2 = model_.score(X_test_encoded, y_test)
    r2_values_test.append(r2)

    rmse_test = np.sqrt(mean_squared_error(y_test, y_pred))
    rmse_values_test.append(rmse_test)

    mse_test = mean_squared_error(y_test, y_pred)
    mse_values_test.append(mse_test)

result = pd.DataFrame(list(zip(r2_values_test, r2_values_train)), columns=["r2_score_test", "r2_score_train"])
result["rmse_test"] = rmse_values_test
result["mse_test"] = mse_values_test
result["model"] = ["XGBoost", "LGBM", "RF", "ExtraTree", "HGBoost"]
print(result)