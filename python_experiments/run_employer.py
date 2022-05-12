import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.tree import DecisionTreeRegressor


def generate_new_salary():
    df1 = pd.read_csv('../datasets/employee/employee_salaries.csv')
    df4 = pd.read_csv('../datasets/employee/employee_dept_emp.csv')
    df4 = df4.rename(columns={"from_date": "dept_from_date", "to_date": "dept_to_date"})
    df1 = df1.rename(columns={"from_date": "salary_from_date", "to_date": "salary_to_date"})

    result = pd.merge(df1, df4, on="emp_no")
    result['keep'] = (result['salary_from_date'] >= result['dept_from_date'])
    pd.set_option('display.max_columns', None)


def main():
    df1 = pd.read_csv('../datasets/employee/employee_salaries.csv')
    df2 = pd.read_csv('../datasets/employee/employee_titles.csv')
    df3 = pd.read_csv('../datasets/employee/employee_employees.csv')
    df4 = pd.read_csv('../datasets/employee/employee_dept_emp.csv')

    df4 = df4.rename(columns={"from_date": "dept_from_date", "to_date": "dept_to_date"})
    df3 = df3[['emp_no', 'birth_date', 'gender', 'hire_date']]
    df2 = df2.rename(columns={"from_date": "title_from_date", "to_date": "title_to_date"})
    df1 = df1.rename(columns={"from_date": "salary_from_date", "to_date": "salary_to_date"})

    #repair_dataset (start time can't be 9999 as year)
    '''
    df1['prev_salary_to'] = df1['salary_to_date'].shift()
    df2['prev_title_to'] = df2['title_to_date'].shift()

    mask = df1['salary_from_date'].str.startswith('9999')
    #df1.loc[mask, ['salary_from_date']] = df1.loc[mask, ['prev_salary_to']]
    mask = df2['title_from_date'].str.startswith('9999')
    #df2.loc[mask, ['title_from_date']] = df2.loc[mask, ['prev_title_to']]

    df1.drop(['prev_salary_to'], axis=1, inplace=True)
    df2.drop(['prev_title_to'], axis=1, inplace=True)
    '''

    #convert dates

    df1['salary_from_date'] = pd.to_datetime(df1['salary_from_date'], format='%Y-%m-%d')
    df3['hire_date'] = pd.to_datetime(df3['hire_date'], format='%Y-%m-%d')
    df3['birth_date'] = pd.to_datetime(df3['birth_date'], format='%Y-%m-%d')
    df4['dept_from_date'] = pd.to_datetime(df4['dept_from_date'], format='%Y-%m-%d')
    df2['title_from_date'] = pd.to_datetime(df2['title_from_date'], format='%Y-%m-%d')

    print('table sizes: ', len(df1), len(df2), len(df3), len(df4))

    #join
    result = pd.merge(df1, df2, on="emp_no")
    result = pd.merge(result, df3, on="emp_no")
    result = pd.merge(result, df4, on="emp_no")

    #cleanup join
    result['department_keep'] = (result['salary_from_date'] >= result['dept_from_date'])#keep record if department is relevant for salary (salary started in that department)
    result['title_keep'] = (result['salary_from_date'] >= result['title_from_date'])#keep record if title is relevant for salary (title started earlier than salary)
    print('full join ',len(result))
    result = result[(result['department_keep'] == True) & (result['title_keep'] == True)]
    result.drop(['title_keep', 'department_keep'], axis=1, inplace=True)
    print('final join size: ', len(result))

    #########
    #timespan

    result['time_in_company'] = (result['salary_from_date'] - result['hire_date']).dt.days//365
    result['age'] = (result['salary_from_date'] - result['birth_date']).dt.days//365
    result['time_with_title'] = (result['salary_from_date'] - result['title_from_date']).dt.days//365
    result['time_in_dept'] = (result['salary_from_date'] - result['dept_from_date']).dt.days//365

    result['year'] = (result['salary_from_date']).dt.year


    result.drop(['salary_to_date', 'salary_from_date', 'title_from_date', 'title_to_date', 'birth_date', 'hire_date', 'dept_from_date', 'dept_to_date'], axis=1, inplace=True)

    #one-hot of department, gender and title
    gender = pd.get_dummies(result.gender, prefix='gender')
    department = pd.get_dummies(result.dept_no, prefix='dept')
    title = pd.get_dummies(result.title, prefix='title')
    result = pd.concat([result, gender, department, title], axis=1)
    result.drop(['gender', 'dept_no', 'title'], axis=1, inplace=True)
    #remove last 5%
    result = result[result.salary <= 118560]
    result = result[result.salary >= 40000]

    scaler = StandardScaler()
    result[result.columns] = scaler.fit_transform(result[result.columns])

    return result

def plot_corr(result):
    f = plt.figure(figsize=(19, 15))
    plt.matshow(result.corr(), fignum=f.number)
    plt.xticks(range(result.select_dtypes(['number']).shape[1]), result.select_dtypes(['number']).columns, fontsize=14,
               rotation=45)
    plt.yticks(range(result.select_dtypes(['number']).shape[1]), result.select_dtypes(['number']).columns, fontsize=14)
    cb = plt.colorbar()
    cb.ax.tick_params(labelsize=14)
    plt.title('Correlation Matrix', fontsize=16)
    plt.show()

    pd.set_option('display.max_columns', None)

def load_employee(nulls={'salary': 0.2, 'time_in_company':0.2}):
    df = main()
    df = df.iloc[:500000]
    df.drop(['emp_no'], axis=1, inplace=True)

    true_vals = {}

    for k, v in nulls.items():
        ix = np.random.choice(df.index, size=int(v * len(df)), replace=False)
        true_vals[k] = df.loc[ix, k].copy()
        df.loc[ix, k] = np.nan

    return df, true_vals


'''
pd.set_option('display.max_columns', None)
print(df.head(100))
plot_corr(df)


from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

#scaler = StandardScaler()
#scaler.fit_transform(df)

X_train, X_test, y_train, y_test = train_test_split(df.drop(['salary'], axis=1), df['salary'], test_size=0.1, random_state=1)
#model = DecisionTreeRegressor()
model = LinearRegression(normalize=True)
#model = Ridge(alpha=1.0)

print('training...')
model = model.fit(X_train, y_train)
print('done')
y_predicted = model.predict(X_train)
fig, ax = plt.subplots()
ax.scatter(y_predicted, y_train, edgecolors=(0, 0, 1))
ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=3)
ax.set_xlabel('Predicted')
ax.set_ylabel('Actual')

plt.show()


def scoring(clf):
    scores = cross_val_score(clf, X_train, y_train, cv=15, n_jobs=1, scoring = 'neg_mean_squared_error')
    print (np.mean(scores) * -1)
rfr = LinearRegression(normalize=True)
scoring(rfr)
'''