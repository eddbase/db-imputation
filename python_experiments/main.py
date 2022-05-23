from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import LinearRegression

from bgd_classifier import CustomMICEClassifier
from dataset_utils import inventory_dataset
from sklearn.metrics import mean_squared_error, r2_score, f1_score
import time

from bgd_linear_regression import *
from dataset_utils import acs_dataset, synth_dataset, cdc_dataset


### subtraction
from load_inventory import load_inventory


def generate_full_cofactor(x, numerical_classes, categorical_classes):

    x_1 = np.concatenate([x, np.ones(len(x))[:, None]], axis=1)
    from sklearn.impute import SimpleImputer
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    imp_class = SimpleImputer(missing_values=np.nan, strategy='most_frequent')
    pre_cofactor = x_1.copy()
    pre_cofactor[:, numerical_classes] = imp_mean.fit_transform(pre_cofactor[:, numerical_classes])
    pre_cofactor[:, categorical_classes] = imp_class.fit_transform(pre_cofactor[:, categorical_classes])
    coeff_matrix = np.matmul(pre_cofactor.T, pre_cofactor)

    return coeff_matrix, pre_cofactor, x_1#cofactor, full table imputed, table with nan
#count nonzero


'''
remove cofactor matrix
'''

def removal_appr(x, nan_cols_, materialize_cof_diff, test_bug=False, itr = 5, models = {2:'regression', 4:'regression', 3:'regression', 1:'classification'}):
    classification_cols = []
    numerical_cols = []
    for k, v in models.items():
        if v == 'regression':
            numerical_cols += [k]
        else:
            classification_cols += [k]

    coeff_matrix, pre_cofactor, x_1 = generate_full_cofactor(x, numerical_cols, classification_cols)#cofactor matrix

    classifier = CustomMICEClassifier(pre_cofactor, x_1, classification_cols)

    for iteration in range(itr):
        for col in nan_cols_:
            model = models[col]
            if model == 'regression':
                mice_model = CustomMICERegressor(col)
            else:
                mice_model = classifier
                mice_model.set_current_col(col)

            idx_nan = np.argwhere(np.isnan(x_1[:, col]))[:, 0]
            feats_nan = pre_cofactor[idx_nan, :]#nan rows
            if materialize_cof_diff:
                mk = np.matmul(feats_nan.T, feats_nan)#train over original cofactor - delta
                mice_model.fit(coeff_matrix - mk, pre_cofactor.shape[0] - len(idx_nan), max_epochs=6000, equality_constraint_feats=None, beta=1)#try constraint over first 2 rows feats_nan[2:4, :]] [feats_nan[:2, :], feats_nan[2:4, :], feats_nan[4:6, :]]
            else:
                mice_model.fit(coeff_matrix, pre_cofactor.shape[0] - len(idx_nan), nan_raw_data=feats_nan,
                               max_epochs=6000, equality_constraint_feats=None, beta=0)

            old_vals = np.sum(feats_nan * feats_nan[:, col][:, None], axis=0)
            predictions = mice_model.predict(feats_nan)
            #print("col: ",col,"constraints: ", predictions[:6], " no constraints ", predictions[6:12])

            if model == 'classification':
                print("updating data...")
                mice_model.update_data_struct_1(pre_cofactor, x_1)

            pre_cofactor[idx_nan, col] = predictions #updating dataset preprocessed
            feats_nan = pre_cofactor[idx_nan, :]
            new_vals = np.sum(feats_nan * predictions[:, None], axis=0)#new vals for cofactor
            if model == 'classification':
                print("updating data...")
                mice_model.update_data_struct_2(pre_cofactor, x_1)

            delta = new_vals - old_vals
            coeff_matrix[:, col] += delta
            coeff_matrix[col, :] += delta
            coeff_matrix[col, col] -= delta[col]

            if test_bug:
                rebuild_matrix = np.matmul(pre_cofactor.T, pre_cofactor)
                assert (np.isclose(rebuild_matrix, coeff_matrix).all())
                print("test ok")
        print("iteration completed ", iteration)

    print("done")
    return pre_cofactor

def test_result(data_changed, nan_dataset, true_labels, models = {2:'regression', 4:'regression', 3:'regression', 1:'classification'}):
    for col, model in models.items():
        idx_nan = np.argwhere(np.isnan(nan_dataset.values[:, col]))[:, 0]
        imputed_data = data_changed[idx_nan, col]
        if model == 'regression':
            print('MSE', mean_squared_error(true_labels[col], imputed_data))
            print('R2', r2_score(true_labels[col], imputed_data))
        else:
            print('F1', f1_score(true_labels[col], imputed_data, average='micro'))


models = {2:'regression', 3:'regression', 1:'classification', 0:'classification', 11:'regression', 15:'regression'}
x, labels = load_inventory()#synth_dataset(10000, 6, nan_cols_)
itr = 4 #iterations

nan_cols_ = np.where(np.isnan(x).any(axis=0))[0]

test_bug = False

print("1")#full cofactor - nan cofactor
start_best = time.time()
imputed_data_1 = removal_appr(x, nan_cols_, True, test_bug=test_bug, itr=itr, models=models)#best approach so far
test_result(imputed_data_1, x, labels, models=models)
end_best = time.time()

print("time best: ", end_best-start_best)


start_best = time.time()
#full cofactor - nan cofactor, second approach
imputed_data_2 = removal_appr(x, nan_cols_, False, test_bug=test_bug, itr=itr, models=models)
test_result(imputed_data_2, x, labels, models=models)
end_best = time.time()
print("time second best: ", end_best-start_best)

#standard sklearn
start_best = time.time()
estimator = StandardSKLearnImputation(models=models)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_standard = imputer.fit_transform(x)
test_result(imputed_standard, x, labels, models=models)
end_best = time.time()
print("time sklearn: ", end_best-start_best)

#generates a cofactor matrix for every column
start_best = time.time()
estimator = UnoptimizedMICE(models = models)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_fact_no_opt = imputer.fit_transform(x)
test_result(imputed_fact_no_opt, x, labels, models=models)
end_best = time.time()
print("time unoptimized: ", end_best-start_best)


'''
#multiple cofactor + updates
start_best = time.time()
fact_matrices, len_train_matrices, imputed_matrix = build_matrices(x, nan_cols=nan_cols_)
end_build_matrix=time.time()
results = []


for iteration in range(itr):
    print("iteration: ", iteration)
    for col_idx, matrix in fact_matrices.items():
        regressor = CustomMICERegressor(col_idx)
        print("fitting regressor")
        regressor.fit(fact_matrices[col_idx], len_train_matrices[col_idx])#train
        print("done")
        idx_nan = np.argwhere(np.isnan(x[:, col_idx]))[:, 0]
        feats_nan = imputed_matrix[idx_nan, :]
        predictions = regressor.predict(feats_nan)#impute
        #update
        old_vals = imputed_matrix[idx_nan, col_idx]
        delta_struct = {}

        for col_idx_upd in fact_matrices.keys():
            if col_idx_upd == col_idx:
                continue

            idx_nan_include_delta = np.argwhere(~np.isnan(x[idx_nan, col_idx_upd]))[:,
                                    0]  # idx where col_idx is null and col_idx_upd is not null (compute delta over here) (n. of rows over idx_nan)
            imputed_matrix_include_delta = idx_nan[
                idx_nan_include_delta]  # n. of rows of imputed_matrix to include in delta
            old_vals_n = np.sum(imputed_matrix[imputed_matrix_include_delta, :] * old_vals[idx_nan_include_delta, None], axis=0)
            delta_struct[col_idx_upd] = [old_vals_n, imputed_matrix_include_delta, idx_nan_include_delta]

        imputed_matrix[idx_nan, col_idx] = predictions

        print("updating...")
        # update train matrices
        for col_idx_upd in fact_matrices.keys():
            if col_idx_upd == col_idx:  # do not update current values. Nan rows are removed from train matrix
                continue

            new_vals = np.sum(imputed_matrix[delta_struct[col_idx_upd][1], :] * predictions[delta_struct[col_idx_upd][2], None], axis=0)
            delta = new_vals - delta_struct[col_idx_upd][0]
            ordered_update = delta#[ordered_update]

            column_update = col_idx

            update_matrix = fact_matrices[col_idx_upd]
            update_matrix[column_update, :] += ordered_update  # or col_idx
            update_matrix[:, column_update] += ordered_update
            update_matrix[column_update, column_update] -= ordered_update[column_update]
            fact_matrices[col_idx_upd] = update_matrix

        if test_bug:
            test_matrices, _, _ = build_matrices(x, nan_cols=nan_cols_, start_matrix=imputed_matrix[:, :-1])
            for col_idx_upd in fact_matrices.keys():
                #print("fact_matrices: ", fact_matrices[col_idx_upd])
                #print("test_matrices: ", test_matrices[col_idx_upd])
                #print(fact_matrices[col_idx_upd].shape," , ", test_matrices[col_idx_upd].shape)
                assert (np.isclose(fact_matrices[col_idx_upd], test_matrices[col_idx_upd], atol=1e-6, rtol=0).all())
            print("test ok")

end_best = time.time()
print("time multiple cofactor: ", end_best-start_best, " time build matrix: ", end_build_matrix - start_best)


#######################################

imp_mean = IterativeImputer(estimator=LinearRegression(), skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=3)
imputed_data = imp_mean.fit_transform(x)

imputed_values_original = []
imputed_values_1 = []
imputed_values_2 = []
imputed_values_3 = []
imputed_values_4 = []
imputed_values_5 = []

for col in nan_cols_:
    idx_nan = np.argwhere(np.isnan(x[:, col]))[:, 0]

    imputed_values_original += [imputed_data[idx_nan, col]]
    imputed_values_1 += [imputed_data_1[idx_nan, col]]
    imputed_values_2 += [imputed_data_2[idx_nan, col]]

    imputed_values_3 += [imputed_standard[idx_nan, col]]
    imputed_values_4 += [imputed_fact_no_opt[idx_nan, col]]
    imputed_values_5 += [imputed_matrix[idx_nan, col]]

imputed_values_original = np.concatenate(imputed_values_original, axis=None)
imputed_values_1 = np.concatenate(imputed_values_1, axis=None)
imputed_values_2 = np.concatenate(imputed_values_2, axis=None)
imputed_values_3 = np.concatenate(imputed_values_3, axis=None)
imputed_values_4 = np.concatenate(imputed_values_4, axis=None)
imputed_values_5 = np.concatenate(imputed_values_5, axis=None)


print("MSE:  ", mean_squared_error(imputed_values_original, imputed_values_1))
print("MSE:  ", mean_squared_error(imputed_values_original, imputed_values_2))
print("MSE:  ", mean_squared_error(imputed_values_original, imputed_values_3))
print("MSE:  ", mean_squared_error(imputed_values_original, imputed_values_4))
print("MSE:  ", mean_squared_error(imputed_values_original, imputed_values_5))

print("ISCLOSE: ", np.isclose(imputed_values_original, imputed_values_1).all())
print("ISCLOSE: ", np.isclose(imputed_values_original, imputed_values_2).all())
print("ISCLOSE: ", np.isclose(imputed_values_original, imputed_values_3).all())
print("ISCLOSE: ", np.isclose(imputed_values_original, imputed_values_4).all())
print("ISCLOSE: ", np.isclose(imputed_values_original, imputed_values_5).all())

print(len(imputed_values_original[~np.isclose(imputed_values_original, imputed_values_1)]))
print(imputed_values_original[~np.isclose(imputed_values_original, imputed_values_1)])
print(imputed_values_1[~np.isclose(imputed_values_original, imputed_values_1)])

'''
#########


