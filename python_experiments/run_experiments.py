from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.linear_model import LinearRegression
import time

from bgd_linear_regression import *
from dataset_utils import acs_dataset, synth_dataset, cdc_dataset


### subtraction
from load_sf_bikeshare import load_sf_bike
from run_employer import load_employee


def generate_full_cofactor(x):

    x_1 = np.concatenate([x, np.ones(len(x))[:, None]], axis=1)
    from sklearn.impute import SimpleImputer
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    pre_cofactor = imp_mean.fit_transform(x_1)
    coeff_matrix = np.matmul(pre_cofactor.T, pre_cofactor)

    return coeff_matrix, pre_cofactor, x_1#cofactor, full table imputed, table with nan
#count nonzero


'''
remove cofactor matrix
'''

def removal_appr(x, nan_cols_, materialize_cof_diff, test_bug=False, itr = 5):
    coeff_matrix, pre_cofactor, x_1 = generate_full_cofactor(x)

    for iteration in range(itr):
        for col in nan_cols_:
            if col%10 == 0:
                print(col)

            mice_model = CustomMICERegressor(col)
            idx_nan = np.argwhere(np.isnan(x_1[:, col]))[:, 0]
            #print("IDX NAN: ", idx_nan)
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
            pre_cofactor[idx_nan, col] = predictions
            feats_nan = pre_cofactor[idx_nan, :]
            #print(idx_nan.shape)
            #print(feats_nan.shape)
            #print(predictions.shape)
            new_vals = np.sum(feats_nan * predictions[:, None], axis=0)

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

x, true_vals = load_employee()#synth_dataset(10000, 6, nan_cols_)
print(x.shape)
itr = 1 #iterations

nan_cols_ = np.where(np.isnan(x).any(axis=0))[0]
print(nan_cols_)

test_bug = False

print("1")#full cofactor - nan cofactor
start_best = time.time()
imputed_data_1 = removal_appr(x, nan_cols_, True, test_bug=test_bug, itr=itr)#best approach so far
end_best = time.time()

print("time best: ", end_best-start_best)


start_best = time.time()
#full cofactor - nan cofactor, second approach
'''
imputed_data_2 = removal_appr(x, nan_cols_, False, test_bug=test_bug, itr=itr)
end_best = time.time()
print("time second best: ", end_best-start_best)
'''

#standard sklearn
start_best = time.time()
estimator = StandardSKLearnImputation()
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_standard = imputer.fit_transform(x)
end_best = time.time()
print("time sklearn: ", end_best-start_best)

#generates a cofactor matrix for every column during every iteration
start_best = time.time()

estimator = UnoptimizedMICE()
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_fact_no_opt = imputer.fit_transform(x)

end_best = time.time()
print("time build cofactor every time: ", end_best-start_best)




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


#########


