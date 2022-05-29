from bgd_classifier import CustomMICEClassifier
import time
import numpy as np

from optimized_model import CustomMICERegressor

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



def mice(x, nan_cols_, materialize_cof_diff, test_bug=False, itr = 5, models = {2:'regression', 4:'regression', 3:'regression', 1:'classification'}, logger=None, sample_distribution = False, tol=1e-3):
    classification_cols = []
    numerical_cols = []
    for k, v in models.items():
        if v == 'regression':
            numerical_cols += [k]
        else:
            classification_cols += [k]

    cof_matrix, imputed_dataset, null_dataset = generate_full_cofactor(x, numerical_cols, classification_cols)#cofactor matrix

    classifier = CustomMICEClassifier(imputed_dataset, null_dataset, classification_cols)

    mask_missing_values = np.isnan(null_dataset)

    if not sample_distribution:  # check convergence
        imputed_dataset_prev = imputed_dataset.copy()
        normalized_tol = tol * np.max(np.abs(null_dataset[~mask_missing_values]))

    for iteration in range(itr):
        for col in nan_cols_:
            t_start = time.time()

            model = models[col]
            if model == 'regression':
                mice_model = CustomMICERegressor(col, sample_dist=sample_distribution)
            else:
                mice_model = classifier
                mice_model.set_current_col(col)

            idx_nan = np.argwhere(np.isnan(null_dataset[:, col]))[:, 0]
            feats_nan = imputed_dataset[idx_nan, :]#nan rows

            #if model != 'regression':
                #clf = LinearDiscriminantAnalysis(solver="lsqr", store_covariance=True, shrinkage=None)
                #row_idx = np.argwhere(~np.isnan(x_1[:, col]))[:, 0]
                #print(row_idx.shape)
                #print(pre_cofactor.shape)
                #print(np.r_[0:col, col+1:coeff_matrix.shape[1]])
                #test = pre_cofactor[row_idx, :]
                #test = test[:, np.r_[0:col, col+1:coeff_matrix.shape[1]]]
                #clf.fit(test, pre_cofactor[row_idx, col])
                #print(clf.covariance_)
                #print("inverting sklearn")
                #tt = np.linalg.inv(clf.covariance_)

            if materialize_cof_diff:
                mk = np.matmul(feats_nan.T, feats_nan)#train over original cofactor - delta
                mice_model.fit(cof_matrix - mk, imputed_dataset.shape[0] - len(idx_nan), max_epochs=6000, equality_constraint_feats=None, beta=1)#try constraint over first 2 rows feats_nan[2:4, :]] [feats_nan[:2, :], feats_nan[2:4, :], feats_nan[4:6, :]]
            else:
                mice_model.fit(cof_matrix, imputed_dataset.shape[0] - len(idx_nan), nan_raw_data=feats_nan,
                               max_epochs=6000, equality_constraint_feats=None, beta=0)

            old_vals = np.sum(feats_nan * feats_nan[:, col][:, None], axis=0)
            predictions = mice_model.predict(feats_nan)
            #print("col: ",col,"constraints: ", predictions[:6], " no constraints ", predictions[6:12])

            if model == 'classification':
                print("updating data...")
                mice_model.update_data_struct_1(imputed_dataset, null_dataset)

            imputed_dataset[idx_nan, col] = predictions #updating dataset preprocessed
            feats_nan = imputed_dataset[idx_nan, :]
            new_vals = np.sum(feats_nan * predictions[:, None], axis=0)#new vals for cofactor
            if model == 'classification':
                print("updating data...")
                mice_model.update_data_struct_2(imputed_dataset, null_dataset)

            delta = new_vals - old_vals
            cof_matrix[:, col] += delta
            cof_matrix[col, :] += delta
            cof_matrix[col, col] -= delta[col]

            t_end = time.time()
            logger.log_train(iteration, col, t_end - t_start)

            if test_bug:
                rebuild_matrix = np.matmul(imputed_dataset.T, imputed_dataset)
                assert (np.isclose(rebuild_matrix, cof_matrix).all())
                print("test ok")

        if not sample_distribution:
            inf_norm = np.linalg.norm(imputed_dataset - imputed_dataset_prev, ord=np.inf, axis=None)
            print("Change: {}, scaled tolerance: {} ".format(inf_norm, normalized_tol))
            if inf_norm < normalized_tol:
                print("Early stopping criterion reached.")
                break
            imputed_dataset_prev = imputed_dataset.copy()
        print("iteration completed ", iteration)

    print("done")
    return imputed_dataset