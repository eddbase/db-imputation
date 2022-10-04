from bgd_classifier import CustomMICEClassifier
import time
import numpy as np
import math
from optimized_model import CustomMICERegressor

def generate_full_cofactor(x, numerical_classes, categorical_classes, imputation_numeric="mean"):

    x_1 = np.concatenate([x, np.ones(len(x))[:, None]], axis=1)
    from sklearn.impute import SimpleImputer
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    imp_class = SimpleImputer(missing_values=np.nan, strategy='most_frequent')

    #pre_cofactor = x_1#.copy()
    mask_missing_values = np.isnan(x_1)
    if len(numerical_classes) > 0:
        if imputation_numeric != "mean":
            #hot deck imputation
            for c in range(x.shape[1]):
                #nulls = np.isnan(x[:,c])
                if mask_missing_values[:, c].any():
                    np.random.seed(c)
                    x_1[mask_missing_values[:, c], c] = np.random.choice(x_1[~mask_missing_values[:, c], c], mask_missing_values[:, c].sum())
        else:
            x_1[:, numerical_classes] = imp_mean.fit_transform(x_1[:, numerical_classes])
    if len(categorical_classes) > 0:
        x_1[:, categorical_classes] = imp_class.fit_transform(x_1[:, categorical_classes])
    coeff_matrix = np.matmul(x_1.T, x_1)

    return coeff_matrix, x_1, mask_missing_values#cofactor, full table imputed, table with nan
#count nonzero

class OptimizedMICE():
    def __init__(self, models = {2:'regression', 4:'regression', 3:'regression', 1:'classification'}, logger=None, regr_max_iter=1000, regr_max_iter_no_change=12, regr_tol=1e-4):
        self.classification_cols = []
        self.numerical_cols = []

        self.regr_max_iter = regr_max_iter
        self.max_iter_no_change = regr_max_iter_no_change
        self.regr_tol = regr_tol

        for k, v in models.items():
            if v == 'regression':
                self.numerical_cols += [k]
            else:
                self.classification_cols += [k]

        self.logger = logger
        self.statistics = {}

    def _save_imputed_data(self, imputed_dataset, idxs_nan):
        imputed_data = {}
        for k, v in idxs_nan.items():
            imputed_data [k] = imputed_dataset[v, k]
        return imputed_data

    def _add_imputed_data(self, imputed_data, idxs_nan, orig_dataset):
        for k, v in idxs_nan.items():
            orig_dataset [v, k] = imputed_data[k]
        return orig_dataset

    def store_statistics(self, imputed_data):
        for k, v in imputed_data.items():#for each chain
            if k not in self.statistics:
                self.statistics[k] = {}
            for k_1, v_1 in v.items():#for each column
                mean = np.mean(v_1)
                std = np.std(v_1)
                if str(k_1)+'_mean' not in self.statistics[k]:
                    self.statistics[k] = {str(k_1)+'_mean': [mean], str(k_1)+'_std': [std]}
                else:
                    self.statistics[k][str(k_1)+'_mean'] += [mean]
                    self.statistics[k][str(k_1)+'_std'] += [std]

    def compute_rhat(self):
        means = {}
        variance = {}
        n = 0
        for k, v in self.statistics.items():#for every chain
            for k_1, v_1 in v.items():#for every variable and metric
                arr = np.asarray(v_1)
                if k_1 not in means:#list of mean and variance for each attribute. Each element is a chain
                    n = len(arr)
                    means[k_1] = [np.mean(arr)]#vj mean (mean of jth chain for selected attr)
                    variance[k_1] = [np.var(arr, ddof=1)]#sj^2
                else:
                    means[k_1] += [np.mean(arr)]
                    variance[k_1] += [np.var(arr, ddof=1)]

        m = len(self.statistics)
        R_hat = {}
        for k, v in means.items():
            B = np.var(np.asarray(means[k]), ddof=1)#(mean chain j - mean chains)^2
            W = np.mean(np.asarray(variance[k]))#
            R_hat[k] = math.sqrt(((n-1)/n) + (B/W))

        return R_hat

    '''
    implement the proper multiple imputation
    '''
    def mice_chains(self, orig_data, materialize_cof_diff, chains, test_bug=False, itr = 5, sample_distribution = False):
        imputed_data = {}
        classifiers = []
        cof_matrices = []

        idxs_nan = {}
        for x in self.classification_cols + self.numerical_cols:
            idxs_nan[x] = (np.isnan(orig_data[:, x]))

        for chain in range(chains):
            #init every chain
            cof_matrix, imputed_dataset, mask_missing_values = generate_full_cofactor(orig_data, self.numerical_cols,
                                                                               self.classification_cols)  # cofactor matrix
            cof_matrices += [cof_matrix]
            classifiers += [CustomMICEClassifier(imputed_dataset, mask_missing_values, self.classification_cols)]
            #extract imputed values from dataset
            imputed_data[chain] = self._save_imputed_data(imputed_dataset, idxs_nan)

        for iteration in range(itr):
            for chain in range(chains):
                imputed_dataset = self._add_imputed_data(imputed_data[chain], idxs_nan, imputed_dataset)
                cof_matrix, imputed_dataset = self.mice(cof_matrices[chain], imputed_dataset, idxs_nan, classifiers[chain],
                                                        sample_distribution, materialize_cof_diff, test_bug)
                imputed_data[chain] = self._save_imputed_data(imputed_dataset, idxs_nan)
            self.store_statistics(imputed_data)

        R_hat = self.compute_rhat()#check convergence
        for k, v in R_hat.items():
            if v <= 1.11:
                print("col ", k, " converged. R statistic: ", v)
            else:
                print("col ", k, " NOT converged. R statistic: ", v)
        return imputed_data

    #run a single chain
    def single_mice(self, orig_data, materialize_cof_diff, test_bug=False, itr = 5, sample_distribution = False, tol=1e-3):
        #default imputation
        cof_matrix, imputed_dataset, mask_missing_values = generate_full_cofactor(orig_data, self.numerical_cols, self.classification_cols)  # cofactor matrix
        classifier = CustomMICEClassifier(imputed_dataset, mask_missing_values, self.classification_cols)

        #mask_missing_values = np.isnan(null_dataset)

        idxs_nan = {}
        for x in self.classification_cols + self.numerical_cols:
            idxs_nan[x] = mask_missing_values[:, x]

        if not sample_distribution:  # check convergence
            imputed_dataset_prev = imputed_dataset.copy()
            normalized_tol = tol * np.max(np.abs(imputed_dataset[~mask_missing_values]))

        converged = False
        for iteration in range(itr):
            cof_matrix, imputed_dataset = self.mice(cof_matrix, imputed_dataset, idxs_nan, classifier, sample_distribution, materialize_cof_diff, test_bug)

            if not sample_distribution:
                inf_norm = np.linalg.norm(imputed_dataset - imputed_dataset_prev, ord=np.inf, axis=None)
                print("Change: {}, scaled tolerance: {} ".format(inf_norm, normalized_tol))
                if inf_norm < normalized_tol:
                    print("Early stopping criterion reached.")
                    converged = True
                    break
                imputed_dataset_prev = imputed_dataset.copy()

        if not converged and not sample_distribution:
            print("Algorithm not converged!")

        imputed_elements = self._save_imputed_data(imputed_dataset, idxs_nan)
        return imputed_elements

    def mice(self, cof_matrix, imputed_dataset, idxs_nan, classifier, sample_distribution=False, materialize_cof_diff=False, test_bug=False):
        nan_cols_ = np.array(self.classification_cols + self.numerical_cols)
        nan_cols_idx = np.argsort(nan_cols_)
        nan_cols_ = nan_cols_[nan_cols_idx]
        models = np.array(['classification' for _ in range (len(self.classification_cols))] + ['regression' for _ in range (len(self.numerical_cols))])
        models = models[nan_cols_idx]

        for col_idx in range(len(nan_cols_)):
            t_start = time.time()
            model = models[col_idx]
            col = nan_cols_[col_idx]
            if model == 'regression':
                mice_model = CustomMICERegressor(col, sample_dist=sample_distribution, max_epochs=self.regr_max_iter, max_iter_no_change=self.max_iter_no_change, regr_tol=self.regr_tol)
            else:
                mice_model = classifier
                mice_model.set_current_col(col)

            idx_nan = idxs_nan[col]#(np.isnan(null_dataset[:, col]))#np.argwhere(...)[:, 0]
            feats_nan = imputed_dataset[idx_nan, :]#nan rows

            if materialize_cof_diff:
                mk = np.matmul(feats_nan.T, feats_nan)#train over original cofactor - delta
                t_prep = time.time()
                mice_model.fit(cof_matrix - mk, imputed_dataset.shape[0] - idx_nan.sum(), equality_constraint_feats=None, beta=1)#try constraint over first 2 rows feats_nan[2:4, :]] [feats_nan[:2, :], feats_nan[2:4, :], feats_nan[4:6, :]]
            else:
                mice_model.fit(cof_matrix, imputed_dataset.shape[0] - idx_nan.sum(), nan_raw_data=feats_nan,
                                   max_epochs=1000, equality_constraint_feats=None, beta=0)

            old_vals = np.sum(feats_nan * feats_nan[:, col][:, None], axis=0)
            t_pred = time.time()
            predictions = mice_model.predict(feats_nan)
            t_upd = time.time()

            if model == 'classification':
                mice_model.update_data_struct_1(imputed_dataset, idx_nan)

            imputed_dataset[idx_nan, col] = predictions #updating dataset preprocessed
            feats_nan = imputed_dataset[idx_nan, :]
            new_vals = np.sum(feats_nan * predictions[:, None], axis=0)#new vals for cofactor
            if model == 'classification':
                mice_model.update_data_struct_2(imputed_dataset, idx_nan)

            delta = new_vals - old_vals
            cof_matrix[:, col] += delta
            cof_matrix[col, :] += delta
            cof_matrix[col, col] -= delta[col]

            t_end = time.time()
            if self.logger is not None:
                self.logger.log_train(col, t_prep - t_start, 'missinc_cofac')
                self.logger.log_train(col, t_pred - t_prep, 'fit')
                self.logger.log_train(col, t_upd - t_pred, 'predict')
                self.logger.log_train(col, t_end - t_upd, 'update_cofac')

            if test_bug:
                rebuild_matrix = np.matmul(imputed_dataset.T, imputed_dataset)
                assert (np.isclose(rebuild_matrix, cof_matrix).all())
                print("test ok")

        print("done")
        return cof_matrix, imputed_dataset