from sklearn.base import BaseEstimator, RegressorMixin
import numpy as np
import math
import scipy

class CustomMICEClassifier(BaseEstimator, RegressorMixin):
    def __init__(self, preproc_dataset, original_dataset, nan_cols_):#index label
        self.sums = {}
        self.items = {}
        self.tot_items = {}
        self.nan_cols_ = nan_cols_

        for col in nan_cols_:
            #print(col)
            self.sums[col] = {}
            self.items[col] = {}

            idx_nan = np.argwhere(~np.isnan(original_dataset[:, col]))[:, 0]
            feats_not_nan = preproc_dataset[idx_nan, :]  # non nan rows
            classes = np.unique(feats_not_nan[:, col])

            #print('check ', classes)

            for i in classes:  # for each class
                tmp = feats_not_nan[np.where(feats_not_nan[:, col] == i)]
                self.sums[col][i] = tmp.sum(axis=0)
                self.items[col][i] = len(tmp)
            self.tot_items[col] = feats_not_nan.shape[0]
            #print('tot items col: ', self.tot_items[col])

    '''
    last col of coeff_matrix MUST be of 1s
    '''
    def fit(self, coeff_matrix, len_original_data, original_data=None, y=None, max_epochs=6000, tol=1e-6,
            max_iter_no_change=20, nan_raw_data = None, equality_constraint_feats=None, beta=0):

        self.covariance = np.copy(coeff_matrix)
        self.means = {}

        #build covariance
        for i in self.items[self.current_col].keys():#for each class
            #print('col: ', self.current_col, 'i ', i, self.sums[self.current_col], self.items[self.current_col])
            self.means[i] = self.sums[self.current_col][i]/self.items[self.current_col][i]#already removed nans for each class
            self.covariance += (self.items[self.current_col][i] * np.matmul(self.means[i][None].T, self.means[i][None]))
            product = np.matmul(self.means[i][None].T, self.sums[self.current_col][i][None])
            self.covariance -= product
            self.covariance -= product.T

        self.covariance /= len_original_data
        return self

    def predict(self, X):
        col_index = np.r_[0:self.current_col, self.current_col+1:X.shape[1]]
        feat_impute = X[:, col_index]#filter label
        covariance_impute = self.covariance[col_index,:]
        covariance_impute = covariance_impute[:,col_index]
        inv_matrix = np.linalg.inv(covariance_impute)

        imputed_probs = []
        classes = []
        for k, v in self.means.items():#k = class, v=mean
            classes += [k]
            curr_means = self.means[k][col_index].reshape(-1,1)
            sec_part = (1/2)*np.matmul(np.matmul(curr_means.T, inv_matrix), curr_means)
            third_part = np.matmul(np.matmul(feat_impute, inv_matrix), curr_means)
            #print('shapes ', sec_part.shape, third_part.shape)
            imputed_probs += [math.log(self.items[self.current_col][k] / self.tot_items[self.current_col]) - sec_part + third_part]

        classes = np.array(classes)
        best_classes = classes[np.argmax(imputed_probs, axis=0)]

        return best_classes.flatten()

    def update_data_struct_1(self, original_dataset, data_with_nan):
        # update means, covariance, ...
        for col_upd in self.items.keys():  # for each col
            if self.current_col == col_upd:
                continue#skip col label
            #print('tot items keys', self.tot_items[self.current_col])
            for klass in self.items[self.current_col].keys():  # update mean each class
                self.sums[col_upd][klass] -= original_dataset[
                                         (original_dataset[:, col_upd] == klass) & ~(np.isnan(data_with_nan[:, col_upd])), :].sum(
                    axis=0)#rows that are labelled as klass and are null (imputation changed)

    def update_data_struct_2(self, original_dataset, data_with_nan):
        for col_upd in self.items.keys():  # for each col
            if self.current_col == col_upd:
                continue
            for klass in self.items[self.current_col].keys():  # update mean each class
                self.sums[col_upd][klass] += original_dataset[
                                         (original_dataset[:, col_upd] == klass) & ~(np.isnan(data_with_nan[:, col_upd])), :].sum(
                    axis=0)

    def set_current_col(self, current_col):
        self.current_col = current_col


    def get_learned_params(self):
        return self.best_params[:-2], self.best_params[-2]