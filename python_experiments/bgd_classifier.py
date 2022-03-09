from sklearn.base import BaseEstimator, RegressorMixin
import numpy as np
'''

mean_1 = x[x[:,4]==1].mean(axis=0)[:-1]
mean_0 = x[x[:,4]==0].mean(axis=0)[:-1]

sums_0 = x[x[:, 4] == 0].sum(axis=0)[:-1]
sums_1 = x[x[:, 4] == 1].sum(axis=0)[:-1]

scarti_1 = (x[x[:, 4]==1,:-1]-mean_1)
scarti_0 = (x[x[:, 4]==0,:-1]-mean_0)

(np.matmul(scarti_0.T, scarti_0) + np.matmul(scarti_1.T, scarti_1))/4

(cofactor + (2*np.matmul(mean_1[None].T, mean_1[None])) + (2*np.matmul(mean_0[None].T, mean_0[None])) - (np.matmul(mean_0[None].T, sums_0[None])) - (np.matmul(mean_1[None].T, sums_1[None])) - (np.matmul(sums_0[None].T, mean_0[None])) - (np.matmul(sums_1[None].T, mean_1[None])))/4



'''

import scipy


class CustomMICEClassifier(BaseEstimator, RegressorMixin):
    def __init__(self, label):#index label
        self.label = label

    '''
    last col of coeff_matrix MUST be of 1s
    '''
    def fit(self):
        return self

    def predict(self, X, covariance, means, n_items):

        col_index = np._r[0:self.label, self.label+1:X.shape[1]]
        feat_impute = X[:, col_index]
        covariance_impute = covariance[col_index, col_index]

        imputed_probs = []
        classes = []

        for k, v in means.items():
            classes += [k]
            current_class_mean = v[col_index]
            imputed_probs += [scipy.stats.multivariate_normal.pdf(feat_impute, mean=current_class_mean, cov=covariance_impute) * n_items[k]]

        classes = np.array(classes)
        best_classes = classes[np.argmax(imputed_probs)]

        return best_classes

    def get_learned_params(self):
        return self.best_params[:-2], self.best_params[-2]