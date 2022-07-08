from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.metrics import mean_squared_error
import math
import numpy as np
import time

learningRate = 1e-3


##given a matrix trains a regression model. Always generates cofactor matrix
class UnoptimizedMICE(BaseEstimator, RegressorMixin):
    def __init__(self, models={2:'regression', 4:'regression', 3:'regression', 1:'classification'}, regr_max_iter=1000, max_iter_no_change = 5, regr_tol=1e-3):
        self.best_params = None

        self.best_params = None
        self.models = models
        self.cols = list(models.keys())
        self.cols.sort()
        self.trained = ''
        self.regr_max_iter = regr_max_iter
        self.max_iter_no_change = max_iter_no_change
        self.regr_tol=regr_tol

        #self.logger = logger


    def fit(self, X, y, compute_only_gradient=True):
        self.t_start = time.time()
        y_int = y.astype(int)
        if len(np.unique(y)) < 200 and np.all(y_int == y):
            print('train classifier')
            self.trained = 'classifier'
            self.train_lda(X, y)
        else:
            print('train regression')
            self.trained = 'regression'
            self.train_linear_reg(X, y, compute_only_gradient)
        #print('time train: ', time.time()-t_1)


    def train_lda(self, X, y):

        covariance = np.matmul(X.T, X)
        self.sums = {}
        self.items = {}
        for clss in np.unique(y):
            idxs = np.where(y == clss)
            self.items[clss] = len(idxs[0])
            self.sums[clss] = X[idxs, :].sum(axis=1)

        for clss in np.unique(y):
            covariance -= (np.matmul(self.sums[clss].reshape(-1, 1), self.sums[clss].reshape(-1, 1).T) / self.items[clss])

        covariance /= len(X)
        self.inv_matrix = np.linalg.pinv(covariance)
        self.tot_items = len(X)

        #self.clf = LinearDiscriminantAnalysis()
        #self.clf.fit(X, y)

    def train_linear_reg(self, X, y, compute_only_gradient=True):
        coeff_matrix = np.concatenate([X, np.ones(len(X))[:, None], y[:, None]], axis=1)
        coeff_matrix = np.matmul(coeff_matrix.T, coeff_matrix)

        self.learned_coeffs = np.zeros(coeff_matrix.shape[0])
        # self.learned_coeffs = self.learned_coeffs.astype(np.float128)

        n_iter_no_change = 0
        epoch = 0
        best_loss = None

        while n_iter_no_change < self.max_iter_no_change and epoch < self.regr_max_iter:  # epochs
            self.learned_coeffs[-1] = -1
            gradient = np.matmul(self.learned_coeffs, coeff_matrix)#((np.sum(np.multiply(self.learned_coeffs, coeff_matrix), axis=1)))
            self.learned_coeffs -= learningRate * (2 / X.shape[0]) * gradient
            # print("gradient: ", np.linalg.norm(gradient[:-1]))

            if compute_only_gradient:
                loss = np.linalg.norm(gradient[:-1])
                #print("loss: ", loss)
            else:
                loss = mean_squared_error(y, self.predict(X, params=self.learned_coeffs))
                #print("loss: ", loss)

            if best_loss is None or (loss + self.regr_tol) < best_loss:
                best_loss = loss
                n_iter_no_change = 0
                self.best_params = self.learned_coeffs.copy()
            else:
                n_iter_no_change += 1
            epoch += 1

        #print("params: weights: ", self.get_learned_params()[0], " bias ", self.get_learned_params()[1])
        return self

    def predict(self, X, y=None, params=None):
        t = time.time()
        if self.trained == 'regression':
            pred = self.predict_linear_reg(X, y, params)
        else:
            pred = self.predict_lda(X, y)
        self.t_end = time.time()
        #self.logger.log_sklearn(self.t_end-self.t_start)

        f = open('metrics.txt', 'a+')
        f.write(str(3) + ";" + str(0) + ";;" + str(self.t_end - self.t_start) + "\n")
        f.close()
        print('time predict: ', time.time() - t)
        return pred

    def predict_lda(self, X, y=None):
        feat_impute = X
        imputed_probs = []
        classes = []
        for k, v in self.sums.items():  # k = class, v=mean
            classes += [k]
            curr_means = (v / self.items[k]).reshape(-1, 1)
            sec_part = (1 / 2) * np.matmul(np.matmul(curr_means.T, self.inv_matrix), curr_means)
            third_part = np.matmul(np.matmul(feat_impute, self.inv_matrix), curr_means)
            imputed_probs += [math.log(self.items[k] / self.tot_items) - sec_part + third_part]

        classes = np.array(classes)
        best_classes = np.ravel(classes[np.argmax(imputed_probs, axis=0)])
        return best_classes
        #return self.clf.predict(X)


    def predict_linear_reg(self, X, y=None, params=None):
        if params is None:
            predictions = np.sum(np.multiply(self.best_params[:-2], X), axis=1) + self.best_params[-2]
        else:
            predictions = np.sum(np.multiply(params[:-2], X), axis=1) + params[-2]
        return predictions

    def get_learned_params(self):
        return self.best_params[:-2], self.best_params[-2]




# creates matrices optimized approach
def build_matrices(X_nan, nan_cols=[0, 2, 4], start_matrix=None):
    from sklearn.impute import SimpleImputer
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    fact_matrix = {}
    len_train_matrix = {}
    if start_matrix is not None:
        imputed_matrix = start_matrix
    else:
        imputed_matrix = imp_mean.fit_transform(X_nan)  # put mean in other nan cells

    imputed_matrix_constant = np.concatenate([imputed_matrix, np.ones(len(imputed_matrix))[:, None]], axis=1)

    for col in nan_cols:
        idx_nan = np.argwhere(np.isnan(X_nan[:, col]))[:, 0]  # nan-s in current col
        is_row_not_nan = np.ones(X_nan.shape[0], dtype=bool)  # remove nan rows and nan col
        is_row_not_nan[idx_nan] = False
        submatrix = imputed_matrix_constant[is_row_not_nan, :]
        len_train_matrix[col] = submatrix.shape[0]
        submatrix_ = np.matmul(submatrix.T, submatrix)
        fact_matrix[col] = submatrix_

    return fact_matrix, len_train_matrix, imputed_matrix_constant
