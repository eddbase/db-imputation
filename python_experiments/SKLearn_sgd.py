import numpy as np
from sklearn.base import BaseEstimator, RegressorMixin
import time

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error


class SKLearn_sgd(BaseEstimator, RegressorMixin):
    def __init__(self, models={2:'regression', 4:'regression', 3:'regression', 1:'classification'}, regr_max_iter=10000, regr_tol=1e-4, max_iter_no_change=12):
        self.best_params = None
        self.models = models
        self.cols = list(models.keys())
        self.cols.sort()
        self.trained = ''
        self.regr_max_iter = regr_max_iter
        self.max_iter_no_change = max_iter_no_change
        self.regr_tol=regr_tol

        #self.logger=logger

    '''
    fit the model. tol and max_iter_no_change: early stopping parameters, tol 1e-5, 2k max epochs
    '''

    def fit(self, X, y, compute_only_gradient=True, ):
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


    def train_lda(self, X, y):
        self.clf = LinearDiscriminantAnalysis()
        self.clf.fit(X, y)

    def train_linear_reg(self, X, y, compute_only_gradient):
        #coeff_matrix = np.concatenate([X, np.ones(len(X))[:, None]], axis=1)
        #self.learned_coeffs = np.zeros(coeff_matrix.shape[1])
        self.regressor = SGDRegressor(max_iter=self.regr_max_iter, tol=self.regr_tol, n_iter_no_change=self.max_iter_no_change, shuffle=True, eta0=0.001, power_t=0.25, random_state=0, alpha=0, verbose=1, learning_rate='constant')
        print(self.regressor)
        self.regressor.fit(X, y)
        return self

    def predict(self, X, y=None, params=None):

        if self.trained == 'regression':
            res = self.predict_linear_reg(X, y, params)
        else:
            res = self.predict_lda(X, y)
        self.t_end = time.time()
        f = open('metrics.txt', 'a+')
        f.write(str(6) + ";0;;" + str(self.t_end - self.t_start) + "\n")
        f.close()
        #self.logger.log_sklearn(self.t_end-self.t_start)
        return res

    def predict_lda(self, X, y=None):
        return self.clf.predict(X)

    def predict_linear_reg(self, X, y=None, params=None):
        return self.regressor.predict(X)

    #def get_learned_params(self):
    #    return self.best_params[:-1], self.best_params[-1]
