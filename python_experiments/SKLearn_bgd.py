import numpy as np
from sklearn.base import BaseEstimator, RegressorMixin
import time

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.metrics import mean_squared_error

learningRate = 0.001
power_t = 0.25


class SKLearn_bgd(BaseEstimator, RegressorMixin):
    def __init__(self, models={2:'regression', 4:'regression', 3:'regression', 1:'classification'}, regr_max_iter=10000, max_iter_no_change = 12, regr_tol=1e-4):
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


    def train_lda(self, X, y):
        self.clf = LinearDiscriminantAnalysis()
        self.clf.fit(X, y)

    def train_linear_reg(self, X, y, compute_only_gradient):
        coeff_matrix = np.concatenate([X, np.ones(len(X))[:, None]], axis=1)
        self.learned_coeffs = np.zeros(coeff_matrix.shape[1])

        n_iter_no_change = 0
        epoch = 0
        best_loss = None

        while n_iter_no_change < self.max_iter_no_change and epoch < self.regr_max_iter:  # epochs
            #predictions = np.multiply(self.learned_coeffs, coeff_matrix)
            predictions = (np.matmul(self.learned_coeffs[None], coeff_matrix.T))[0]
            loss = mean_squared_error(y, predictions, squared=True)#training loss
            gradient = np.sum(
                np.multiply(predictions - y, coeff_matrix.T), axis=1)

            curr_lr = learningRate#(float(learningRate) / ((epoch + 1) ** power_t ))
            self.learned_coeffs -= curr_lr * (2 / coeff_matrix.shape[0]) * gradient
            '''
            if compute_only_gradient:
                loss = np.linalg.norm(gradient)
            else:
                loss = mean_squared_error(y, self.predict(X, params=self.learned_coeffs))
            '''
            if best_loss is None or (loss + self.regr_tol) < best_loss:
                best_loss = loss
                n_iter_no_change = 0
                self.best_params = self.learned_coeffs.copy()
            else:
                n_iter_no_change += 1
            epoch += 1

        #print("params: weights: ", self.get_learned_params()[0], " bias ", self.get_learned_params()[1])
        print("done. Iterations required: ", epoch)
        return self

    def predict(self, X, y=None, params=None):

        if self.trained == 'regression':
            res = self.predict_linear_reg(X, y, params)
        else:
            res = self.predict_lda(X, y)
        self.t_end = time.time()
        f = open('metrics.txt', 'a+')
        f.write(str(2) + ";" + str(0) + ";;" + str(self.t_end - self.t_start) + "\n")
        f.close()
        #self.logger.log_sklearn(self.t_end-self.t_start)
        return res

    def predict_lda(self, X, y=None):
        return self.clf.predict(X)

    def predict_linear_reg(self, X, y=None, params=None):
        if params is None:
            predictions = np.sum(np.multiply(self.best_params[:-1], X), axis=1) + self.best_params[-1]
        else:
            predictions = np.sum(np.multiply(params[:-1], X), axis=1) + params[-1]
        return predictions

    def get_learned_params(self):
        return self.best_params[:-1], self.best_params[-1]
