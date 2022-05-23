from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.metrics import mean_squared_error

import numpy as np
import time

learningRate = 1e-3

class StandardSKLearnImputation(BaseEstimator, RegressorMixin):
    def __init__(self, models={2:'regression', 4:'regression', 3:'regression', 1:'classification'}):
        self.best_params = None
        self.models = models
        self.cols = list(models.keys())
        self.cols.sort()
        self.trained = ''

    '''
    fit the model. tol and max_iter_no_change: early stopping parameters
    '''

    def fit(self, X, y, compute_only_gradient=True, max_epochs=4000, tol=1e-5, max_iter_no_change=20):
        t = time.time()

        y_int = y.astype(int)
        if len(np.unique(y)) < 200 and np.all(y_int == y):
            print('train classifier')
            self.trained = 'classifier'
            self.train_lda(X, y)
        else:
            print('train regression')
            self.trained = 'regression'
            self.train_linear_reg(X, y, compute_only_gradient, max_epochs, tol, max_iter_no_change)


    def train_lda(self, X, y):
        self.clf = LinearDiscriminantAnalysis()
        self.clf.fit(X, y)

    def train_linear_reg(self, X, y, compute_only_gradient, max_epochs, tol, max_iter_no_change):
        coeff_matrix = np.concatenate([X, np.ones(len(X))[:, None]], axis=1)
        self.learned_coeffs = np.zeros(coeff_matrix.shape[1])

        n_iter_no_change = 0
        epoch = 0
        best_loss = None

        while n_iter_no_change < max_iter_no_change and epoch < max_epochs:  # epochs
            gradient = np.sum(
                np.multiply(np.sum(np.multiply(self.learned_coeffs, coeff_matrix), axis=1) - y, coeff_matrix.T), axis=1)
            self.learned_coeffs -= learningRate * (2 / coeff_matrix.shape[0]) * gradient

            if compute_only_gradient:
                loss = np.linalg.norm(gradient)
            else:
                loss = mean_squared_error(y, self.predict(X, params=self.learned_coeffs))

            if best_loss is None or (loss + tol) < best_loss:
                best_loss = loss
                n_iter_no_change = 0
                self.best_params = self.learned_coeffs.copy()
            else:
                n_iter_no_change += 1
            epoch += 1

        #print("params: weights: ", self.get_learned_params()[0], " bias ", self.get_learned_params()[1])
        print("done")
        return self

    def predict(self, X, y=None, params=None):
        t = time.time()

        if self.trained == 'regression':
            res = self.predict_linear_reg(X, y, params)
        else:
            res = self.predict_lda(X, y)

        print('time predict: ', time.time()-t)
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


##given a matrix trains a regression model. Always generates cofactor matrix
class UnoptimizedMICE(BaseEstimator, RegressorMixin):
    def __init__(self, models={2:'regression', 4:'regression', 3:'regression', 1:'classification'}):
        self.best_params = None

        self.best_params = None
        self.models = models
        self.cols = list(models.keys())
        self.cols.sort()
        self.trained = ''


    def fit(self, X, y, compute_only_gradient=True, max_epochs=4000, tol=1e-5, max_iter_no_change=20):
        t_1 = time.time()
        y_int = y.astype(int)
        if len(np.unique(y)) < 200 and np.all(y_int == y):
            print('train classifier')
            self.trained = 'classifier'
            self.train_lda(X, y)
        else:
            print('train regression')
            self.trained = 'regression'
            self.train_linear_reg(X, y, compute_only_gradient, max_epochs, tol, max_iter_no_change)
        print('time train: ', time.time()-t_1)


    def train_lda(self, X, y):
        self.clf = LinearDiscriminantAnalysis()
        self.clf.fit(X, y)

    def train_linear_reg(self, X, y, compute_only_gradient=True, max_epochs=4000, tol=1e-5, max_iter_no_change=20):
        coeff_matrix = np.concatenate([X, np.ones(len(X))[:, None], y[:, None]], axis=1)
        coeff_matrix = np.matmul(coeff_matrix.T, coeff_matrix)

        self.learned_coeffs = np.zeros(coeff_matrix.shape[0])
        # self.learned_coeffs = self.learned_coeffs.astype(np.float128)

        n_iter_no_change = 0
        epoch = 0
        best_loss = None

        while n_iter_no_change < max_iter_no_change and epoch < max_epochs:  # epochs
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

            if best_loss is None or (loss + tol) < best_loss:
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

        print('time predict: ', time.time() - t)
        return pred

    def predict_lda(self, X, y=None):
        return self.clf.predict(X)


    def predict_linear_reg(self, X, y=None, params=None):
        if params is None:
            predictions = np.sum(np.multiply(self.best_params[:-2], X), axis=1) + self.best_params[-2]
        else:
            predictions = np.sum(np.multiply(params[:-2], X), axis=1) + params[-2]
        return predictions

    def get_learned_params(self):
        return self.best_params[:-2], self.best_params[-2]



##given a matrix trains a regression model.
class CustomMICERegressor(BaseEstimator, RegressorMixin):
    def __init__(self, label):#index label
        self.best_params = None
        self.label = label

    '''
    last col of coeff_matrix MUST be of 1s
    '''
    def fit(self, coeff_matrix, len_original_data, original_data=None, y=None, max_epochs=6000, tol=1e-6,
            max_iter_no_change=20, nan_raw_data = None, equality_constraint_feats=None, beta=0):#sequence of submatrices where constraint should hold

        #print("matrix ", coeff_matrix)

        self.learned_coeffs = np.zeros(coeff_matrix.shape[0])

        n_iter_no_change = 0
        epoch = 0
        best_loss = None

        while n_iter_no_change < max_iter_no_change and epoch < max_epochs:  # epochs
            self.learned_coeffs[self.label] = -1
            #print(np.multiply(self.learned_coeffs, coeff_matrix))
            #gradient = ((np.sum(np.multiply(self.learned_coeffs, coeff_matrix), axis=1)))
            gradient = np.matmul(self.learned_coeffs, coeff_matrix)

            if nan_raw_data is not None:#method b training
                gradient -= np.matmul(np.matmul(self.learned_coeffs, nan_raw_data.T), nan_raw_data)  # transposed

            gradient_constraint = np.zeros(len(self.learned_coeffs))

            if equality_constraint_feats is not None:

                submatrices = np.concatenate(equality_constraint_feats)
                matrix_normalize = np.identity(submatrices.shape[0])
                prev_index = 0
                second_norm = np.ones(len(equality_constraint_feats))

                for constraints in equality_constraint_feats:
                    matrix_normalize[prev_index:prev_index+constraints.shape[0], :] *= (2/(constraints.shape[0]))#* len(equality_constraint_feats)
                    #second_norm[prev_index:prev_index+constraints.shape[0]] *= (-2/((constraints.shape[0] ** 2)))#* len(equality_constraint_feats)
                    prev_index += constraints.shape[0]

                predictions = self.predict(submatrices, params=self.learned_coeffs)
                first = np.matmul(submatrices.T, np.matmul(matrix_normalize, predictions[None].T)).squeeze()

                sec = np.zeros(len(self.learned_coeffs))
                prev_index = 0
                for constraints in equality_constraint_feats:
                    predictions_sums = (predictions[prev_index:prev_index+constraints.shape[0]].sum())*(-2/(constraints.shape[0] ** 2))
                    submatrices_sums = (submatrices[prev_index:prev_index + constraints.shape[0]].sum(axis=0))
                    sec += (predictions_sums * submatrices_sums)
                    prev_index += constraints.shape[0]

                #second_norm *= (predictions.sum())
                #sec = np.matmul(second_norm[None], submatrices)
                gradient_constraint = np.squeeze(beta*(first + sec))/ len(equality_constraint_feats)

            gradient *= (2 / len_original_data)
            #gradient_constraint = np.zeros(len(self.learned_coeffs))
            complete_gradient = gradient + gradient_constraint
            self.learned_coeffs -= learningRate * complete_gradient
            #print("coeffs: ", self.learned_coeffs)
            # print("gradient: ", np.linalg.norm(gradient[:-1]))

            if y is not None and original_data is not None:  # stop using train/validation loss
                predictions = self.predict(original_data, params=self.learned_coeffs)
                loss_complete = mean_squared_error(y, predictions)
                print("epoch: ", epoch, "loss: ", loss_complete)
            else:  # stop using train gradient
                order = np.r_[0:self.label, self.label + 1:len(self.learned_coeffs)]
                loss = np.linalg.norm(gradient[order])
                loss_c = np.linalg.norm(gradient_constraint[order])
                loss_complete = np.linalg.norm(complete_gradient[order])

            if best_loss is None or (loss_complete + tol) < best_loss:
                best_loss = loss_complete
                n_iter_no_change = 0
                self.best_params = self.learned_coeffs.copy()
            else:
                n_iter_no_change += 1
            epoch += 1

        return self

    def predict(self, X, y=None, params=None):

        bool_idx_col = np.ones(X.shape[1], dtype=bool)
        bool_idx_col[self.label] = False
        matrix_extract = X[:, bool_idx_col]

        if params is not None:
            prediction_params = params[bool_idx_col]
        else:
            prediction_params = self.best_params[bool_idx_col]

        predictions = (np.matmul(prediction_params[None], matrix_extract.T))[0]
        #print("predictions ", predictions)
        #predictions = np.sum(np.multiply(prediction_params, matrix_extract), axis=1)# + self.best_params[-1]
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

'''
class Removal2MICERegressor(BaseEstimator, RegressorMixin):
    def __init__(self, label):#index label
        self.best_params = None
        self.label = label

    def fit(self, coeff_matrix, len_original_data, full_table_to_subtract, original_data=None, y=None, max_epochs=4000, tol=1e-3,
            max_iter_no_change=2):

        self.learned_coeffs = np.zeros(coeff_matrix.shape[0])
        # self.learned_coeffs = self.learned_coeffs.astype(np.float128)

        n_iter_no_change = 0
        epoch = 0
        best_loss = None

        while n_iter_no_change < max_iter_no_change and epoch < max_epochs:  # epochs
            self.learned_coeffs[self.label] = -1
            gradient = np.matmul(self.learned_coeffs, coeff_matrix)
            gradient_sub = np.matmul(np.matmul(self.learned_coeffs, full_table_to_subtract.T), full_table_to_subtract)#transposed
            gradient -= gradient_sub

            self.learned_coeffs -= learningRate * (2 / len_original_data) * gradient
            # print("gradient: ", np.linalg.norm(gradient[:-1]))

            if y is not None and original_data is not None:  # stop using train/validation loss
                predictions = self.predict(original_data, params=self.learned_coeffs)
                loss = mean_squared_error(y, predictions)
                print("epoch: ", epoch, "loss: ", loss)
            else:  # stop using train gradient
                order = np.r_[0:self.label, self.label + 1:len(self.learned_coeffs)]
                #print("LABEL: ", self.label, "order", order)
                loss = np.linalg.norm(gradient[order])
                #print("Label: ", self.label, "loss: ", loss)

            if best_loss is None or (loss + tol) < best_loss:
                best_loss = loss
                n_iter_no_change = 0
                self.best_params = self.learned_coeffs.copy()
            else:
                n_iter_no_change += 1
            epoch += 1

        return self

    def predict(self, X, y=None, params=None):

        bool_idx_col = np.ones(X.shape[1], dtype=bool)
        bool_idx_col[self.label] = False
        matrix_extract = X[:, bool_idx_col]

        prediction_params = self.best_params[bool_idx_col]
        if params is not None:
            prediction_params = params[bool_idx_col]

        predictions = np.sum(np.multiply(prediction_params, matrix_extract), axis=1)# + self.best_params[-1]
        return predictions

    def get_learned_params(self):
        return self.best_params[:-2], self.best_params[-2]
'''