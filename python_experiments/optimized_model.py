
##given a matrix trains a regression model.
from sklearn.base import BaseEstimator, RegressorMixin
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error

learningRate = 1e-3

class CustomMICERegressor(BaseEstimator, RegressorMixin):
    def __init__(self, label, sample_dist = False):#index label
        self.best_params = None
        self.label = label
        self.sample_dist = sample_dist

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
            #print(gradient)

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
                #print("loss: ", loss_complete)

            if best_loss is None or (loss_complete + tol) < best_loss:
                best_loss = loss_complete
                n_iter_no_change = 0
                self.best_params = self.learned_coeffs.copy()
            else:
                n_iter_no_change += 1
            epoch += 1
        #print("------")

        if self.sample_dist:
            #estimate variance
            row_y = coeff_matrix[self.label, :]
            first_part = row_y[self.label]
            #filter_y = np.r_[0:self.label, self.label+1:len(row_y)]
            filter_y = np.ones(coeff_matrix.shape[1], dtype=bool)
            filter_y[self.label] = False
            params_sigma = self.best_params[filter_y].reshape(-1,1)
            sec_part = row_y[filter_y].reshape(1,-1)  # cofactor y without y*y
            sec_part = np.matmul(sec_part, params_sigma)
            filter_cofactor = coeff_matrix[filter_y, :]
            filter_cofactor = filter_cofactor[:, filter_y]
            third_part = np.matmul(params_sigma.T, np.matmul(filter_cofactor, params_sigma))
            self.sigma = np.sqrt((first_part - (2*sec_part) + third_part)/(len_original_data - coeff_matrix.shape[1] - 1))
            print("sigma: ", self.sigma)
        return self

    def predict(self, X, y=None, params=None):
        #print("using predict...")
        bool_idx_col = np.ones(X.shape[1], dtype=bool)
        bool_idx_col[self.label] = False
        matrix_extract = X[:, bool_idx_col]

        if params is not None:
            prediction_params = params[bool_idx_col]
        else:
            prediction_params = self.best_params[bool_idx_col]

        predictions = (np.matmul(prediction_params[None], matrix_extract.T))[0]
        if self.sample_dist:
            noise = np.ravel(np.random.normal(size=len(predictions)) * self.sigma)
            predictions += noise
            print("predictions: ", predictions)

        return predictions

    def get_learned_params(self):
        return self.best_params[:-2], self.best_params[-2]
