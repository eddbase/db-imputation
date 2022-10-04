import numpy as np

from bgd_classifier import CustomMICEClassifier
from load_dataset.dataset_utils import synth_dataset_classification


def generate_full_cofactor(x, numeric=np.array([False, True])):

    from sklearn.impute import SimpleImputer #most_frequent
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    pre_cofactor_numeric = imp_mean.fit_transform(x[:, numeric])
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='most_frequent')
    pre_cofactor_classes = imp_mean.fit_transform(x[:, ~numeric])
    pre_cofactor = np.zeros(x.shape)
    pre_cofactor[:, np.where(numeric == True)] = pre_cofactor_numeric
    pre_cofactor[:, np.where(numeric == False)] = pre_cofactor_classes

    coeff_matrix = np.matmul(pre_cofactor.T, pre_cofactor)#no constant coeffs.

    return coeff_matrix, pre_cofactor, x #cofactor, full table imputed, table with nan

def removal_appr(x, nan_cols_, materialize_cof_diff, test_bug=False):
    coeff_matrix, pre_cofactor, x_1 = generate_full_cofactor(x)

    sums = {}
    items = {}

    for col in np.where(nan_cols_):
        sums[col] = {}
        items[col] = {}

        idx_nan = np.argwhere(~np.isnan(x_1[:, col]))[:, 0]
        feats_not_nan = pre_cofactor[idx_nan, :]  # nan rows

        for i in np.unique(feats_not_nan[:, col]):  # for each class
            tmp = feats_not_nan[np.where(feats_not_nan[:, col] == i)]
            sums[col][i] = tmp.sum(axis=0)
            items[col][i] = len(tmp)



    for iteration in range(3):
        for col in nan_cols_:
            mice_model = CustomMICEClassifier(col)
            idx_nan = np.argwhere(np.isnan(x_1[:, col]))[:, 0]
            feats_nan = pre_cofactor[idx_nan, :]#nan rows

            #materialize coeff. diff.
            mk = np.matmul(feats_nan.T, feats_nan)#train over original cofactor - delta
            covariance = coeff_matrix - mk

            means = {}

            #build covariance
            for i in np.unique(pre_cofactor[:, col]):#or x_1, for each class
                means[i] = sums[col][i]/items[col][i]#already removed nans for each class
                covariance += (items[col][i] * np.matmul(means[i][None].T, means[i][None]))
                product = np.matmul(means[i][None].T, sums[col][i][None])
                covariance -= product
                covariance -= product.T

            covariance /= x.shape[0]

            imputed_vals = mice_model.predict(x_1[idx_nan, :], covariance, means[col], items[col])

            old_cofactor = np.sum(feats_nan * feats_nan[:, col][:, None], axis=0)

            old_sum = {}
            #update means, covariance, ...
            for col_upd in means.keys():#for each col
                if col == col_upd:
                    continue
                old_sum[col_upd] = {}
                for klass in np.unique(pre_cofactor[:, col_upd]):#update mean each class
                    means[col_upd][klass] -= pre_cofactor[(pre_cofactor[:, col_upd] == klass) & ~(np.isnan(x_1[:, col_upd])), :].sum(axis=0)


            pre_cofactor[idx_nan, col] = imputed_vals

            for col_upd in means.keys():#for each col
                if col == col_upd:
                    continue
                for klass in np.unique(pre_cofactor[:, col_upd]):#update mean each class
                    means[col_upd][klass] += pre_cofactor[(pre_cofactor[:, col_upd] == klass) & ~(np.isnan(x_1[:, col_upd])), :].sum(axis=0)

            #update covariance

            feats_nan = pre_cofactor[idx_nan, :]
            new_cofactor = np.sum(feats_nan * predictions[:, None], axis=0)

            delta = new_cofactor - old_cofactor
            coeff_matrix[:, col] += delta
            coeff_matrix[col, :] += delta
            coeff_matrix[col, col] -= delta[col]

            if test_bug:
                rebuild_matrix = np.matmul(pre_cofactor.T, pre_cofactor)
                assert (np.isclose(rebuild_matrix, coeff_matrix).all())
                print("test ok")

    print("done")
    return pre_cofactor


x, labels = synth_dataset_classification([0,2,4,6])#synth_dataset(10000, 6, nan_cols_)

nan_cols_ = np.where(np.isnan(x).any(axis=0))[0]
print(nan_cols_)
imputed_data_1 = removal_appr(x, nan_cols_, True, test_bug=True)


