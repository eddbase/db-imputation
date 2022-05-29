from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer

from SKLearn_bgd import StandardSKLearnImputation
from bgd_classifier import CustomMICEClassifier
from sklearn.metrics import r2_score, f1_score

from load_dataset.load_inventory import load_inventory
from mice_optimized import mice, generate_full_cofactor
from unoptimized_mice import *

### subtraction
from load_dataset.load_flights import load_flights
from logger import Logger



'''
remove cofactor matrix
'''


def test_result(data_changed, nan_dataset, true_labels, models = {2:'regression', 4:'regression', 3:'regression', 1:'classification'}, logger=None):
    for col, model in models.items():
        idx_nan = np.argwhere(np.isnan(nan_dataset.values[:, col]))[:, 0]
        imputed_data = data_changed[idx_nan, col]
        if model == 'regression':
            print('MSE', mean_squared_error(true_labels[col], imputed_data))
            print('R2', r2_score(true_labels[col], imputed_data))

            logger.log_test('MSE', mean_squared_error(true_labels[col], imputed_data))
            logger.log_test('R2', r2_score(true_labels[col], imputed_data))
        else:
            print('F1', f1_score(true_labels[col], imputed_data, average='micro'))
            logger.log_test('F1', f1_score(true_labels[col], imputed_data, average='micro'))



#models = {2:'regression', 3:'regression', 1:'classification', 0:'classification', 11:'regression', 15:'regression'}#INVENTORY
#models = {2: 'regression', 3:'classification',6:'classification', 7:'regression'}#FAVORITA
#models = {0: 'regression', 3:'regression',4:'regression', 7:'classification', 8:'classification', 9:'classification'}#0,3,4,7,8,9
#x, labels = load_inventory()#synth_dataset(10000, 6, nan_cols_)
x, labels, models = load_inventory()#synth_dataset(10000, 6, nan_cols_)
print(x.columns)
print(models)

itr = 4 #iterations

nan_cols_ = np.where(np.isnan(x).any(axis=0))[0]

logger = Logger()

test_bug = False

print("1")#full cofactor - nan cofactor
start_best = time.time()
imputed_data_1 = mice(x, nan_cols_, True, test_bug=test_bug, itr=itr, models=models, logger=logger, sample_distribution=False)#best approach so far
test_result(imputed_data_1, x, labels, models=models, logger=logger)
end_best = time.time()

print("time best: ", end_best-start_best)

logger.next_model()
'''
start_best = time.time()
#full cofactor - nan cofactor, second approach
imputed_data_2 = removal_appr(x, nan_cols_, False, test_bug=test_bug, itr=itr, models=models, logger=logger)
test_result(imputed_data_2, x, labels, models=models, logger=logger)
end_best = time.time()
print("time second best: ", end_best-start_best)

logger.next_model()
'''
#standard sklearn
start_best = time.time()
estimator = StandardSKLearnImputation(models=models)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_standard = imputer.fit_transform(x)
test_result(imputed_standard, x, labels, models=models, logger=logger)
end_best = time.time()
print("time sklearn: ", end_best-start_best)

logger.next_model()

#generates a cofactor matrix for every column
start_best = time.time()
estimator = UnoptimizedMICE(models = models)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_fact_no_opt = imputer.fit_transform(x)
test_result(imputed_fact_no_opt, x, labels, models=models, logger=logger)
end_best = time.time()
print("time unoptimized: ", end_best-start_best)

#mean - most popular baseline
logger.next_model()

classification_cols = []
numerical_cols = []
for k, v in models.items():
    if v == 'regression':
        numerical_cols += [k]
    else:
        classification_cols += [k]

cof_matrix, imputed_dataset, null_dataset = generate_full_cofactor(x, numerical_cols,
                                                                   classification_cols)  # cofactor matrix
test_result(imputed_dataset, x, labels, models=models, logger=logger)

##### single round performance

imputed_data_1 = mice(x, nan_cols_, True, test_bug=test_bug, itr=1, models=models, logger=logger)#best approach so far
test_result(imputed_data_1, x, labels, models=models, logger=logger)

#add sample from distribution + mi convergence check

logger.close()
