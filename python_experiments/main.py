from collections import Counter

from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer

from SKLearn_bgd import SKLearn_bgd
from SKLearn_sgd import SKLearn_sgd
from bgd_classifier import CustomMICEClassifier
from sklearn.metrics import r2_score, f1_score

from load_dataset.load_inventory import load_inventory
from mice_optimized import generate_full_cofactor, OptimizedMICE
from unoptimized_mice import *

### subtraction
from load_dataset.load_flights import Flights
from logger import Logger

def metrics(predictions, q1, q3, true):
    #each experiment removes data and imputes k multiple imputations
    #this func receives a single experiment and extracts values
    bias = np.mean(predictions) - true
    percent_bias = np.abs(bias / true)*100
    avg_width = np.mean(q3 - q1)
    #check if true is in interval
    coverage_rate = np.mean((q1 < true) & (true < q3))

dataset = Flights()

x, models, idxs_nan = dataset.load_flights()#load_flights()#load_inventory()#synth_dataset(10000, 6, nan_cols_)
itr = 14 #iterations

logger = Logger()
test_bug = False


print("----------START OPTIMIZED-------")#full cofactor - nan cofactor
start_best = time.time()

mice = OptimizedMICE(models=models, logger=logger, regr_max_iter=10000, regr_max_iter_no_change=12, regr_tol=1e-4)
imputed_data = mice.single_mice(x.values, True, test_bug=False, itr = itr, sample_distribution=False)
dataset.test_result(imputed_data, logger)
end_best = time.time()

print("time best: ", end_best-start_best)

logger.next_model()

#standard sklearn
start_best = time.time()
print("----------START BGD-------")

estimator = SKLearn_bgd(models=models, regr_max_iter=500, max_iter_no_change = 5, regr_tol=1e-3)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
x_bk = x.copy()
imputed_standard = imputer.fit_transform(x)
imputed_vals = {}
for k, v in models.items():
    imputed_vals[k] = imputed_standard[idxs_nan[k], k]

dataset.test_result(imputed_vals, logger)
end_best = time.time()
print("time sklearn: ", end_best-start_best)

x = x_bk
x_bk = x.copy()
logger.next_model()

#generates a cofactor matrix for every column
start_best = time.time()
print("----------START UNOPTIMIZED-------")
estimator = UnoptimizedMICE(models = models, regr_max_iter=10000, max_iter_no_change = 12, regr_tol=1e-4)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_standard = imputer.fit_transform(x)

imputed_vals = {}
for k, v in models.items():
    imputed_vals[k] = imputed_standard[idxs_nan[k], k]

#test_result(imputed_vals, labels, models=models, logger=logger)
dataset.test_result(imputed_vals, logger)
end_best = time.time()
print("time unoptimized: ", end_best-start_best)

#mean - most popular baseline
logger.next_model()
print("----------START MEAN AND MOST POPULAR-------")
imputed_vals = {}
for k, v in models.items():
    start = time.time()
    elements = np.delete(x.values[:,k], idxs_nan[k], axis=0)
    count_missing = len(idxs_nan[k])#idxs_nan[k].sum()
    if v == 'regression':
        imputed_vals[k] = np.ones(count_missing) * np.mean(elements)
    else:
        imputed_vals[k] = np.ones(count_missing) * Counter(elements).most_common(1)[0][0]
    end = time.time()
    logger.log_train(k, end-start, "")

#test_result(imputed_vals, labels, models=models, logger=logger)
dataset.test_result(imputed_vals, logger)

##### single round performance
logger.next_model()

print("----------START SINGLE ROUND-------")
mice = OptimizedMICE(models=models, logger=logger, regr_max_iter=10000, regr_max_iter_no_change=12, regr_tol=1e-4)
imputed_data = mice.single_mice(x.values, True, test_bug=False, itr = 1, sample_distribution=False)
dataset.test_result(imputed_data, logger)

####SGD
logger.next_model()

print("----------START SGD-------")
start_best = time.time()
estimator = SKLearn_sgd(models=models)
imputer = IterativeImputer(estimator, skip_complete=True, verbose=2, imputation_order="roman", sample_posterior=False, max_iter=itr)
imputed_standard = imputer.fit_transform(x)

imputed_vals = {}
for k, v in models.items():
    imputed_vals[k] = imputed_standard[idxs_nan[k], k]

print("SGD::: ")
dataset.test_result(imputed_vals, logger)
end_best = time.time()


logger.close()

#dataset + test
