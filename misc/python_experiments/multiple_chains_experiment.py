import numpy as np
import math

from sklearn.linear_model import LinearRegression


def generate_data(n, beta=1, sigma2 = 1):
    x = np.random.normal(0, 1, n)
    y = (beta * x) + np.random.normal(0, math.sqrt(sigma2), n)
    return np.array([x, y]).T


def make_missing(data, p=0.5):
    r = np.random.choice(data.shape[0], data.shape[0]//2)
    data[r, 0] = np.nan
    return data

from mice_optimized import OptimizedMICE


def make_mice(data, chains=5):
    mice = OptimizedMICE(models={0:'regression'})
    imputed_data = mice.mice_chains(data, True, chains, test_bug=False, itr = 5, logger=None, sample_distribution = True)
    coefs = []
    row_idxs = np.isnan(data[:, 0])
    for x, v in imputed_data.items():#for each chain
        for x_1, v_1 in v.items():#for each missing col
            data[row_idxs, 0] = v_1

        reg = LinearRegression().fit(data[:, 0].reshape(-1, 1), data[:, 1])
        coefs += [np.append(reg.coef_, reg.intercept_).reshape(-1, 1).T]

    #pool coefs
    coefs = np.concatenate(coefs, axis=0)
    #compute avg, %
    r_1 = np.quantile(coefs, 0.5, axis=0)
    r_2 = np.quantile(coefs, 0.025, axis=0)
    r_3 = np.quantile(coefs, 0.975, axis=0)
    return r_1, r_2, r_3


def metrics(mean, q1, q3, true):
    #each experiment removes data and imputes k multiple imputations
    #this func receives a single experiment and extracts values
    bias = np.mean(mean) - true
    percent_bias = np.abs(bias / true)*100
    print("bias: ", bias, " . Bias %: ", percent_bias)
    avg_width = np.mean(q3 - q1)
    print("Avg. width: ", avg_width)
    #check if true is in interval
    coverage_rate = np.mean((q1 <= true) & (true <= q3))
    print("Coverage rate: ", coverage_rate)

means = []
q_1s = []
q_3s = []

for _ in range(500):
    data = generate_data(100)
    print(data.shape)
    data = make_missing(data)
    mean, q_1, q_3 = make_mice(data)
    means+=[mean[0]]
    q_1s += [q_1[0]]
    q_3s += [q_3[0]]
    print("Q1: ", q_1[0], " Q3: ", q_3[0])

metrics(np.array(means), np.array(q_1s), np.array(q_3s), 1)