import csv
import json
import sys
import time
from pprint import pprint
import re
from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as f
import seaborn as sns
import matplotlib.pylab as plt
from statsmodels.tsa.arima.model import ARIMA
from scipy.stats import t
import pandas as pd

PARAM_RANGE = 2

def __my_arima(y, param):
    model = ARIMA(y, order=param)
    aic = model.fit()
    return aic

def predict_time_series(x):
    params = []
    # s_params = []
    for p in range(PARAM_RANGE):
        for d in range(PARAM_RANGE):
            for q in range(PARAM_RANGE):
                params.append([p, d, q])
                # s_params.append([p, d, q, 12])

    res_param = 0
    # res_s_param = 0
    min_aic = 1000000000
    for param in params:
        # for s_param in s_params:
            aic = __my_arima(x, param)
            try:
                if aic.aic < min_aic:
                    min_aic = aic.aic
                    res_param = param
                    # res_s_param = s_param
            except:
                continue
    res = __my_arima(x, res_param)
    return (res.get_forecast(steps = 15).predicted_mean).tolist()


ss = SparkSession.builder.appName('Poverty factors').getOrCreate()
extreme_pov_df = ss.read.csv('./indiaPov.csv', header = True, inferSchema = True)

l1 = extreme_pov_df.select(f.collect_list('Poverty')).first()[0]
print(l1)
r1 = l1 + (predict_time_series(l1))
l2 = extreme_pov_df.select(f.collect_list('ExtremePoverty')).first()[0]
print(l2)
r2 = l2 + predict_time_series(l2)

print("R1 ", r1)
print("R2 ", r2)

data = pd.DataFrame({'Year': [x for x in range(1981, 1981+len(r1))], 'Poverty': r1, 'Extreme Poverty': r2})

sns.lineplot(x = 'Year', y = 'Poverty', data = data)
sns.lineplot(x = 'Year', y = 'Extreme Poverty', data = data)

plt.show()
