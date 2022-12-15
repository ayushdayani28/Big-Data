'''
Name - SBU ID

Ayush Dayani - 114241407
Sudipto Bal - 113219784
Keerthana Devadas - 113260469
Manoj Shet - 113277283


Description - Code for calculating the pixel intensity using ARIMA for night time light. 

Data Framework - PySpark with Correlation and ARIMA for poverty prediction for 2030, Tensorflow.
System Used - Ubuntu 20.04, Google Cloud DataProc.

Sample data in /data/tmpIN2.csv
'''


import datetime
import pyspark
import sys
import random
import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from scipy.stats import t
import tensorflow as tf
from PIL import Image
PARAM_RANGE = 2

#defining the arima model for time series calulation of the pixel intensities.
def __my_arima(y, param):
    model = ARIMA(y, order=param)
    aic = model.fit()
    return aic


#hyper paramater tuning for ARIMA model for each time series prediction
def predict_time_series(x):
    params = []
    # s_params = []
    if (len(x) < 50):
        return np.average(x)
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
    return (res.get_forecast(steps = 100).predicted_mean)[-1]

def get_id(name):
    split_name = name.split("-")
    id = int(split_name[1]) * 12 + int(split_name[2])
    return id

if __name__ == '__main__':
    sc = pyspark.SparkContext()
    input_path = sys.argv[1]

    input_rdd = sc.textFile(input_path, 32) \
        .map(lambda x: x.split(','))

    #reading the CSV and processing for time series prediction.
    formatted = input_rdd \
        .map(lambda x: (get_id(x[0]), int(x[1]), int(x[2]), float(x[3]))) \
        .map(lambda x: ((x[1], x[2]), [(x[0], x[3])])) \
        .reduceByKey(lambda x, y: x+y) \
        .map(lambda x: (x[0], sorted(x[1]))) \
        .map(lambda x: (x[0], [x[1][i][1] for i in range(len(x[1]))])) \
        .map(lambda x: [x[0], predict_time_series(x[1])]) 

    #collecting the result and forming the new image.
    data = formatted.collect()
    new_img = np.empty((1337, 2228), dtype=float)
    for i in data:
        new_img[i[0]][i[1]] = i[2]

    #storing the new image in .tif format.
    im = Image.fromarray(arr)
    im.save('./resss_img'+input_path[-7:]+'.tif')
