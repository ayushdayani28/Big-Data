from cProfile import label
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

import numpy as np
import pyspark
from scipy.stats import t
PARAM_RANGE = 2

def __my_arima(y, param):
    model = ARIMA(y, order=param)
    aic = model.fit()
    return aic

def predict_time_series(x, step):
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
    return (res.get_forecast(steps = step).predicted_mean).tolist()


if __name__=='__main__':
    epoverty_file = sys.argv[1]
    epoverty_file1 = sys.argv[2]
    gdp_per_capita_file= sys.argv[3]
    life_expectancy_file = sys.argv[4]
    child_mortality_file = sys.argv[5]
    maternal_mortality_file = sys.argv[6]
    malnutrition_death_file = sys.argv[7]
    school_years_file = sys.argv[8]
    unsafe_sanitation_file = sys.argv[9]
    fertility_file = sys.argv[10]

    sc = pyspark.SparkContext()
    ss = SparkSession.builder.appName('Poverty factors').getOrCreate()

    extreme_pov_df = ss.read.csv(epoverty_file, header = True, inferSchema = True)
    poverty_gap_df = ss.read.csv(epoverty_file1, header = True, inferSchema = True)
    gdp_capita_df = ss.read.csv(gdp_per_capita_file, header = True, inferSchema = True)
    child_mortality_df = ss.read.csv(child_mortality_file, header = True, inferSchema = True)
    life_exp_df = ss.read.csv(life_expectancy_file, header = True, inferSchema = True) 
    maternal_mortality_df = ss.read.csv(maternal_mortality_file, header = True, inferSchema = True)
    malnutrition_death_df = ss.read.csv(malnutrition_death_file, header = True, inferSchema = True)
    school_years_df = ss.read.csv(school_years_file, header = True, inferSchema = True)
    unsafe_sanitation_df = ss.read.csv(unsafe_sanitation_file, header = True, inferSchema = True)
    fertility_df = ss.read.csv(fertility_file, header = True, inferSchema = True) \
                            .drop(*['MeanSchooling', 'Continent'])

    print(fertility_df.take(5), '\n\n')

    joined_df = extreme_pov_df.join(poverty_gap_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(gdp_capita_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(life_exp_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(child_mortality_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(maternal_mortality_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(malnutrition_death_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(school_years_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(unsafe_sanitation_df, ['Entity', 'Code', 'Year'], "outer") \
                            .join(fertility_df, ['Entity', 'Code', 'Year'], "outer")
                            
    
    joined_rdd = joined_df.filter(joined_df["Year"] > 1990) \
                            .filter(joined_df["Code"] != "")

    print(joined_rdd.count())

    df1 = joined_rdd.na.drop()

    print(df1.count())

    # corr_df = df1.drop(*['Entity', 'Code'])
    # vector_col = "corr_features"
    # assembler = VectorAssembler(inputCols=corr_df.columns, outputCol=vector_col)
    # df_vector = assembler.transform(corr_df).select(vector_col)

    # # get correlation matrix
    # matrix = Correlation.corr(df_vector, vector_col)

    # corr_vals = (matrix.collect()[0][0]).toArray()
    # print(corr_vals)
    # # print(corr_vals[0])
    # # print(corr_vals[1]) 
    # print(len(corr_vals))
    # # print(corr_vals[1])
    # print('\n\n')
    # mask = np.zeros_like(corr_vals)
    # mask[np.triu_indices_from(mask)] = True
    # ax = sns.heatmap(corr_vals, linewidth=0.5, vmax=.3, square=True,  cmap="YlGnBu", mask=mask, annot=True)
    
    # ax.set_yticks(np.arange(0.5, len(corr_df.columns), 1))
    # ax.set_xticks(np.arange(0.5, len(corr_df.columns), 1))
    # ax.set_xticklabels(corr_df.columns, rotation=90)
    # ax.set_yticklabels(corr_df.columns,fontsize=10, rotation=0)
    # plt.show()


    countries = ['India']
    for c in countries:
        countryDf = df1.filter(df1['Entity'] == c).sort('Year')
        years = countryDf.select(f.collect_list('Year')).first()[0]
        min, max = min(years), max(years)
        for i in range(max+1, 2031):
            years.append(i)
        
        print(countryDf)
        l1 = countryDf.select(f.collect_list('Poverty')).first()[0]
        print(l1)
        r1 = l1 + predict_time_series(l1, 2030-max)
        l2 = countryDf.select(f.collect_list('ExtremePoverty')).first()[0]
        print(l2)
        r2 = l2 + predict_time_series(l2, 2030-max)
        l3 = countryDf.select(f.collect_list('GDPPerCapita')).first()[0]
        r3 = l2 + predict_time_series(l3, 2030-max)

        data = pd.DataFrame({'Year': years, 'Poverty': r1, 'Extreme Poverty': r2, 'GDP per capita': r3})

        sns.lineplot(x = 'Year', y = 'Poverty', data = data, label = 'Poverty')
        sns.lineplot(x = 'Year', y = 'Extreme Poverty', data = data, label = 'Extreme Poverty')
        # sns.lineplot(x = 'Year', y = 'GDP per capita', data = data, label = 'GDP per capita')
        plt.title("Country: " + c)
        plt.show()
        





