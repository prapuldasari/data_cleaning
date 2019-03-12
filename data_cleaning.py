#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 12:57:18 2019

@author: prapuldasari
"""

import findspark
findspark.init()
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
import time 
from pyspark.sql import functions as f
from functools import reduce
from pyspark.sql.types import (StructField, StringType, StructType,IntegerType, FloatType, TimestampType)
from pyspark.sql.functions import isnan, when, count, substring, length, col, expr, udf, isnan, lit, lower, unix_timestamp, expr, datediff, to_date, broadcast
from pyspark.sql import DataFrame
from pyspark.mllib.stat import Statistics
import json

toFloat= {'to_float': {'V1', 'V2', 'V3','V4', 'V5', 'V6','V7', 'V8', 'V9', 'V10', 'V11', 'V12', 'V13','V14', 'V15', 'V16','V17', 'V18', 'V19', 'V20', 'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28', 'Amount', 'Time', 'Class'}}
impCol= {'imp_Col': {'V1', 'V2', 'Time', 'Class'}}
outlierCheck= {'outlier_check' : {'V1', 'V2'}}
targetEncode= {'target_enocde' : {'Test'}}
checkCateg= {'check_categ': {'Test1', 'Test'}}
target_variable= 'Class'
strip_string= 'Test2'
        
def data_stats(df, target_variable, strip_string, toFloat, impCol, outlierCheck, targetEncode, to_Drop, null_threshold,outlier_threshold, toPlot):
    print ('Data Schema')
    df.printSchema()
    print ('Taking the five entries in the dataset')
    '''print (pd.DataFrame(df.take(5), columns=df.columns).transpose())'''
    display (pd.DataFrame(df.take(5), columns=df.columns).transpose())
    print ('Stats for the dataframe')
    '''print (df.describe().toPandas())'''
    display (df.describe().toPandas()) #we can also just use show function for getting the output in pyspark dataframe format
    print ('Target variable frequency')
    df.groupBy(target_variable).count().show()

    def stripString(x):
        if(pd.notnull(x)):
            return x.strip()
        else:
            return x
    udf_string_strip = udf(lambda x: stripString(x), StringType())
    stripping_columns= [item[0] for item in df.dtypes if item[1].startswith('string')]
    for i in stripping_columns:
        df= df.withColumn(i,udf_string_strip(col(i)))
    
    '''#Check if you need to strip any unwanted things in a column:
    #this one has to be changed accordinly to the stripping value here its for removing the larst five characters in the string
    #Need to change the sunstring part according to reqirement
    df = df.withColumn(strip_string,expr("substring(Test1, 1, length(Test1)-5)"))'''

    #Changing the data types to float accordingly
    for c in toFloat['to_float']:
        df= df.withColumn(c, (col(c).cast('float')))
    df.printSchema()

    #Checnking for the percentage of missing values in the important column
    for c in impCol['imp_Col']:
        df.select([(count(when(isnan(c), c)/df.count())).alias(c)]).show()
    #Better Model   
    nullColumns = []
    numRows = df.count()
    for k in df.columns:
      nullRows = df.where(col(k).isNull()).count()
      if (nullRows/numRows >0.70): # i.e. if ALL values are NULL
        nullColumns.append(k)
        print ('The feature {} has more than 70 missing values'.format(k))
    print ('Lets drop these rows')
    reduce(DataFrame.drop, nullColumns, df)

    #function for checking if important  column in the nullColumns:
    for i in impCol['imp_Col']:
        if (i in nullColumns):
            print ('The important feature is having more missing values')
        print ('We are having the required dataset')
    
    #checking for the outliers present in the dataset we pass the columns for which we wnat to check
    def outlier(name1, mean1, std):
        k= (name1-mean1)/std
        return k
    udf_outlier= udf(lambda x:outlier(x, mean1, std),FloatType())

    for i in outlierCheck['outlier_check']:
        print (i)
        from pyspark.sql.functions import mean, stddev
        df_stats = df.select(mean(col(i)).alias('mean'),stddev(col(i)).alias('std')).collect()
        mean1 = df_stats[0]['mean']
        std = df_stats[0]['std']
        print (mean1)
        print (std)
        #df_out= df.withColumn('test', udf_testing_2(col(c)))
        #df_out= df_out.filter(col('test')>3)
        df= df.withColumn('outlier_1', udf_outlier(col(i)))
        print ('Before removing {} '.format(df.count()))
        print ('The outliers presetn in the {} column is {}' .format(i,df.filter(col('outlier_1')>3).count()))
        df= df.filter(col('outlier_1')<3)
        #print (df.count())         
    df= df.drop('outlier_1')
    df.take(1)

    #Target encoding weighted takes columns to be targetr encoded and also the target variable name
    # For getting correlation between targert variables and independent variables
    for i in targetEncode['target_enocde']:
        print (i)
        weight = df.groupby(i).agg(f.log((f.sum(target_variable)+1)/(f.count(target_variable) - f.sum(target_variable))).alias("new_"+i))
        df = df.join(broadcast(weight), on= i)
        #df = df.join(broadcast(weight), on= i).drop(i) good to drop but for further use in determining the target varible distribution we are not dropping
        #this needs to be redone working only for one item in the dict
       
    def compute_correlation_matrix(df, method='pearson'):
        columns=[item[0] for item in df.dtypes if (item[1].startswith('float') or item[1].startswith('double'))]#need to work according to the datatypes
        df_filter=df.select(columns)
        df_rdd = df_filter.rdd.map(lambda row: row[0:])
        corr_mat = Statistics.corr(df_rdd, method=method)
        corr_mat_df = pd.DataFrame(corr_mat,columns=df_filter.columns,index=df_filter.columns)
        return corr_mat_df
    l=compute_correlation_matrix(df)
    indexes = np.where(l > 0.7)
    indexes = [(l.index[x], df.columns[y]) for x, y in zip(*indexes)if x != y and x < y]
    print ("The features which are having correlation are {}".format(indexes))
    print ('Heatmap for the correlations')
    sns.heatmap(l) #gives the plot if required