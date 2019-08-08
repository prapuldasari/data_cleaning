#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 12:57:18 2019

@author: prapuldasari
"""

# testnig if its triggtring or not
# is it working  #testing2÷÷÷÷÷÷÷÷#######********#########

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
    
    #Checking for the distribution of both the test and train datasets:
    #Here we are using handy spark module for plotting the distribution of each variable for both trianig and test datasets
    #It will take a bit of time depending on the features present
    (trainData, testData) = df.randomSplit([0.7, 0.3])
    hdf1 = trainData.toHandy()
    hdf2 = testData.toHandy()
    columnList = [item[0] for item in df.dtypes if (item[1].startswith('float') or item[1].startswith('double'))]
    for c in columnList:
        fig, axs = plt.subplots(1,2)
        hdf1.cols[c].hist(ax=axs[0])
        hdf2.cols[c].hist(ax=axs[1])
    
    (trainData, testData) = df.randomSplit([0.7, 0.3])
        #Checking for the distribution of all variables over the training and testing datasets
    #We need to convert them into pandas dataframes for plotting them using sns
    trainDatapd= trainData.toPandas()
    testDatapd= testData.toPandas()
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    trainDatapd= trainDatapd.dropna()
    testDatapd= testDatapd.dropna()
    for i,c in enumerate(trainDatapd.select_dtypes(include=numerics).columns):
        plt.figure(i)
        sns.distplot(trainDatapd[c])
        sns.distplot(testDatapd[c]) 

    #Checking for the distribution of the target varaible for categorical variables this shows the plots for both training and testing datasets
    for c in checkCateg['check_categ']:
        print ('Distribution of target variables classes over {}'. format(c))
        testDatapd1= testDatapd.groupby(target_variable)[c].value_counts()
        datatest = testDatapd1.to_frame(name='number')
        datatest = datatest.reset_index()
        datatest1=datatest[datatest[target_variable]==1]
        datatest0=datatest[datatest[target_variable]==0]
        datatest1 = pd.pivot_table(datatest1, values='number',index=target_variable ,columns=c)
        datatest0 = pd.pivot_table(datatest0, values='number',index=target_variable ,columns=c)
        datatest1.plot(kind= 'bar', ax= plt.subplot(2,2,1));
        plt.title('{}- {}1'.format(c, target_variable));
        plt.legend(loc=2, prop={'size': 6})
        datatest0.plot(kind= 'bar', ax= plt.subplot(2,2,2));
        plt.title('{}- {}0'.format(c, target_variable));
        plt.legend(loc=2, prop={'size': 6})
        trainDatapd1= trainDatapd.groupby(target_variable)[c].value_counts()
        datatrain = trainDatapd1.to_frame(name='number')
        datatrain = datatrain.reset_index()
        datatrain1=datatrain[datatrain.Class==1]
        datatrain0=datatrain[datatrain.Class==0]
        datatrain1 = pd.pivot_table(datatrain1, values='number',index=target_variable,columns=c)
        datatrain0 = pd.pivot_table(datatrain0, values='number',index=target_variable,columns=c)
        datatrain1.plot(kind= 'bar', ax= plt.subplot(2,2,3));
        plt.title('{}- {}1'.format(c, target_variable));
        plt.legend(loc=2, prop={'size': 6})
        datatrain0.plot(kind= 'bar', ax= plt.subplot(2,2,4));
        plt.title('{}- {}1'.format(c, target_variable));
        plt.subplots_adjust(wspace=0.3, hspace= 0.7)
        plt.legend(loc=2, prop={'size': 6})
        plt.show()

    
    
