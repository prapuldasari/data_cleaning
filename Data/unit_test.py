import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from folder.tm_threshold import ThresholdTuning
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql import Row
import unittest
# def get_data():
#         list_= []
#         for i in [float(j) / 100 for j in range(0, 100, 1)]:
#             list_.append(i)
#         print (list_)
#         df = spark.createDataFrame(list_, FloatType()).toDF('prop')
#         df = df.withColumn('Target', target_udf(col('prop')).cast(IntegerType()))
#         df = df.withColumn('Prediction', prediction_udf(col('prop')))
#         df = df.withColumn('Target', col('Target').cast(IntegerType()))
#         df = df.withColumn('Prediction', col('Prediction').cast(IntegerType()))
#         df = df.withColumn('prop', col('prop').cast(FloatType()))
#         df.printSchema()
#         return df
# def target_(x):
#     if x> 0.69:
#         return 1
#     elif x==0.62 or x==0.63 or x==0.55 or x==0.56 or x==0.57 or x==0.58 or x==0.59 or x==0.45 or x==0.46 or x==0.39:
#         return 1
#     else:
#         return 0
# target_udf = udf(lambda x : target_(x))
# def prediction_(x):
#     if x> 0.69:
#         return 1
#     elif x==0.62 or x==0.63 or x==0.55 or x==0.56 or x==0.57 or x==0.58 or x==0.59 or x==0.45 or x==0.47 or x==0.39:
#         return 1
#     else:
#         return 0
# prediction_udf = udf(lambda x : prediction_(x))

spark = SparkSession.builder.appName('unit_test').getOrCreate()
# df= spark.read.csv('Data/threshold_test_df.csv', header = True, inferSchema = True)

df= spark.read.csv('/Users/pk/Downloads/threshold_test_data.csv', header = True, inferSchema = True)



class Thresholdunit(unittest.TestCase):
    def __init__(self, spark=spark, Target=None, Probability='prop', prediction_col='prediction'):
        #         self.df= get_data()
        self.threshold = ThresholdTuning(spark=spark, dataframe=df, MaxMisclassification_tolerence=0.04,
                                         expected_FalseAlertReduction=0.4,
                                         buckets=10, MaxMisclassification_tolerence_local=None, NeedComputation=True,
                                         regulater_factor=0.0001, Target='Target', Probability='prop',
                                         recall_limit=0.75, prediction_col='prediction')
        self.df_threshold = self.threshold.get_ProbThrehold_byBadRateDistribution()
        self.df = df
        self.Probability = self.threshold.Probability
        self.Target = self.threshold.Target
        self.MaxMisclassification_tolerence = self.threshold.MaxMisclassification_tolerence

    def values(self):
        assert self.df_threshold.collect()[0]['L1-Threshold'] < self.df_threshold.collect()[0]['L2-Threshold']

    def test_thresholds(self):
        assert self.df_threshold.collect()[0]['L1-Threshold'] == 0.41, "Should be 0.41"
        assert self.df_threshold.collect()[0]['L2-Threshold'] == 0.71, "Should be 0.71"

    def check_values(self):
        assert (df.filter(col(self.Probability) <= self.df_threshold.collect()[0]['L1-Threshold']).count() >= 0.4)
        df_new = self.df.filter(col(self.Probability) <= self.df_threshold.collect()[0]['L1-Threshold'])
        miss_count = df_new.filter(col('Target') == 1).count()
        miss_c = miss_count / self.df.count()
        assert (miss_c <= self.MaxMisclassification_tolerence)

    def all_combined(self):
        self.test_thresholds()
        self.check_values()
        self.values()
