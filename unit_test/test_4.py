from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql import Row
import unittest
import sys
import os

try:
    from tm_thresholding import ThresholdTuning
except:
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from folder.tm_thresholding import ThresholdTuning


spark = SparkSession.builder.appName('unit_test').getOrCreate()
df = spark.read.csv('/Users/pk/Downloads/threshold_test_data.csv', header=True, inferSchema=True)


class TestAdd6(unittest.TestCase):
    def test_init(self,Target=None, Probability='prop', prediction_col='prediction'):
        #         self.df= get_data()
        
        
        self.spark = SparkSession.builder.appName('unit_test').getOrCreate()
        self.threshold = ThresholdTuning(spark=self.spark, dataframe=df, MaxMisclassification_tolerence=0.04,
                                         expected_FalseAlertReduction=0.4,
                                         buckets=10, MaxMisclassification_tolerence_local=None, NeedComputation=True,
                                         regulater_factor=0.0001, Target='Target', Probability='prop',
                                         recall_limit=0.75, prediction_col='prediction')
        print ('---debug')
        print (self.spark)
        self.df_threshold = self.threshold.get_ProbThrehold_byBadRateDistribution()
        self.df = df
        self.Probability = self.threshold.Probability
        self.Target = self.threshold.Target
        self.MaxMisclassification_tolerence = self.threshold.MaxMisclassification_tolerence

    def test_values(self):
        self.assertTrue(self.df_threshold.collect()[0]['L1-Threshold'] < self.df_threshold.collect()[0]['L2-Threshold'])

    def test_thresholds(self):
        self.assertTrue(self.df_threshold.collect()[0]['L1-Threshold'] == 0.41, "Should be 0.41")
        self.assertTrue(self.df_threshold.collect()[0]['L2-Threshold'] == 0.71, "Should be 0.71")

    def test_check_values(self):
        self.assertTrue((df.filter(col(self.Probability) <= self.df_threshold.collect()[0]['L1-Threshold']).count() >= 0.4))
        df_new = self.df.filter(col(self.Probability) <= self.df_threshold.collect()[0]['L1-Threshold'])
        miss_count = df_new.filter(col('Target') == 1).count()
        miss_c = miss_count / self.df.count()
        self.assertTrue(miss_c <= self.MaxMisclassification_tolerence)
