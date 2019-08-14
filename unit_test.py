from tm_thresholding import ThresholdTuning
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import FloatType

spark = SparkSession.builder.appName('testing').getOrCreate()
df= spark.read.csv('Testing_df - Sheet1.csv', header = True, inferSchema = True)
print ('now in master new chnage')


###testing
# def get_data():
#     list_= []
#     for i in [float(j) / 100 for j in range(0, 100, 1)]:
#         list_.append(i) 
#     df = spark.createDataFrame(list_, FloatType()).toDF('prop')
#     df = df.withColumn('Target', target_udf(col('prop')))
#     df = df.withColumn('Prediction', prediction_udf(col('prop')))
#     return df
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


class Thresholdunit(object):
    def __init__(self,spark= spark,Target= None ,Probability = 'Prediction_Prob_1', prediction_col = 'prediction'):
#         self.df= get_data()
        self.threshold = ThresholdTuning(spark = spark,dataframe=df,MaxMisclassification_tolerence=0.04, expected_FalseAlertReduction = 0.4, 
                buckets=10, MaxMisclassification_tolerence_local = None, NeedComputation = True, regulater_factor=0.0001,Target= 'Target',Probability = 'prop', recall_limit= 0.75, prediction_col = 'prediction' )
        
    def test_thresholds(self):
        df_threshold =self.threshold.get_ProbThrehold_byBadRateDistribution()
        assert df_threshold.collect()[0]['L1-Threshold'] == 0.41, "Should be 0.31"
        assert df_threshold.collect()[0]['L2-Threshold'] == 0.71, "Should be 0.71"


if __name__ == "__main__":
    test_class =  Thresholdunit()
    test_class.test_thresholds()
    print("Everything passed")
