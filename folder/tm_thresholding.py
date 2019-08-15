
import math
from pyspark.sql.functions import col, udf, when, lit
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt

class ThresholdTuning(object):
    def __init__(self,spark= None, dataframe= None, MaxMisclassification_tolerence=0.05, expected_FalseAlertReduction = 0.4,buckets=10, MaxMisclassification_tolerence_local = None, NeedComputation = True, regulater_factor=0.0001,Target= None ,Probability = 'Prediction_Prob_1', prediction_col = 'prediction', recall_limit= 0.75):
        self.df= dataframe
        self.prediction_col = prediction_col
        self.df= self.df.filter((col(self.prediction_col)==0) | (col(self.prediction_col)==1))
        self.df= self.df.dropna()
        self.MaxMisclassification_tolerence = MaxMisclassification_tolerence
        self.expected_FalseAlertReduction = expected_FalseAlertReduction
        self.buckets = buckets
        self.MaxMisclassification_tolerence_local = MaxMisclassification_tolerence_local
        self.NeedComputation = NeedComputation
        self.regulater_factor = regulater_factor
        self.Target= Target
        self.Probability= Probability
        self.spark= spark
        self.recall_limit= recall_limit
        #Data_Processing
        self.df_new = self.df.select(self.Target, self.Probability)
        self.df_new.rdd.getNumPartitions()
        self.df_new = self.df_new.repartition(1)
        self.df_new.rdd.getNumPartitions()
        y_test= [i[0] for i in self.df_new.select(self.Target).collect()]
        list_pro= [i[0] for i in self.df_new.select(self.Probability).collect()]
        self.Probs_1 = np.array(list_pro)
        self.True_Alert_labels = np.array(y_test)
        
        #L1_Threshold_Parameters:
        self.bad_recall_list = []
        self.local_bad_recall_list = []
        self.pre_local_bad_recall = 0
        self.best_threshold = 0
        self.total_accum = 0
        self.L1_Thresholds = []
        self.L2_Thresholds = []
        
        #L2_Threshold_Parameters:
        self.good_recall_ctotal = 0
        self.total_accum2 = 0
        self.previous_bucket_recall = 0
        self.Recalls_bucket_reverse= {}
        self.Local_recall_reverse = []

    def get_ProbThrehold_byBadRateDistribution(self):
        '''
        :param Probs_1: probability array for label 1 (True alert)
        :param True_Alert_labels: the array of True labels
        :param MaxfMisclassification_tolerence: the max tolerence of misclarification, which depends on the probability threhold for
        defining true or false alert
        :param expected_FalseAlertReduction: the expected or wished False Alert Reduction
        :param buckets: num of probability bins
        :param MaxfMisclassification_tolerence_local: the local max tolerence of misclarification
        :param NeedComputation: handy parameter
        :param regulater_factor: avoid the denominator = 0
        :return: the threshold defined
        Here good = Non Issue (False) Alert, bad = True Alert
        '''
        assert len(self.Probs_1)==len(self.True_Alert_labels)
        assert len(self.Probs_1)>=self.buckets
        Total_TrueAlerts = sum(self.True_Alert_labels)
        Total_FalseAlerts = len(self.True_Alert_labels) - Total_TrueAlerts
        print ('Total_TrueAlerts--{}' .format(Total_TrueAlerts))
        expected_FalseAlertReduction_number = self.expected_FalseAlertReduction * Total_FalseAlerts
        MaxMisclassification_tolerence_number = self.MaxMisclassification_tolerence*Total_TrueAlerts
        '''
        baseline_badRecall: e.g. 40% reduction, with 2.8% misclassification. 
        then For the L1, the good bad ratio =  2.8%*totalTrueAlert / (40%*totalFalseAlerts+2.8%*totalTrueAlert)
        '''
        if self.MaxMisclassification_tolerence_local is None:
            baseline_badRecall = expected_FalseAlertReduction_number/(MaxMisclassification_tolerence_number+expected_FalseAlertReduction_number)
            self.MaxMisclassification_tolerence_local = baseline_badRecall
        print ('MaxMisclassification_tolerence_local : {}' .format(self.MaxMisclassification_tolerence_local))
        Probs_1_sorted = sorted(self.Probs_1)
        length_prob = len(Probs_1_sorted)
        bucket_indexes = [(float(i)/self.buckets)*length_prob for i in range(self.buckets+1)]
        bucket_indexes[-1] = length_prob-1
        prob_cutOffs = [Probs_1_sorted[int(i)] for i in bucket_indexes]
        print (prob_cutOffs)
        
        #L1-Thresholding:
        for c_lower, c_upper in zip(prob_cutOffs[:-1], prob_cutOffs[1:]):
            indexes = (self.Probs_1>=c_lower) & (self.Probs_1<c_upper)
            Probs_c = self.Probs_1[indexes]
            Labels_c = self.True_Alert_labels[indexes]
            self.total_accum += len(Labels_c)
            total_trueAlerts_c = sum(Labels_c)
            total_falseAlerts_c = len(Labels_c)-total_trueAlerts_c
            bad_recall_c = float(total_trueAlerts_c)/Total_TrueAlerts
            self.bad_recall_list.append(bad_recall_c)
            local_bad_recall = float(total_trueAlerts_c) /(len(Labels_c)+self.regulater_factor)
            local_bad_recall_delta = local_bad_recall - self.pre_local_bad_recall
            self.pre_local_bad_recall = local_bad_recall_delta
            self.local_bad_recall_list.append(local_bad_recall)
            accumulate_bad_recall = sum(self.bad_recall_list)
            if self.NeedComputation:
                if accumulate_bad_recall>=self.MaxMisclassification_tolerence:
                    '''
                    the accurate_bad_recall can not exceed the max tolerence 
                    Monitor the good/bad ratio, aiming to find a optimal point by its distribution comparing against 
                    the overal expected ratio
                    '''
                    print ('condition 1')
                    self.best_threshold = c_lower
                    MisclassificationRate = accumulate_bad_recall-bad_recall_c
                    total_reduction = self.total_accum - len(Labels_c)
                    self.NeedComputation = False

                elif local_bad_recall_delta>self.MaxMisclassification_tolerence_local:
                    print ('condition 2')
                    self.best_threshold = c_lower
                    MisclassificationRate = accumulate_bad_recall-bad_recall_c
                    total_reduction = self.total_accum - len(Labels_c)
                    self.NeedComputation = False
        print ('Result ------\n')
        print ('Best_L1_Threshold : {}' .format(self.best_threshold))
        print ('MissclassificationRate : {}'.format(MisclassificationRate))
        print ('Total Reduction : {}'.format(float(total_reduction)/length_prob))
        # plt.plot(prob_cutOffs[:-1], self.local_bad_recall_list,label='local_bad_recall')
        # plt.axvline(self.best_threshold, label='threshold', c='r')
        # plt.xticks(np.round(prob_cutOffs, 3))
        # plt.ylabel('percentage')
        # plt.legend()

        #L2_Thresholding:
        prob_cutOffsr = prob_cutOffs
        prob_cutOffsr.reverse()
        for c_lower, c_upper in zip(prob_cutOffsr[1:], prob_cutOffsr[:-1]):
            indexes = (self.Probs_1>=c_lower) & (self.Probs_1<c_upper)
            Probs_c = self.Probs_1[indexes]
            Labels_c = self.True_Alert_labels[indexes]
            total_trueAlerts_c = sum(Labels_c)
            total_falseAlerts_c = len(Labels_c)-total_trueAlerts_c
            good_recall_c = float(total_trueAlerts_c)
            local_recall = good_recall_c/(len(Labels_c)+self.regulater_factor)
            self.total_accum2 += len(Labels_c)
            self.good_recall_ctotal += good_recall_c
            overall_recall = self.good_recall_ctotal/self.total_accum2

            self.Recalls_bucket_reverse.update({overall_recall : (c_lower, c_upper)})
            if (overall_recall < self.recall_limit*self.previous_bucket_recall) or (overall_recall > self.previous_bucket_recall) or (overall_recall < self.recall_limit):
                if (overall_recall < self.recall_limit):
                    print ('Condition1')
                    L3 = c_upper
                    break
                elif (overall_recall < self.recall_limit*self.previous_bucket_recall):
                    print ('Condition2')
                    L3 = c_upper
                elif (overall_recall > self.previous_bucket_recall):
                    print ('Condition3')
                    L3 = c_lower
            self.previous_bucket_recall = overall_recall
        if overall_recall> self.recall_limit:
            print ('The overall recall is always greater than the given recall limit')
            print ('Taking the immediate bucket as the L3 threshold')
            prob_cutOffs.sort()
            L3 = [i for i in prob_cutOffs if i > self.best_threshold][0]
            print (L3)
        if (L3 == max(prob_cutOffsr)) or (L3<= self.best_threshold):
            print ('Hard Constraints')
            L3= min(self.Recalls_bucket_reverse[max(self.Recalls_bucket_reverse)])
        print ('L2_Threshold : {}' .format(L3))
        print ('L1_Threshold : {}' .format(self.best_threshold))
        # plt.axvline(L3, label='threshold', c='r')
        # plt.show()
        # self.L1_Thresholds.append(round(self.best_threshold, 2))
        # self.L2_Thresholds.append(round(L3,2))
        self.L1_Thresholds.append(self.best_threshold)
        self.L2_Thresholds.append(L3)
        #Creating DataFrame
        print (self.L1_Thresholds, self.L2_Thresholds)
        dataset = pd.DataFrame({'L1-Threshold':self.L1_Thresholds,'L2-Threshold':self.L2_Thresholds})
        df_threshold = self.spark.createDataFrame(dataset)
        return df_threshold


class statistics(object):
    def __init__(self, spark=None, train_df=None, test_df=None, target=None, fraction=0.75):
        self.train_df = train_df
        self.test_df = test_df
        self.target = target
        self.fraction = fraction
        self.one_s = []
        self.zero_s = []

    def statistics_df(self):
        print ('The train dataset target varibale counts are as:')
        self.train_df.groupBy(self.target).count().show()
        train_df1 = self.train_df.groupBy(self.target).count().rdd.collectAsMap()
        s = sum(train_df1.values())
        for k, v in train_df1.items():
            pct = v * 100.0 / s
            print(k, pct)
            if k == 0:
                self.zero_s.append(pct)
            else:
                self.one_s.append(pct)

        print ('The test dataset target varibale counts are as:')
        self.test_df.groupBy(self.target).count().show()
        test_df1 = self.test_df.groupBy(self.target).count().rdd.collectAsMap()
        s = sum(test_df1.values())
        for k, v in test_df1.items():
            pct = v * 100.0 / s
            print(k, pct)
            if k == 0:
                self.zero_s.append(pct)
            else:
                self.one_s.append(pct)
        print ('The whole dataset target varibale counts are as:')
        # df.groupBy(target).count().show()
        print ("Target 0's % in whole data {}".format(sum(self.zero_s) / 2))
        print ("Target 1's % in whole data {}".format(sum(self.one_s) / 2))

        # sampled_train = df_train.sampleBy(target, fractions={0:sum(zero_s)/200 , 1:self.fraction}, seed=0)
        # sampled_test = df_test.sampleBy(target, fractions={0:sum(zero_s)/200 , 1:self.fraction}, seed=0)
        # fractions = df.select(target).distinct().withColumn("fraction", lit(0.6)).rdd.collectAsMap()
        # print('fractions is {}'.format(fractions))
        # seed = random.randint(1, 100)
        # sampled_df = df.stat.sampleBy(target, fractions, seed)
        # sampled_df.groupBy(target).count().show()
        # test_df = df.exceptAll(sampled_df)
        # test_df.groupBy(target).count().show()

        return self.train_df, self.test_df

