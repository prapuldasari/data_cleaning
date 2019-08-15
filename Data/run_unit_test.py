from unit_test import Thresholdunit

class UnitTest(object):

    def run(self):
        print ('new files are being tested')
        t1 = Thresholdunit()
        t1.all_combined()

if __name__ == '__main__': 
    ut= UnitTest()
    ut.run()
