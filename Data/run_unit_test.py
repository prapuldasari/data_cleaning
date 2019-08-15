import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) 

from unit_test import Thresholdunit

class UnitTest(object):

    def run(self):
        print ('new files are being tested here th')
        t1 = Thresholdunit()
        t1.all_combined()

if __name__ == '__main__': 
    ut= UnitTest()
    ut.run()
