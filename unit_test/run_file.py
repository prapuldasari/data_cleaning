import unittest
import sys

if __name__ == '__main__':
    testsuite = unittest.TestLoader().discover('.')
    unittest.TextTestRunner(verbosity=2, failfast= True).run(testsuite)
    sys.exit(not result.wasSuccessful())
