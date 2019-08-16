import unittest
import sys

if __name__ == '__main__':
    testsuite = unittest.TestLoader().discover('.')
    result = unittest.TextTestRunner(verbosity=2).run(testsuite)
    sys.exit(not result.wasSuccessful())
