import unittest

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
__all__ = []

import pkgutil
import inspect
print (sys.path)
print (__all__)

for loader, name, is_pkg in pkgutil.walk_packages(sys.path):
    module = loader.find_module(name).load_module(name)

    for name, value in inspect.getmembers(module):
        if name.startswith('__'):
            continue

        globals()[name] = value
        __all__.append(name)

print (sys.path)
print (__all__)

from tm.tm_1 import *
if __name__ == '__main__':
     unittest.main()
