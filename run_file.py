import unittest
__all__ = []
import pkgutil
import inspect
import sys
for loader, name, is_pkg in pkgutil.walk_packages(['/Users/pk/.jenkins/workspace/testing_sh/tm', '/Users/pk/.jenkins/workspace/testing_sh/ns']):
    module = loader.find_module(name).load_module(name)
    for name, value in inspect.getmembers(module):
        if name.startswith('__'):
            continue
        globals()[name] = value
        __all__.append(name)
if __name__ == '__main__':
     unittest.main(verbosity= 2)
# **
