import unittest


class TestAdd4(unittest.TestCase):
    """
    Test the add function from the mymath library
    """

    def test_add_integers4(self):
        """
        Test that the addition of two integers returns the correct total
        """
        result = 1+2
        self.assertEqual(result, 3)

    def test_add_floats4(self):
        """
        Test that the addition of two floats returns the correct result
        """
        result = 10.5+2
        self.assertEqual(result, 12.5)

    def test_add_strings4(self):
        """
        Test the addition of two strings returns the two string as one
        concatenated string
        """
        result = 'abc' + 'defgch'
        self.assertEqual(result, 'abcdef')
       
       
       
class TestAdd5(unittest.TestCase):
    """
    Test the add function from the mymath library
    """

    def test_add_integers5(self):
        """
        Test that the addition of two integers returns the correct total
        """
        result = 1+2
        self.assertEqual(result, 3)

    def test_add_floats5(self):
        """
        Test that the addition of two floats returns the correct result
        """
        result = 10.5+2
        self.assertEqual(result, 12.5)

    def test_add_strings5(self):
        """
        Test the addition of two strings returns the two string as one
        concatenated string
        """
        result = 'abc' + 'def'
        self.assertEqual(result, 'abcdef')

