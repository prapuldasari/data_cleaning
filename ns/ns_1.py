import unittest


class ns_1(unittest.TestCase):
    """
    Test the add function from the mymath library
    """

    def test_ns_1_add_integers(self):
        """
        Test that the addition of two integers returns the correct total
        """
        result = 1+2
        self.assertEqual(result, 3)

    def test_ns_1_add_floats(self):
        """
        Test that the addition of two floats returns the correct result
        """
        result = 10.5+2
        self.assertEqual(result, 12.5)

    def test_ns_1_add_strings(self):
        """
        Test the addition of two strings returns the two string as one
        concatenated string
        """
        result = 'abc' + 'def'
        self.assertEqual(result, 'abcdef')

if __name__ == '__main__':
     unittest.main()
