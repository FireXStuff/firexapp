#
# Most of code can be executed via normal Python unit testing frameworks like unittest and pytest.
#
import unittest


from firexapp.tasks.example import greet


class GreetTests(unittest.TestCase):

    def test_greet_non_default(self):
        greeting = greet.undecorated(name='John')
        self.assertEqual("Hello John!", greeting)
