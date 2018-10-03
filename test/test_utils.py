import unittest
from datetime import datetime, timedelta

from todb.util import seconds_between


class UtilsTest(unittest.TestCase):
    def test_should_measure_seconds_since_event(self):
        start_point = datetime.utcnow() - timedelta(seconds=1.5)
        time_that_passed = seconds_between(start_point)
        self.assertAlmostEqual(time_that_passed, 1.5, delta=0.05)

    def test_should_measure_seconds_between_events(self):
        end_point = datetime.utcnow()
        start_point = end_point - timedelta(seconds=2.5)
        time_that_passed = seconds_between(start_point, end_point)
        self.assertAlmostEqual(time_that_passed, 2.5, delta=0.05)
