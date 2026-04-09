"""Tests for data_loader module."""
import unittest
from src.data_loader import load_csv
class TestDataLoader(unittest.TestCase):
    def test_load_csv(self):
        data = load_csv('data/sample_sensor_data.csv')
        self.assertGreater(len(data), 0)