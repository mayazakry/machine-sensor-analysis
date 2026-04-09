"""Tests for preprocessor module."""
import unittest
import pandas as pd
from src.preprocessor import clean_data
class TestPreprocessor(unittest.TestCase):
    def test_clean_data(self):
        df = pd.DataFrame({'a': [1, 2, None, 4]})
        cleaned = clean_data(df)
        self.assertEqual(len(cleaned), 3)