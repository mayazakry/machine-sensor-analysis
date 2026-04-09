"""Data loading module for sensor data."""
import pandas as pd

def load_csv(filepath):
    return pd.read_csv(filepath)