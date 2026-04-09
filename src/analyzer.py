"""Data analysis and statistics module."""
import numpy as np
def calculate_stats(data):
    return {'mean': np.mean(data), 'std': np.std(data)}