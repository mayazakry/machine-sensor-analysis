"""Data preprocessing script."""
from src.data_loader import load_csv
from src.preprocessor import clean_data
if __name__ == '__main__':
    data = load_csv('data/sample_sensor_data.csv')
    cleaned = clean_data(data)
    print('Data preprocessing complete!')