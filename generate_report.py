"""Report generation script."
from src.data_loader import load_csv
from src.analyzer import calculate_stats
if __name__ == '__main__':
    data = load_csv('data/sample_sensor_data.csv')
    stats = calculate_stats(data.iloc[:, 1:])
    print('Report generated with statistics:', stats)