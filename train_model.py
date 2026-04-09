"""Model training script."""
from src.data_loader import load_csv
from src.anomaly_detector import detect_anomalies
if __name__ == '__main__':
    data = load_csv('data/sample_sensor_data.csv')
    anomalies = detect_anomalies(data.iloc[:, 1:])
    print('Anomalies detected:', sum(anomalies == -1))