import numpy as np
from sklearn.ensemble import IsolationForest

class AnomalyDetector:
    def __init__(self, contamination=0.05):
        self.contamination = contamination
        self.model = IsolationForest(contamination=self.contamination)

    def fit(self, X):
        self.model.fit(X)

    def predict(self, X):
        return self.model.predict(X)  # Returns 1 for normal, -1 for anomalies

    def fit_predict(self, X):
        return self.model.fit_predict(X)  # Returns 1 for normal, -1 for anomalies

    def get_anomalies(self, X):
        predictions = self.predict(X)
        return X[predictions == -1]