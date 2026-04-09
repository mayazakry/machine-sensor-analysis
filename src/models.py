# Machine Learning Model Definitions

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

class LogisticRegressionModel:
    def __init__(self, **kwargs):
        self.model = LogisticRegression(**kwargs)

    def fit(self, X, y):
        self.model.fit(X, y)

    def predict(self, X):
        return self.model.predict(X)

class RandomForestModel:
    def __init__(self, **kwargs):
        self.model = RandomForestClassifier(**kwargs)

    def fit(self, X, y):
        self.model.fit(X, y)

    def predict(self, X):
        return self.model.predict(X)

class SupportVectorModel:
    def __init__(self, **kwargs):
        self.model = SVC(**kwargs)

    def fit(self, X, y):
        self.model.fit(X, y)

    def predict(self, X):
        return self.model.predict(X)