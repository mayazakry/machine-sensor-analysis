import pandas as pd


def clean_data(df):
    # Remove duplicates
    df = df.drop_duplicates()
    # Fill missing values
    df = df.fillna(method='ffill')
    return df


def preprocess_features(df):
    # Normalize numerical features
    num_cols = df.select_dtypes(include=['float64', 'int']).columns
    df[num_cols] = (df[num_cols] - df[num_cols].mean()) / df[num_cols].std()
    return df
