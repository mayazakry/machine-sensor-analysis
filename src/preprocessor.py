"""Data preprocessing module."""
def clean_data(df):
    df = df.dropna()
    return df