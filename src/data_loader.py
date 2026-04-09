"""Data loading module for sensor data.
"""\nimport pandas as pd\ndef load_csv(filepath):\n    return pd.read_csv(filepath)