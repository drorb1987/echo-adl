import pandas as pd

def day_night_clustering(df: pd.DataFrame):
    df['start'] = pd.to_datetime(df['start'])
    df['stop'] = pd.to_datetime(df['stop'])
