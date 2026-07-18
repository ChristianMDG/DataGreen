import pandas as pd
import json
import os
import glob
from datetime import datetime

CLEAN_PATH = "/opt/airflow/data/clean"
CHECK_PATH = "/opt/airflow/data/quality_checks"

def load_latest():
    files = glob.glob(f"{CLEAN_PATH}/**/*.parquet", recursive=True)
    if not files:
        print("No files")
        return None
    latest = max(files, key=os.path.getctime)
    df = pd.read_parquet(latest)
    print(f"Loaded {len(df)} records")
    return df

def temporal_check(df):
    if df.empty:
        return {}
    return {
        'date_min': str(df['date'].min()),
        'date_max': str(df['date'].max()),
        'hours': sorted(df['hour'].unique()),
        'missing_hours': sorted(set(range(24)) - set(df['hour'].unique()))
    }

def drift_check(df):
    if df.empty or len(df) < 2:
        return {'drift': False}
    df['date'] = pd.to_datetime(df['date'])
    daily = df.groupby(df['date'].dt.date)['aqi'].mean()
    if len(daily) < 2:
        return {'drift': False}
    change = daily.pct_change().mean() * 100
    return {'drift': abs(change) > 20, 'change': round(change, 2)}

def alerts(df):
    alerts = []
    if df.empty:
        return alerts
    if df['aqi'].max() >= 4:
        alerts.append(f"High AQI: {df['aqi'].max()}")
    if df['pm2_5'].max() > 50:
        alerts.append(f"High PM2.5: {df['pm2_5'].max()}")
    if df['pm10'].max() > 100:
        alerts.append(f"High PM10: {df['pm10'].max()}")
    return alerts

def main():
    os.makedirs(CHECK_PATH, exist_ok=True)
    df = load_latest()
    if df is None:
        return
    results = {
        'timestamp': datetime.now().isoformat(),
        'total': len(df),
        'cities': df['city'].unique().tolist(),
        'temporal': temporal_check(df),
        'drift': drift_check(df),
        'alerts': alerts(df)
    }
    p = f"{CHECK_PATH}/check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(p, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Check saved to {p}")
    if results['alerts']:
        print("Alerts:")
        for a in results['alerts']:
            print(f"  - {a}")

if __name__ == "__main__":
    main()
