import pandas as pd
import json
import os
import glob
from datetime import datetime

p_raw = "/opt/airflow/data/raw"
p_clean = "/opt/airflow/data/clean"
p_quality = "/opt/airflow/data/quality_reports"

def read_json():
    out = []
    for f in glob.glob(f"{p_raw}/**/*.json", recursive=True):
        with open(f) as file:
            d = json.load(file)
            if 'list' in d:
                city = d.get('city', 'unknown')
                for item in d['list']:
                    item['city'] = city
                    out.append(item)
    return out

def transform_basic(raw):
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df['date'] = df['dt'].dt.date
    df['hour'] = df['dt'].dt.hour
    df['aqi'] = df['main'].apply(lambda x: x['aqi'] if isinstance(x, dict) else None)
    comp = df['components'].apply(pd.Series)
    df = pd.concat([df, comp], axis=1)
    cols = ['city','date','hour','aqi','pm2_5','pm10','co','no','no2','o3','so2']
    return df[[c for c in cols if c in df.columns]]

def transform_complete(raw):
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df['date'] = df['dt'].dt.date
    df['hour'] = df['dt'].dt.hour
    df['day'] = df['dt'].dt.day_name()
    df['month'] = df['dt'].dt.month
    df['year'] = df['dt'].dt.year
    df['weekend'] = df['dt'].dt.dayofweek >= 5
    seas = {1:'Winter',2:'Winter',3:'Spring',4:'Spring',5:'Spring',
            6:'Summer',7:'Summer',8:'Summer',9:'Autumn',10:'Autumn',11:'Autumn',12:'Winter'}
    df['season'] = df['month'].map(seas)
    df['aqi'] = df['main'].apply(lambda x: x['aqi'] if isinstance(x, dict) else None)
    comp = df['components'].apply(pd.Series)
    df = pd.concat([df, comp], axis=1)
    df['pm_ratio'] = df['pm2_5'] / df['pm10']
    cat = {1:'Good',2:'Fair',3:'Moderate',4:'Poor',5:'Very Poor'}
    df['aqi_cat'] = df['aqi'].map(cat)
    df['quality'] = df['aqi'].apply(lambda x: 'Good' if x <= 2 else 'Bad' if x <= 3 else 'Very Bad')
    cols = ['city','date','hour','day','month','year','season','weekend',
            'aqi','pm2_5','pm10','co','no','no2','o3','so2','pm_ratio','aqi_cat','quality']
    return df[[c for c in cols if c in df.columns]]

def transform_final(raw):
    if not raw:
        return pd.DataFrame()
    df = transform_complete(raw)
    df['p_date'] = pd.to_datetime(df['date'])
    return df

def save_basic(df):
    os.makedirs(p_clean, exist_ok=True)
    path = f"{p_clean}/basic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(path, index=False)
    print(f"Basic: {len(df)} records")

def save_complete(df):
    os.makedirs(p_clean, exist_ok=True)
    path = f"{p_clean}/complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(path, index=False)
    print(f"Complete: {len(df)} records")

def save_final(df):
    os.makedirs(p_clean, exist_ok=True)
    for date, group in df.groupby(df['p_date'].dt.date):
        path = f"{p_clean}/{date}"
        os.makedirs(path, exist_ok=True)
        file = f"{path}/data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        group.drop(columns=['p_date']).to_parquet(file, index=False)
        print(f"Final: {len(group)} records -> {date}")

def save_quality(df):
    os.makedirs(p_quality, exist_ok=True)
    r = {
        'total': len(df),
        'cities': df['city'].nunique(),
        'aqi_mean': float(df['aqi'].mean()),
        'aqi_min': float(df['aqi'].min()),
        'aqi_max': float(df['aqi'].max())
    }
    path = f"{p_quality}/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(path, 'w') as f:
        json.dump(r, f, indent=2)
    print("Report saved")

def main():
    raw = read_json()
    print(f"Found {len(raw)} records")
    
    print("\nTache 1 - Basic")
    df1 = transform_basic(raw)
    if not df1.empty:
        save_basic(df1)
    
    print("\nTache 2 - Complete")
    df2 = transform_complete(raw)
    if not df2.empty:
        save_complete(df2)
        save_quality(df2)
    
    print("\nTache 3 - Final")
    df3 = transform_final(raw)
    if not df3.empty:
        save_final(df3)
    
    print("\nDone!")

if __name__ == "__main__":
    main()
