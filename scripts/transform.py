import pandas as pd
import json, os, glob
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

def clean(df):
    if df.empty or 'dt' not in df.columns:
        return df
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df['date'] = df['dt'].dt.date
    df['hour'] = df['dt'].dt.hour
    df['aqi'] = df['main'].apply(lambda x: x['aqi'] if isinstance(x, dict) else None)
    comp = df['components'].apply(pd.Series)
    df = pd.concat([df, comp], axis=1)
    return df

def basic(df):
    if df.empty:
        return df
    df = clean(df)
    cols = ['city','date','hour','aqi','pm2_5','pm10','co','no','no2','o3','so2']
    return df[[c for c in cols if c in df.columns]]

def complete(df):
    if df.empty:
        return df
    df = clean(df)
    df['day'] = df['dt'].dt.day_name()
    df['month'] = df['dt'].dt.month
    df['year'] = df['dt'].dt.year
    df['weekend'] = df['dt'].dt.dayofweek >= 5
    seas = {1:'Winter',2:'Winter',3:'Spring',4:'Spring',5:'Spring',6:'Summer',7:'Summer',8:'Summer',9:'Autumn',10:'Autumn',11:'Autumn',12:'Winter'}
    df['season'] = df['month'].map(seas)
    df['pm_ratio'] = df['pm2_5'] / df['pm10']
    cat = {1:'Good',2:'Fair',3:'Moderate',4:'Poor',5:'Very Poor'}
    df['aqi_cat'] = df['aqi'].map(cat)
    df['quality'] = df['aqi'].apply(lambda x: 'Good' if x <= 2 else 'Bad' if x <= 3 else 'Very Bad')
    cols = ['city','date','hour','day','month','year','season','weekend','aqi','pm2_5','pm10','co','no','no2','o3','so2','pm_ratio','aqi_cat','quality']
    return df[[c for c in cols if c in df.columns]]

def save(df, name):
    os.makedirs(p_clean, exist_ok=True)
    df.to_parquet(f"{p_clean}/{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet", index=False)
    print(f"{name}: {len(df)} records")

def save_report(df):
    os.makedirs(p_quality, exist_ok=True)
    r = {'total': len(df), 'cities': df['city'].nunique(), 'aqi_mean': float(df['aqi'].mean()), 'aqi_min': float(df['aqi'].min()), 'aqi_max': float(df['aqi'].max())}
    with open(f"{p_quality}/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
        json.dump(r, f, indent=2)
    print("Report saved")

def main():
    raw = read_json()
    print(f"Found {len(raw)} records")
    df = pd.DataFrame(raw)
    
    d1 = basic(df.copy())
    if not d1.empty: save(d1, "basic")
    
    d2 = complete(df.copy())
    if not d2.empty: save(d2, "complete"); save_report(d2)
    
    d3 = complete(df.copy())
    if not d3.empty:
        d3['p_date'] = pd.to_datetime(d3['date'])
        for date, group in d3.groupby(d3['p_date'].dt.date):
            path = f"{p_clean}/{date}"
            os.makedirs(path, exist_ok=True)
            group.drop(columns=['p_date']).to_parquet(f"{path}/data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet", index=False)
            print(f"Final: {len(group)} records -> {date}")

if __name__ == "__main__":
    main()
