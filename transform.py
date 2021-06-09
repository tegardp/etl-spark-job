import os
import pandas as pd
from datetime import datetime, timedelta

DATA_PATH = 'data/1.raw'
OUTPUT_PATH = 'data/2.output'

# Get list of filenames inside data/raw folder
filenames = os.listdir(DATA_PATH)
filenames.sort()

for filename in filenames:
    delta = 723
    df = pd.read_json(f'{DATA_PATH}/{filename}', encoding='utf-8', lines=True)
    flight_date = filename.split('.')[0]
    flight_date = datetime.strptime(flight_date, '%Y-%m-%d')
    
    flight_date = flight_date + timedelta(days=723)
    file_date = flight_date + timedelta(days=1)
    file_date = file_date.strftime('%Y-%m-%d')
    flight_date = flight_date.strftime('%Y-%m-%d')
    
    df['flight_date'] = flight_date
    df.to_csv(f'{OUTPUT_PATH}/{file_date}.csv')