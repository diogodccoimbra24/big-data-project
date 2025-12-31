

import json
import pandas as pd
import numpy as np
from pathlib import Path
import fastparquet as fp
import pyarrow

#To find the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

#File path to the json file
file_path = PROJECT_ROOT / "data" / "raw" / "crypto" / "bitcoin_price_from_2025-01-01_to_2025-12-31.json"

with open(file_path, "r") as f:
    data = json.load(f)

#Creating a data frame for the data
df = pd.DataFrame(
    data['prices'],
    columns=['timestamps_ms', 'price']
)

#Creating a column to tell us the crypto we are analysing
df['crypto_id'] = 'bitcoin'

#Converting unix to UTC
df['datetime'] = pd.to_datetime(df['timestamps_ms'], unit = 'ms', utc = True)

#Droping unnecessary columns
df.drop(['timestamps_ms'], axis = "columns", inplace=True)

#To make sure the date is sorted
df.sort_values('datetime', ascending = True)

#Excluding the hours because it's not necessary anymore
df['date'] = df['datetime'].dt.date
df.drop(['datetime'], axis = "columns", inplace = True)

#Creating a column to tell us the value of the first price of the day (meaning the last price of the previous day)
df['price_start'] = df['price'].shift(periods = 1)

#Creating a column for the last price of the day
df['price_end'] = df['price']

#Creating a column for the daily variation
df['daily_change'] = df['price_end'] - df['price_start']

#Creating a column for daily variation percentage
df['daily_change_pct'] = (df['price_end'] - df['price_start']) / df['price_start'] * 100

#Dropping unnecessary column
df.drop(['price'], axis = "columns", inplace = True)


#Saving in parquet format
#To save in the designated directory
output_path = PROJECT_ROOT / "data" / "formatted" / "crypto" / "crypto_daily.parquet"
df.to_parquet(output_path, engine = 'pyarrow')

#To check if the parquet was created properly
df_check = pd.read_parquet(output_path)
print(df_check.head())
print(df_check.shape)

#Check if the parquet was created since it's not appearing on the file tree
print(output_path.exists())