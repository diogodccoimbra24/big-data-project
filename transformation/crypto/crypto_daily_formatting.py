

import json
import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa

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

#Converting stamps to date time
df["datetime"] = pd.to_datetime(df["timestamps_ms"], unit="ms", utc=True)
#Making sure the data it's all sorted
df = df.sort_values("datetime")

#daily close
df = (
    df.set_index("datetime")["price"]
      .resample("D")
      .last()
      .reset_index(name="price_end")
)

#Creating a column to tell us the crypto we are analysing
df['crypto_id'] = 'bitcoin'
df['crypto_id'] = df['crypto_id'].astype("string")

#Excluding hours
df["date"] = df["datetime"].dt.date
df.drop(columns=["datetime"], inplace=True)

df['price_start'] = df['price_end'].shift(1)

#Column for the daily variation
df['daily_change'] = df['price_end'] - df['price_start']

#Column for daily variation percentage
df['daily_change_pct'] = (df['price_end'] - df['price_start']) / df['price_start'] * 100

# arrow date type friendliness
df["date"] = pd.to_datetime(df["date"])

#Saving in parquet format
#To save in the designated directory
output_path = PROJECT_ROOT / "data" / "formatted" / "crypto" / "crypto_daily.parquet"
output_path.parent.mkdir(parents=True, exist_ok=True)


# Force Arrow schema since the viewer is displaying in other formats
schema = pa.schema([
    ("crypto_id", pa.string()),
    ("date", pa.date32()),
    ("price_start", pa.float64()),
    ("price_end", pa.float64()),
    ("daily_change", pa.float64()),
    ("daily_change_pct", pa.float64()),
])

table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
pq.write_table(table, output_path)

print(df.columns)
print("rows:", len(df))