

import json
import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa

#To find the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

#File path to the json file (news_tone)
file_path = PROJECT_ROOT / "data" / "raw" / "news" / "news_tone.json"

with open (file_path, "r") as f:
    data = json.load(f)

#Extracting daily list from json
daily_list = data["timeline"][0]["data"]

#Creating data frame for tone
df_tone = pd.DataFrame(daily_list)

#Converting date format
df_tone["date"] = pd.to_datetime(df_tone["date"], format="%Y%m%dT%H%M%SZ")
df_tone["date"] = df_tone["date"].dt.date

#Renaming a column
df_tone.rename(columns={"value": "avg_tone"}, inplace = True)



#File path for json file (news_volume)
file_path = PROJECT_ROOT / "data" / "raw" / "news" / "news_volume.json"

with open (file_path, "r") as f:
    data = json.load(f)

daily_list = data["timeline"][0]["data"]

#Creating data frame for volume
df_volume = pd.DataFrame(daily_list)

#Converting date format
df_volume["date"] = pd.to_datetime(df_volume["date"], format="%Y%m%dT%H%M%SZ")
df_volume["date"] = df_volume["date"].dt.date

#Renaming column
df_volume.rename(columns ={"value" : "news_volume"}, inplace = True)

#Dropping column
df_volume.drop(columns=["norm"], inplace=True)

#Merging the 2 datasets
news_daily_df = df_tone.merge(df_volume, on = "date")

#Adding 17 missing days to dataset
#Created dataframe to merge with news_daily_df
date_r = pd.date_range(start="2025-01-01", end="2025-12-31", freq="D")
df_fill = pd.DataFrame(date_r, columns = ["date"])

#Forcing both to have the same date format
news_daily_df["date"] = pd.to_datetime(news_daily_df["date"])
df_fill["date"] = pd.to_datetime(df_fill["date"])

#Merging with left join to keep all the dates from df_fill
filled_news_daily_df = df_fill.merge(news_daily_df, on = "date", how = "left")

#Filling NaN with 0 in news_volume column
filled_news_daily_df["news_volume"] = filled_news_daily_df["news_volume"].fillna(0)


#Saving in parquet format
#To save in the designated directory
output_path = PROJECT_ROOT / "data" / "formatted" / "news" / "news_daily.parquet"
output_path.parent.mkdir(parents=True, exist_ok=True)


# Force Arrow schema since the viewer is displaying in other formats
schema = pa.schema([
    ("date", pa.date32()),
    ("avg_tone", pa.float64()),
    ("news_volume", pa.int64()),
])

table = pa.Table.from_pandas(filled_news_daily_df, schema=schema, preserve_index=False)
pq.write_table(table, output_path)




