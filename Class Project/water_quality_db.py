import numpy as np
import pandas as pd
import mongomock


# Paths to the CSVs files, all in one place
paths = [
    "Data/2021-dec16.csv",
    "Data/2021-oct21.csv",
    "Data/2022-nov16.csv",
    "Data/2022-oct7.csv",
]

# Read only the needed columns from each file and stack them vertically
num_cols = ["Temperature (c)", "Salinity (ppt)", "ODO mg/L" ]
num_df = pd.concat([pd.read_csv(p, usecols=num_cols) for p in paths], ignore_index=True)

# Creates a Database with all the information to later take out the outliers
full = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)

# Computes z-scores cell-by-cell, using each column’s mean and std
z_scores = (num_df - num_df.mean()) / num_df.std(ddof=0)


# Flags a row if ANY numeric column has |z| > 3
outliers = (z_scores.abs() > 3).any(axis=1)

# Creates a new Database without the outliers
no_outliers = full.loc[~outliers].copy()

#Report
print(f"Total rows originally: {len(full)}")
print(f"Rows removed as outliers: {outliers.sum()}")
print(f"Rows remaining after cleaning: {len(no_outliers)}")

# Create a fake (in-memory) MongoDB server
client = mongomock.MongoClient()

# Choose the database and collection where we’ll store/query documents
db = client["water_quality_data"]
col = db["asv_1"]

# Create an ascending index on "Temperature (c)" for faster temperature queries
col.create_index([("Temperature (c)", 1)], name="idx_temp_raw")

# Mongo can't store NaN so it converts NaN -> None to prevent any errors that missing cells might cause
to_insert = no_outliers.where(no_outliers.notna(), None)

# Turns the DataFrame into a plain Python list of dictionaries
records = to_insert.to_dict(orient="records")

# Inserts the rows into the asv_1 collection only if there are records
if records:
    res = col.insert_many(records)
    print(f"Inserted {len(res.inserted_ids)} documents into water_quality_data.asv_1 (mongomock)")
else:
    print("No records to insert.")