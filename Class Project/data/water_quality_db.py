import pandas as pd
from datetime import datetime
from mongomock import MongoClient
import numpy as np
import os
from math import floor

# --- Configuration ---

# List of CSV files to load.
paths = [
    "2021-dec16.csv",
    "2021-oct21.csv",
    "2022-nov16.csv",
    "2022-oct7.csv",
]

# Map common raw column names to standardized names used by the API.
COLUMN_REMAP = {
    # Standardized column names and their variants
    'timestamp': 'timestamp', 
    'date': 'date', 'date m/d/y': 'date', 
    'time': 'time', 'time hh:mm:ss': 'time', 'date and time': 'timestamp', 
    
    'latitude': 'latitude', 
    'longitude': 'longitude', 'long': 'longitude',
    
    'temperature': 'temperature', 'temp c': 'temperature', 'temp (c)': 'temperature', 
    'salinity': 'salinity', 'sal ppt': 'salinity', 'salinity (ppt)': 'salinity', 
    'odo': 'odo', 'odo mg/l': 'odo', 'd.o.': 'odo', 'dissolved oxygen': 'odo', 
}

# --- Database Class ---

class WaterQualityDB:
    """Manages data cleaning, outlier removal, and interaction with the mock MongoDB."""
    
    # Class variables for reporting and database connection
    original_rows = 0
    removed_outliers = 0
    remaining_rows = 0
    
    client = MongoClient()
    db = client.water_quality_data
    collection = db.asv_1

    def __init__(self):
        """Initializes the database by cleaning and inserting data."""
        # Check if collection is empty before running initialization
        if self.collection.count_documents({}) == 0:
            print("--- Database Initialization ---")
            df = self._load_and_standardize_data()
            df = self._clean_outliers(df)
            self._insert_data(df)
            self._print_report()
            self._print_sample_data()
            
    def _load_and_standardize_data(self):
        """Loads data, standardizes column names, and combines date/time columns."""
        all_data = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        required_cols = ['timestamp', 'latitude', 'longitude', 'temperature', 'salinity', 'odo']

        for file_name in paths:
            file_path = os.path.join(base_dir, file_name)
            try:
                df = pd.read_csv(file_path, low_memory=False)
                
                # Standardize column names (lowercase, no spaces)
                df.columns = [str(x).lower().strip() for x in df.columns]
                
                # Apply standardization map and handle conflicts
                new_cols_map = {}
                mapped_names = set()
                for old_col in df.columns:
                    standard_name = COLUMN_REMAP.get(old_col, old_col)
                    
                    # Map only if the standard name hasn't been used yet
                    if standard_name not in mapped_names:
                        new_cols_map[old_col] = standard_name
                        mapped_names.add(standard_name)
                    else:
                        print(f"Warning: Skipping column '{old_col}' in {file_name} due to conflict with '{standard_name}'.")

                df.rename(columns=new_cols_map, inplace=True)
                
                # Combine 'date' and 'time' into 'timestamp'
                if 'date' in df.columns and 'time' in df.columns:
                    datetime_series = df['date'].astype(str) + ' ' + df['time'].astype(str)
                    # FIX: Removed the strict format to allow Pandas to auto-infer M/D/Y or Y-M-D
                    df['timestamp'] = pd.to_datetime(datetime_series, errors='coerce') 
                    df.drop(columns=['date', 'time'], inplace=True, errors='ignore')
                elif 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

                # Filter to only keep required columns
                df = df.filter(items=required_cols, axis=1)

                # Convert numeric columns to float, coercing errors to NaN
                numeric_cols_to_check = ['temperature', 'salinity', 'odo', 'latitude', 'longitude']
                for col in numeric_cols_to_check:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Replace inf/-inf with NaN to prevent statistical crashes
                df.replace([np.inf, -np.inf], np.nan, inplace=True)

                all_data.append(df)
            except FileNotFoundError:
                print(f"Error: File not found at {file_path}")
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

        if not all_data:
            return pd.DataFrame()
            
        combined_df = pd.concat(all_data, ignore_index=True)
        self.original_rows = len(combined_df)

        # Drop rows missing data in any required column
        combined_df.dropna(subset=required_cols, how='any', inplace=True)
        
        return combined_df.sort_values('timestamp').reset_index(drop=True)

    def _clean_outliers(self, df, z_score_threshold=3.0):
        """Removes rows where any numeric field has a Z-score deviation > threshold."""
        if df.empty:
            return df

        numeric_cols = ['temperature', 'salinity', 'odo']
        df_clean = df.copy()
        
        for col in numeric_cols:
            if col in df_clean.columns:
                mean = df_clean[col].mean()
                std = df_clean[col].std()
                
                # Calculate Z-score, handling division by zero if std=0
                df_clean[f'{col}_z'] = np.where(
                    std == 0, 
                    0.0, 
                    ((df_clean[col] - mean) / std).abs()
                )
        
        # Check if ANY of the Z-score columns exceeds the threshold
        z_cols = [f'{c}_z' for c in numeric_cols if f'{c}_z' in df_clean.columns]
        
        if z_cols:
            outlier_mask = (df_clean[z_cols] > z_score_threshold).any(axis=1)
            df_cleaned = df_clean[~outlier_mask].drop(columns=z_cols)
        else:
            df_cleaned = df_clean 

        self.removed_outliers = self.original_rows - len(df_cleaned)
        self.remaining_rows = len(df_cleaned)
        return df_cleaned.reset_index(drop=True)

    def _insert_data(self, df):
        """Inserts the cleaned DataFrame into the mongomock database."""
        if df.empty:
            print("No data to insert.")
            return

        # Convert to list of dictionaries, replacing NaN with None for MongoDB
        records = df.where(df.notna(), None).to_dict(orient="records")
        
        if records:
            res = self.collection.insert_many(records)
            print(f"Inserted {len(res.inserted_ids)} documents into water_quality_data.asv_1 (mongomock)")

        self.collection.create_index("temperature")
        print("Data inserted and 'temperature' field indexed.")

    def _print_report(self):
        """Prints the data cleaning report."""
        print("\n--- Data Cleaning Report (Z-score > 3.0) ---")
        print(f"Total rows originally: {self.original_rows}")
        print(f"Rows removed as outliers and missing required data: {self.removed_outliers}") 
        print(f"Rows remaining after cleaning: {self.remaining_rows}")
        print("-------------------------------------------\n")

    def _print_sample_data(self):
        """Prints the first five records from the collection."""
        print("\n--- Sample Data (First 5 Records) ---")
        try:
            sample_cursor = self.collection.find().limit(5).sort('timestamp', 1)
            
            sample_items = []
            for item in sample_cursor:
                # Convert datetime to string for clean printing
                if isinstance(item.get('timestamp'), datetime):
                    item['timestamp'] = item['timestamp'].isoformat()
                
                # Filter out the internal MongoDB ID
                sample_items.append({k: v for k, v in item.items() if k != '_id'})
            
            if sample_items:
                print(pd.DataFrame(sample_items).to_string(index=False))
            else:
                print("Database collection is empty.")
        except Exception as e:
            print(f"Error reading sample data: {e}")
        print("-------------------------------------\n")

    def get_data(self):
        """Returns the mongomock collection for use by the API."""
        return self.collection

# Initialize the database instance (triggers data loading and cleaning)
db_manager = WaterQualityDB()
