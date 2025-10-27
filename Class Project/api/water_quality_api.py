import pandas as pd
from flask import Flask, jsonify, request
from bson.decimal128 import Decimal128
from datetime import datetime
from json import JSONEncoder
import numpy as np
import sys
import os 

# --- Database Setup and Import ---

# Add the 'data' directory to the Python path to import the database manager.
data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
if data_path not in sys.path:
    sys.path.append(data_path)

try:
    from water_quality_db import db_manager 
    db_collection = db_manager.get_data()
    if db_collection is None:
        print("WARNING: Database collection is empty. Check data/water_quality_db.py.")
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import water_quality_db. Error: {e}")
    db_collection = None 

# --- Custom JSON Encoder ---

class MongoJSONEncoder(JSONEncoder):
    """Custom encoder for non-standard types like Decimal128, datetime, and numpy generics."""
    def default(self, obj):
        if isinstance(obj, Decimal128):
            return float(obj.to_decimal())
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, np.generic):
            return obj.item()
        return super().default(obj)

# --- Conversion Helper ---

def convert_decimals_to_float(item):
    """Converts database types (Decimal128, datetime) to Python types for JSON output."""
    new_item = {}
    for key, value in item.items():
        if isinstance(value, Decimal128):
            new_item[key] = float(value.to_decimal())
        elif isinstance(value, datetime):
            new_item[key] = value.isoformat()
        else:
            new_item[key] = value
    return new_item

# --- Flask App Initialization ---
app = Flask(__name__)
app.json_encoder = MongoJSONEncoder

# --- Helper Functions ---

def safe_iso_to_datetime(iso_string):
    """
    Converts ISO 8601 string to a naive datetime object.
    Handles microsecond overflow issues by truncating to 6 digits.
    """
    try:
        # Attempt standard parsing first
        dt = datetime.fromisoformat(iso_string)
        # Ensure it's naive (no timezone info) for consistent filtering
        return dt.replace(tzinfo=None) 
    except ValueError:
        # Fallback for strings with too many microseconds
        if '.' in iso_string:
            try:
                base_part, micro_tz_part = iso_string.rsplit('.', 1)
                
                # Separate microseconds from timezone suffix (if present)
                tz_suffix = ''
                micro_part = micro_tz_part
                
                if 'Z' in micro_tz_part:
                    tz_suffix = 'Z'
                    micro_part = micro_tz_part.replace('Z', '')
                elif '+' in micro_tz_part:
                    tz_index = micro_tz_part.find('+')
                    tz_suffix = micro_tz_part[tz_index:]
                    micro_part = micro_tz_part[:tz_index]
                elif '-' in micro_tz_part and ':' in micro_tz_part:
                    # Handle negative timezone offset
                    tz_index = micro_tz_part.rfind('-') 
                    tz_suffix = micro_tz_part[tz_index:]
                    micro_part = micro_tz_part[:tz_index]

                # Truncate microseconds to 6 digits and pad
                clean_micro = micro_part[:6].zfill(6)
                clean_iso_string = f"{base_part}.{clean_micro}{tz_suffix}"
                
                # Parse the cleaned string and remove timezone info
                dt = datetime.fromisoformat(clean_iso_string)
                return dt.replace(tzinfo=None)

            except Exception:
                return None
        return None


def build_mongo_query(args):
    """Creates a MongoDB query filter dictionary from URL parameters."""
    query = {}
    
    # Time Range Filter
    if 'start' in args or 'end' in args:
        query['timestamp'] = {}
        if 'start' in args:
            start_dt = safe_iso_to_datetime(args['start'])
            if start_dt is None:
                return None, "Invalid 'start' date format."
            query['timestamp']['$gte'] = start_dt
        if 'end' in args:
            end_dt = safe_iso_to_datetime(args['end'])
            if end_dt is None:
                return None, "Invalid 'end' date format."
            query['timestamp']['$lte'] = end_dt

    # Numeric Range Filters
    filters = {
        'temperature': ('min_temp', 'max_temp'),
        'salinity': ('min_sal', 'max_sal'),
        'odo': ('min_odo', 'max_odo')
    }
    
    for field, (min_key, max_key) in filters.items():
        if min_key in args or max_key in args:
            query[field] = {}
            try:
                if min_key in args:
                    query[field]['$gte'] = float(args[min_key])
                if max_key in args:
                    query[field]['$lte'] = float(args[max_key])
            except ValueError:
                return None, f"Invalid value for {min_key} or {max_key}. Must be a number."
            
    return query, None

# --- API Endpoints ---

@app.route("/api/health", methods=["GET"])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "ok"})

@app.route("/api/observations", methods=["GET"])
def get_observations():
    """Fetches filtered and paginated water quality observations."""
    if db_collection is None:
        return jsonify({"error": "Database not initialized."}), 500

    args = request.args
    query, error = build_mongo_query(args)
    if error:
        return jsonify({"error": error}), 400
    
    try:
        limit = min(int(args.get('limit', 100)), 1000) 
        skip = int(args.get('skip', 0))
        if limit < 0 or skip < 0:
            return jsonify({"error": "Limit and skip must be non-negative."}), 400
    except ValueError:
        return jsonify({"error": "Limit and skip must be valid integers."}), 400

    try:
        total_count = db_collection.count_documents(query)
        
        # Find items, excluding the non-serializable '_id' field
        items_cursor = db_collection.find(query, {"_id": 0}).skip(skip).limit(limit).sort('timestamp', 1) 
        
        # Convert items to JSON-friendly types
        items = [convert_decimals_to_float(item) for item in items_cursor]
        
        return jsonify({
            "count": total_count,
            "items": items
        })
    except Exception as e:
        print(f"Database Query Error: {e}") 
        return jsonify({"error": "Internal server error during database query."}), 500


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Calculates summary statistics (mean, min, max, quartiles) for fields."""
    if db_collection is None:
        return jsonify({"error": "Database not initialized."}), 500
        
    try:
        # Fetch data, excluding the ID, and convert types
        all_items = list(db_collection.find({}, {"_id": 0, "temperature": 1, "salinity": 1, "odo": 1}))
        processed_items = [convert_decimals_to_float(item) for item in all_items]
        df_all = pd.DataFrame(processed_items)
        
        stats = {}
        for col in ['temperature', 'salinity', 'odo']:
            if col in df_all.columns:
                series = df_all[col].dropna().astype(float)
                if not series.empty:
                    stats[col] = {
                        "count": int(series.count()),
                        "mean": series.mean(),
                        "min": series.min(),
                        "max": series.max(),
                        "25%": series.quantile(0.25),
                        "50%": series.median(),
                        "75%": series.quantile(0.75)
                    }
                else:
                    stats[col] = { "count": 0, "mean": None, "min": None, "max": None, "25%": None, "50%": None, "75%": None }

        return jsonify(stats)

    except Exception as e:
        print(f"Stats Error: {e}")
        return jsonify({"error": "Internal server error during statistics calculation."}), 500


@app.route("/api/outliers", methods=["GET"])
def find_outliers():
    """Identifies outliers using IQR or Z-score method on a specified field."""
    if db_collection is None:
        return jsonify({"error": "Database not initialized."}), 500
        
    args = request.args
    field = args.get('field')
    method = args.get('method', 'iqr')
    
    try:
        k_factor = float(args.get('k', 1.5 if method == 'iqr' else 3.0))
    except ValueError:
        return jsonify({"error": "Invalid factor 'k'."}), 400

    if field not in ['temperature', 'salinity', 'odo']:
        return jsonify({"error": "Invalid field specified."}), 400
    
    try:
        # Fetch relevant fields, convert types, and create DataFrame
        all_items = list(db_collection.find({}, {"_id": 0, field: 1, "timestamp": 1, "latitude": 1, "longitude": 1}))
        processed_items = [convert_decimals_to_float(item) for item in all_items]
        df = pd.DataFrame(processed_items)

    except Exception as e:
        print(f"Outliers Query Error: {e}")
        return jsonify({"error": "Internal server error during database query."}), 500
    
    # Prepare data for outlier calculation
    df.dropna(subset=[field], inplace=True)
    df[field] = df[field].astype(float)
    
    if df.empty:
        return jsonify({"outliers": []})

    outlier_mask = pd.Series(False, index=df.index)

    if method == 'zscore':
        mean = df[field].mean()
        std = df[field].std()
        if std != 0:
            z_scores = (df[field] - mean) / std
            outlier_mask = z_scores.abs() > k_factor
    
    elif method == 'iqr':
        Q1 = df[field].quantile(0.25)
        Q3 = df[field].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - k_factor * IQR
        upper_bound = Q3 + k_factor * IQR
        outlier_mask = (df[field] < lower_bound) | (df[field] > upper_bound)
    
    else:
        return jsonify({"error": "Invalid outlier method. Use 'iqr' or 'zscore'."}), 400

    outliers_df = df[outlier_mask]
    outlier_records = outliers_df.to_dict(orient='records')
    
    return jsonify({"outliers": outlier_records, "count": len(outlier_records)})

if __name__ == "__main__":
    app.run(port=5000, debug=True)
