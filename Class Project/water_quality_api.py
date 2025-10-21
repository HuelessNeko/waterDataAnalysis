from flask import Flask, jsonify, request
import pandas as pd
import numpy as np
from datetime import datetime
import glob
import os

app = Flask(__name__)

# Data loading and preprocessing
def load_data():
    """Load and preprocess all CSV files from the Data directory"""
    data_dir = os.path.join(os.path.dirname(__file__), 'Data')
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    dfs = []
    for file in csv_files:
        df = pd.read_csv(file)
        # Convert timestamp to ISO format if it exists
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.isoformat()
        dfs.append(df)
    
    # Combine all dataframes
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def calculate_stats(df):
    """Calculate summary statistics for numeric columns"""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    stats = {}
    
    for col in numeric_cols:
        col_stats = {
            'count': int(df[col].count()),
            'mean': float(df[col].mean()),
            'min': float(df[col].min()),
            'max': float(df[col].max()),
            'percentiles': {
                '25': float(df[col].quantile(0.25)),
                '50': float(df[col].quantile(0.50)),
                '75': float(df[col].quantile(0.75))
            }
        }
        stats[col] = col_stats
    
    return stats

def detect_outliers(df, field, method='iqr', k=1.5):
    """Detect outliers in specified field using IQR or z-score method"""
    if field not in df.columns:
        return []
    
    if method == 'iqr':
        Q1 = df[field].quantile(0.25)
        Q3 = df[field].quantile(0.75)
        IQR = Q3 - Q1
        outlier_mask = (df[field] < (Q1 - k * IQR)) | (df[field] > (Q3 + k * IQR))
    else:  # z-score method
        z_scores = np.abs((df[field] - df[field].mean()) / df[field].std())
        outlier_mask = z_scores > k

    return df[outlier_mask].to_dict('records')

# API Endpoints
@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

@app.route('/api/observations')
def get_observations():
    """Get observations with optional filters"""
    # Load data
    df = load_data()
    if df.empty:
        return jsonify({"error": "No data available"}), 404

    # Parse query parameters
    start = request.args.get('start')
    end = request.args.get('end')
    min_temp = request.args.get('min_temp', type=float)
    max_temp = request.args.get('max_temp', type=float)
    min_sal = request.args.get('min_sal', type=float)
    max_sal = request.args.get('max_sal', type=float)
    min_odo = request.args.get('min_odo', type=float)
    max_odo = request.args.get('max_odo', type=float)
    limit = min(int(request.args.get('limit', 100)), 1000)
    skip = int(request.args.get('skip', 0))

    # Apply filters
    if start:
        df = df[df['timestamp'] >= start]
    if end:
        df = df[df['timestamp'] <= end]
    if min_temp is not None:
        df = df[df['temperature'] >= min_temp]
    if max_temp is not None:
        df = df[df['temperature'] <= max_temp]
    if min_sal is not None:
        df = df[df['salinity'] >= min_sal]
    if max_sal is not None:
        df = df[df['salinity'] <= max_sal]
    if min_odo is not None:
        df = df[df['odo'] >= min_odo]
    if max_odo is not None:
        df = df[df['odo'] <= max_odo]

    # Apply pagination
    total_count = len(df)
    df = df.iloc[skip:skip + limit]

    return jsonify({
        "count": total_count,
        "items": df.to_dict('records')
    })

@app.route('/api/stats')
def get_stats():
    """Get summary statistics for numeric fields"""
    df = load_data()
    if df.empty:
        return jsonify({"error": "No data available"}), 404

    stats = calculate_stats(df)
    return jsonify(stats)

@app.route('/api/outliers')
def get_outliers():
    """Get outliers for a specific field"""
    df = load_data()
    if df.empty:
        return jsonify({"error": "No data available"}), 404

    field = request.args.get('field', type=str)
    if not field:
        return jsonify({"error": "Field parameter is required"}), 400

    method = request.args.get('method', 'iqr')
    if method not in ['iqr', 'zscore']:
        return jsonify({"error": "Invalid method. Use 'iqr' or 'zscore'"}), 400

    k = float(request.args.get('k', 1.5))
    
    outliers = detect_outliers(df, field, method, k)
    return jsonify({
        "count": len(outliers),
        "outliers": outliers
    })

@app.errorhandler(404)
def not_found_error(error):
    return jsonify({"error": "Resource not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True)
