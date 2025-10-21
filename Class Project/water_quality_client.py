import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime

# --- Configuration ---
# API address for the Flask app
API_BASE_URL = "http://localhost:5000/api"
st.set_page_config(layout="wide", page_title="Water Quality API Client")

# --- Helper Functions ---

def fetch_data(endpoint, params=None):
    """Fetches data from the API."""
    url = f"{API_BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, params=params, timeout=10)
        # Check for errors in a simple way
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API returned an error: {response.status_code}")
            return None
    except requests.exceptions.ConnectionError:
        st.error(f"Connection Error: Is the Flask API running at {API_BASE_URL}? ")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

def build_observations_params(sidebar_params):
    """Builds the query parameters for the observations endpoint."""
    params = {
        'limit': sidebar_params['limit'],
        'skip': sidebar_params['skip']
    }

    # Date filters
    if sidebar_params['date_range'] and len(sidebar_params['date_range']) == 2:
        params['start'] = sidebar_params['date_range'][0].isoformat() + 'T00:00:00'
        params['end'] = sidebar_params['date_range'][1].isoformat() + 'T23:59:59'

    # Min/Max filters
    if sidebar_params['temp_min'] is not None: params['min_temp'] = sidebar_params['temp_min']
    if sidebar_params['temp_max'] is not None: params['max_temp'] = sidebar_params['temp_max']
    if sidebar_params['sal_min'] is not None: params['min_sal'] = sidebar_params['sal_min']
    if sidebar_params['sal_max'] is not None: params['max_sal'] = sidebar_params['max_sal']
    if sidebar_params['odo_min'] is not None: params['min_odo'] = sidebar_params['odo_min']
    if sidebar_params['odo_max'] is not None: params['max_odo'] = sidebar_params['max_odo']
    
    return params

# --- Streamlit App Layout ---

st.title("Water Data Analysis App")

# --- 1. Sidebar (Controls Panel) ---
st.sidebar.header("Data Filters")

# Date Range
st.sidebar.subheader("Date Range")
date_range = st.sidebar.date_input(
    "Select Period",
    (datetime(2021, 10, 1).date(), datetime(2022, 12, 31).date()),
    max_value=datetime.now().date()
)

# Min/Max Filters
st.sidebar.subheader("Min/Max Values")

col1, col2 = st.sidebar.columns(2)
with col1:
    temp_min = st.number_input("Min Temp (C)", value=None, key="t_min")
    sal_min = st.number_input("Min Salinity (ppt)", value=None, key="s_min")
    odo_min = st.number_input("Min ODO (mg/L)", value=None, key="o_min")
with col2:
    temp_max = st.number_input("Max Temp (C)", value=None, key="t_max")
    sal_max = st.number_input("Max Salinity (ppt)", value=None, key="s_max")
    odo_max = st.number_input("Max ODO (mg/L)", value=None, key="o_max")

# Limit and Pagination
st.sidebar.subheader("Pagination")
limit = st.sidebar.slider("Limit (rows)", 10, 1000, 100)
skip = st.sidebar.number_input("Skip (Offset)", 0, step=limit)

# Store all sidebar parameters
sidebar_params = {
    'date_range': date_range,
    'temp_min': temp_min, 'temp_max': temp_max,
    'sal_min': sal_min, 'sal_max': sal_max,
    'odo_min': odo_min, 'odo_max': odo_max,
    'limit': limit,
    'skip': skip
}

# Get Observations Data and Display ---
params = build_observations_params(sidebar_params)
obs_data = fetch_data('observations', params)

if obs_data and 'items' in obs_data:
    df_raw = pd.DataFrame(obs_data['items'])
    total_count = obs_data.get('count', len(df_raw))

    st.subheader(f"Filtered Data ({len(df_raw)} of {total_count} total)")
    
    if not df_raw.empty:
        # Simplified Column Mapping
        COLUMN_MAPPING = {
            'Temperature (c)': 'Temperature',
            'Salinity (ppt)': 'Salinity',
            'ODO mg/L': 'ODO',
            'timestamp': 'Timestamp'
        }
        
        df_display = df_raw.rename(columns=COLUMN_MAPPING)

        # Convert timestamp for plotting
        if 'Timestamp' in df_display.columns:
            try:
                df_display['Timestamp'] = pd.to_datetime(df_display['Timestamp'])
            except:
                st.warning("Could not convert Timestamp column.")
        
        # Data Table
        st.dataframe(df_display, use_container_width=True)



        # Visualizations
        st.header("Visualizations")
        
        # Prepare data for plotting
        required_cols = ['Timestamp', 'Temperature', 'Salinity', 'ODO']
        plot_df = df_display[[c for c in required_cols if c in df_display.columns]].dropna()

        if len(plot_df) > 1:
            col_viz1, col_viz2 = st.columns(2)
            
            # Line Chart: Temperature over Time
            with col_viz1:
                st.subheader("1. Temperature Trend")
                fig1 = px.line(
                    plot_df, 
                    x='Timestamp', 
                    y='Temperature', 
                    title='Temperature (C)',
                    template='plotly_white'
                )
                st.plotly_chart(fig1, use_container_width=True)
            
            # Histogram: Salinity Distribution
            with col_viz2:
                st.subheader("2. Salinity Distribution")
                fig2 = px.histogram(
                    plot_df, 
                    x='Salinity', 
                    title='Salinity (ppt) Frequency',
                    template='plotly_white'
                )
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("---")
            
            # Scatter Plot: Temp vs Salinity, color by ODO
            st.subheader("3. Temp vs. Salinity")
            fig3 = px.scatter(
                plot_df, 
                x='Salinity', 
                y='Temperature', 
                color='ODO',
                title='Temperature vs Salinity',
                template='plotly_white'
            )
            st.plotly_chart(fig3, use_container_width=True)
            
        else:
            st.warning("Not enough data to create charts.")

    else:
        st.info("No data found matching the filters.")
        
    # Statistics View
    st.header("Summary Statistics")
    stats_data = fetch_data('stats')
    if stats_data:
        
        stats_df = pd.DataFrame(stats_data).T
        st.dataframe(stats_df, use_container_width=True)


    # Outliet View
    st.header("Outlier Detection")
    st.info("Check for outliers in one field.")
    
    col_field, col_method, col_k = st.columns(3)

    numeric_fields = ['temperature', 'salinity', 'odo']
    
    with col_field:
        outlier_field = st.selectbox("Select Field", numeric_fields)

    with col_method:
        outlier_method = st.selectbox("Method", ['iqr', 'zscore'])

    with col_k:
        k_factor = st.number_input(f"Factor (k)", 
                                    value=1.5 if outlier_method == 'iqr' else 3.0, 
                                    min_value=0.1, 
                                    step=0.1)

    outlier_params = {
        'field': outlier_field,
        'method': outlier_method,
        'k': k_factor
    }

    if st.button("Find Outliers"):
        outlier_results = fetch_data('outliers', outlier_params)
        
        if outlier_results and 'outliers' in outlier_results:
            outliers_df = pd.DataFrame(outlier_results['outliers'])
            st.subheader(f"Found {len(outliers_df)} Outliers")
            if not outliers_df.empty:
                st.dataframe(outliers_df, use_container_width=True)
            else:
                st.info("No outliers found.")
