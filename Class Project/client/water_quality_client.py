import streamlit as st
import requests
import pandas as pd
from datetime import datetime, date, time
import numpy as np
import plotly.express as px

# --- Configuration ---
API_URL = "http://localhost:5000/api"

# --- Data Fetching Functions ---

@st.cache_data(ttl=600)
def fetch_data(endpoint, params=None):
    """
    Fetches data from the Flask API endpoint.
    """
    url = f"{API_URL}/{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.ConnectionError:
        st.error("Connection Error: The Flask API is not running or is inaccessible at http://localhost:5000. Please start 'water_quality_api.py'.")
        return None
    except requests.exceptions.HTTPError as e:
        try:
            # Attempt to read the error message from the API response
            error_data = response.json()
            st.error(f"API Error ({response.status_code}): {error_data.get('error', 'Unknown error.')}")
        except:
            st.error(f"HTTP Error: {e}. Could not parse API error message.")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

# --- UI Components ---

def setup_sidebar_filters():
    """Sets up the filter controls in the sidebar."""
    st.sidebar.header("Data Filters")

    params = {}

    # --- Date Range Filter ---
    st.sidebar.subheader("Time Range")
    
    # Define max date for safety
    today = date.today()
    
    # Updated default start date to ensure all data is captured
    default_start = date(2020, 1, 1)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=default_start)
    with col2:
        end_date = st.date_input("End Date", value=today)

    if start_date:
        # Convert date object to ISO format string for API: YYYY-MM-DDTHH:MM:SS
        params['start'] = datetime.combine(start_date, time.min).isoformat()
    if end_date:
        params['end'] = datetime.combine(end_date, time.max).isoformat()
        
    # --- Numeric Filters ---
    st.sidebar.subheader("Numeric Ranges")
    
    # Temperature (C)
    st.sidebar.markdown("##### Temperature (°C)")
    min_temp = st.sidebar.number_input("Min Temp", value=None, format="%.2f", step=0.1, key='min_temp')
    max_temp = st.sidebar.number_input("Max Temp", value=None, format="%.2f", step=0.1, key='max_temp')
    if min_temp is not None: params['min_temp'] = min_temp
    if max_temp is not None: params['max_temp'] = max_temp

    # Salinity (ppt)
    st.sidebar.markdown("##### Salinity (ppt)")
    min_sal = st.sidebar.number_input("Min Salinity", value=None, format="%.2f", step=0.1, key='min_sal')
    max_sal = st.sidebar.number_input("Max Salinity", value=None, format="%.2f", step=0.1, key='max_sal')
    if min_sal is not None: params['min_sal'] = min_sal
    if max_sal is not None: params['max_sal'] = max_sal

    # ODO (mg/L)
    st.sidebar.markdown("##### ODO (mg/L)")
    min_odo = st.sidebar.number_input("Min ODO", value=None, format="%.2f", step=0.1, key='min_odo')
    max_odo = st.sidebar.number_input("Max ODO", value=None, format="%.2f", step=0.1, key='max_odo')
    if min_odo is not None: params['min_odo'] = min_odo
    if max_odo is not None: params['max_odo'] = max_odo
    
    # --- Pagination Control (Always included) ---
    st.sidebar.subheader("Pagination")
    # Increased default limit to 1000 to maximize data shown by default
    params['limit'] = st.sidebar.slider("Limit (Max 1000)", 10, 1000, 1000)
    params['skip'] = st.sidebar.number_input("Skip", 0, step=params['limit'])


    return params

def display_observations_data(params):
    """
    Fetches observations data, displays the table, and returns the DataFrames
    for use in other sections.
    """
    st.info(f"API Call: `/api/observations` with parameters: {params}")

    # Fetch data
    observations_data = fetch_data("observations", params)
    
    if observations_data and 'items' in observations_data:
        total_count = observations_data.get('count', len(observations_data['items']))
        st.markdown(f"**Total matching records found (in DB): {total_count}**")
        
        items = observations_data['items']
        if not items:
            st.warning("No observations found matching the current filters.")
            return None, None

        # Convert to DataFrame
        df = pd.DataFrame(items)
        
        # Clean up column names
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])

        # Prepare DataFrame for plotting (index by timestamp)
        plot_df = df.copy()
        if 'timestamp' in plot_df.columns:
            plot_df['timestamp'] = pd.to_datetime(plot_df['timestamp'])
            plot_df = plot_df.set_index('timestamp')

        # Display the Raw Data Table (This is the only section visible initially)
        st.subheader("Filtered Data Table")
        st.dataframe(df, use_container_width=True)
        
        return df, plot_df
    
    return None, None

def display_visualizations(df, plot_df):
    """Renders the 1 Line, 1 Scatter, 1 Histogram."""
    
    st.subheader("Visualizations")
    
    col_viz_1, col_viz_2 = st.columns(2)

    # 1. Line Chart (Time Series)
    with col_viz_1:
        st.markdown("##### Temperature Over Time")
        if 'temperature' in plot_df.columns:
            # Display a line chart of temperature indexed by timestamp
            st.line_chart(plot_df['temperature'])
        else:
            st.warning("Temperature data missing.")

    # 2. Scatter Plot (Relationship)
    with col_viz_2:
        st.markdown("##### Salinity vs. Temperature")
        if 'salinity' in df.columns and 'temperature' in df.columns:
            # Use a basic scatter plot to show correlation
            st.scatter_chart(df, x='temperature', y='salinity')
        else:
            st.warning("Salinity or Temperature data missing.")

    # 3. Histogram (Distribution)
    st.markdown("##### ODO Distribution")
    if 'odo' in df.columns:
        # Create a Plotly Histogram
        fig = px.histogram(df, x="odo", nbins=20, 
                           title='Distribution of Dissolved Oxygen (mg/L)',
                           labels={'odo': 'Dissolved Oxygen (mg/L)'})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("ODO data missing.")


def display_stats():
    """Fetches and displays the summary statistics."""
    st.subheader("Summary Statistics")
    
    stats_data = fetch_data("stats")
    
    if stats_data:
        st.info("Statistics are calculated over the entire cleaned dataset (no filters applied).")
        
        # Convert the dictionary of stats into a list of dictionaries for DataFrame conversion
        data_for_df = []
        for field, stats in stats_data.items():
            row = {'Field': field}
            # Unpack all keys from the stats dictionary
            row.update(stats)
            data_for_df.append(row)
            
        df_stats = pd.DataFrame(data_for_df)
        
        # Format numeric columns for display
        numeric_cols = df_stats.columns.drop('Field')
        df_stats[numeric_cols] = df_stats[numeric_cols].apply(
            lambda x: x.map(lambda y: f"{y:.2f}" if pd.notna(y) else "N/A"), axis=1
        )
        
        st.table(df_stats.set_index('Field'))


def display_outliers():
    """Sets up UI for and displays outlier analysis."""
    st.subheader("Outlier Analysis")

    st.info("Outliers are calculated over the entire cleaned dataset (no filters applied), based on the selected method.")
    
    # UI controls for outlier analysis
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        field = st.selectbox("Select Field", ['temperature', 'salinity', 'odo'], key='outlier_field')
    with col2:
        method = st.selectbox("Select Method", ['iqr', 'zscore'], key='outlier_method')
    with col3:
        # Default K factor depends on the method
        default_k = 1.5 if method == 'iqr' else 3.0
        k_factor = st.number_input("K Factor", value=default_k, min_value=0.1, step=0.1, key='k_factor')

    # Prepare parameters for the API call
    params = {
        'field': field,
        'method': method,
        'k': k_factor
    }

    # Fetch outliers
    outlier_data = fetch_data("outliers", params)
    
    if outlier_data and 'outliers' in outlier_data:
        outliers = outlier_data['outliers']
        st.markdown(f"**Found {len(outliers)} outliers for {field} using {method} (K={k_factor})**")
        
        if outliers:
            df_outliers = pd.DataFrame(outliers)
            
            # Clean up and display
            if '_id' in df_outliers.columns:
                df_outliers = df_outliers.drop(columns=['_id'])

            # Highlight the field being analyzed
            def highlight_field(s):
                return ['background-color: yellow' if s.name == field else '' for _ in s]

            st.dataframe(
                df_outliers.style.apply(highlight_field, axis=1), 
                use_container_width=True
            )
        else:
            st.success("No outliers detected with the current method and K factor.")


# --- Main Application Logic ---

def main():
    st.set_page_config(layout="wide", page_title="Water Quality Data Dashboard")
    st.title("Water Quality Data Dashboard")
    st.markdown("Use the sidebar to filter the observations data via the Flask API.")

    # 1. Setup Filters
    filter_params = setup_sidebar_filters()

    # 2. Display Observations Table (Initial view)
    df, plot_df = display_observations_data(filter_params)
    
    st.markdown("---") # Separator after the table
    
    if df is not None:
        # 3. Create Tabs for Visualizations, Statistics, and Outliers
        tab1, tab2, tab3 = st.tabs(["Visualizations", "Statistics", "Outliers"])
        
        with tab1:
            display_visualizations(df, plot_df)

        with tab2:
            display_stats()

        with tab3:
            display_outliers()


if __name__ == "__main__":
    main()
