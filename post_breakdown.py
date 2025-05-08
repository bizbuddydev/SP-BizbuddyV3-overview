import streamlit as st
import pandas as pd
from google.cloud import bigquery  # If you're using BigQuery
import requests  # If you're calling the Graph API directly
import json
from google.oauth2 import service_account
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Set page components
st.set_page_config(page_title="SP Bizz Overview", layout="wide", page_icon="📊")

#Load Vars
PROJECT_ID = "bizbuddydemo-v3"
FB_PAGE_ID = 12101296
IG_USER_ID = 17841400708882174

# Load credentials and project ID from st.secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

@st.cache_data
def pull_ig_insights(dataset_id, table_id):
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` WHERE user_id = {IG_USER_ID}"
    try:
        # Execute the query
        query_job = client.query(query)
        result = query_job.result()
        # Convert the result to a DataFrame
        data = result.to_dataframe()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

@st.cache_data
def pull_ig_account_insights(dataset_id, table_id):
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` WHERE id = {IG_USER_ID}"
    try:
        # Execute the query
        query_job = client.query(query)
        result = query_job.result()
        # Convert the result to a DataFrame
        data = result.to_dataframe()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

@st.cache_data
def pull_post_analysis(dataset_id, table_id):
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}`"
    try:
        # Execute the query
        query_job = client.query(query)
        result = query_job.result()
        # Convert the result to a DataFrame
        data = result.to_dataframe()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

@st.cache_data
def get_data():
    
    #Get ig posts
    ig_dataset_id = "instagram_business_instagram_business"
    ig_table_id = "instagram_business__posts"
    basic_ig_df = pull_ig_insights(ig_dataset_id, ig_table_id)

    #Get ig posts
    ig_account_dataset_id = "instagram_business"
    ig_account_table_id = "user_insights"
    ig_account_df = pull_ig_account_insights(ig_account_dataset_id, ig_account_table_id)

    #Get analyzed posts
    client_dataset_id = "client"
    client_table_id = "sp_analyzed_posts"
    pa_df = pull_post_analysis(client_dataset_id, client_table_id)

    #return all dfs
    return basic_ig_df, ig_account_df, pa_df

def main():
    basic_ig_df, ig_account_df, pa_df = get_data()
    st.title("📱 Social Post Breakdown")
    st.write(basic_ig_df)

    # SECTION 1: Filters
    st.markdown("### 🔧 Filter Options")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        platform = st.selectbox("Platform", ["Instagram"], disabled=True)  # Only Instagram available for now
    
    with col2:
        content_type = st.selectbox("Content Type", ["All", "Image", "Video", "Carousel", "Reel"])
    
    with col3:
        default_start = basic_ig_df['created_timestamp'].min().date()
        default_end = basic_ig_df['timestamp'].max().date()
        date_range = st.date_input("Date Range", [default_start, default_end])
    
    # --- Filter Data ---
    df = basic_ig_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Filter by content type
    if content_type != "All":
        df = df[df['media_type'].str.lower() == content_type.lower()]
    
    # Filter by date
    if isinstance(date_range, list) and len(date_range) == 2:
        start_date = pd.to_datetime(date_range[0])
        end_date = pd.to_datetime(date_range[1])
        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
    
    # Compute Engagement
    df['engagement'] = (
        df.get('like_count', 0) +
        df.get('comments_count', 0) +
        df.get('save_count', 0)
    )
    
    # Compute Engagement Rate (safe division)
    df['engagement_rate'] = df['engagement'] / df['impressions'].replace(0, pd.NA)
    
    # SECTION 2: Top-Level Metrics
    st.markdown("### 📊 Post-Level Summary Metrics")
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    
    with kpi_col1:
        st.metric("Total Posts", f"{len(df):,}")
    
    with kpi_col2:
        st.metric("Total Impressions", f"{int(df['impressions'].sum()):,}")
    
    with kpi_col3:
        avg_er = df['engagement_rate'].mean()
        st.metric("Avg Engagement Rate", f"{avg_er:.1%}" if pd.notna(avg_er) else "N/A")


    # SECTION 3: Top Performing Posts Table
    st.markdown("### 🔥 Top Performing Posts")
    st.dataframe(pd.DataFrame({
        "Post ID": [101, 102, 103],
        "Impressions": [12000, 11500, 11000],
        "Engagements": [540, 470, 460],
        "Engagement Rate": ["4.5%", "4.1%", "4.2%"],
        "Posted On": ["2024-04-10", "2024-04-08", "2024-04-06"]
    }))

    # SECTION 4: Engagement Breakdown
    st.markdown("### 📈 Engagement Over Time")
    col4, col5 = st.columns([2, 1])
    with col4:
        # Placeholder line chart
        st.plotly_chart(px.line(
            x=pd.date_range(start="2024-04-01", periods=10),
            y=[100, 200, 180, 250, 300, 280, 260, 240, 210, 190],
            labels={"x": "Date", "y": "Engagements"},
            title="Daily Engagement Trend",
            template="plotly_white"
        ), use_container_width=True)
    with col5:
        st.markdown("##### Engagement by Type")
        st.bar_chart(pd.DataFrame({
            "Likes": [400],
            "Comments": [120],
            "Shares": [80],
            "Saves": [40]
        }))

    # SECTION 5: Creative Analysis
    st.markdown("### 🖼️ Creative Insights (Placeholder)")
    st.markdown("This section will show analysis of creative hooks, CTA clarity, tone, and text presence.")

if __name__ == "__main__":
    main()
