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

# Basic Ad Data
def pull_ad_data(dataset_id, table_id):
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` WHERE account_id = {FB_PAGE_ID}"
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
    #Get basic ads
    ad_dataset_id = "facebook_ads"
    ad_table_id = "basic_ad"
    basic_ad_df = pull_ad_data(ad_dataset_id, ad_table_id)

    #Get ad set
    adset_dataset_id = "facebook_ads"
    adset_table_id = "basic_ad_set"
    basic_adset_df = pull_ad_data(adset_dataset_id, adset_table_id)

    #Get ad set
    campaign_dataset_id = "facebook_ads"
    campaign_table_id = "basic_campaign"
    basic_campaign_df = pull_ad_data(campaign_dataset_id, campaign_table_id)

    #Get demo set
    demo_dataset_id = "client"
    demo_table_id = "ad_demographics"
    basic_demo_df = pull_ad_data(demo_dataset_id, demo_table_id)

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
    return basic_ad_df, basic_adset_df, basic_campaign_df, basic_demo_df, basic_ig_df, ig_account_df, pa_df

def main():
    st.set_page_config(page_title="Social Post Breakdown", layout="wide")
    st.title("📱 Social Post Breakdown")

    # SECTION 1: Filters
    st.markdown("### 🔧 Filter Options")
    col1, col2, col3 = st.columns(3)
    with col1:
        platform = st.selectbox("Platform", ["Instagram", "Facebook"])
    with col2:
        content_type = st.selectbox("Content Type", ["Image", "Video", "Carousel", "Reel"])
    with col3:
        date_range = st.date_input("Date Range")

    # SECTION 2: Top-Level Metrics
    st.markdown("### 📊 Post-Level Summary Metrics")
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("Total Posts", "62")
    with kpi_col2:
        st.metric("Total Impressions", "230,000")
    with kpi_col3:
        st.metric("Avg Engagement Rate", "3.2%")

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
