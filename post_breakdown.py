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

    # --- SECTION 1: FILTERS ---
    st.markdown("### 🔧 Filter Options")
    col1, col2 = st.columns(2)

    with col1:
        # Dynamically load media types from actual data
        media_types = sorted(basic_ig_df['media_type'].dropna().unique())
        content_type = st.selectbox("Content Type", ["All"] + media_types)

    with col2:
        basic_ig_df['created_timestamp'] = pd.to_datetime(basic_ig_df['created_timestamp'])
        default_end = basic_ig_df['created_timestamp'].max()
        default_start = default_end - timedelta(days=30)
        selected_dates = st.date_input("Date Range", [default_start.date(), default_end.date()])

    # --- SECTION 2: SCORECARDS ---
    st.markdown("### 📊 Account Overview")
    sc1, sc2 = st.columns(2)

    with sc1:
        account_name = ig_account_df['username'].iloc[0] if 'username' in ig_account_df.columns else "Instagram Account"
        st.metric("Account", account_name)

    with sc2:
        followers_col = 'follower_count' if 'follower_count' in ig_account_df.columns else 'followers_count'
        latest_followers = ig_account_df[followers_col].iloc[-1] if followers_col in ig_account_df.columns else "N/A"
        st.metric("Total Followers", f"{int(latest_followers):,}" if pd.notna(latest_followers) else "N/A")

    # Filtered copy of post data
    df = basic_ig_df.copy()
    df['timestamp'] = pd.to_datetime(df['created_timestamp'])

    # Content type filtering
    if content_type != "All":
        df = df[df['media_type'].str.lower() == content_type.lower()]

    # Date filtering (with explicit .dt.date comparison)
    if not df.empty and isinstance(selected_dates, list) and len(selected_dates) == 2:
        start_date = pd.to_datetime(selected_dates[0]).date()
        end_date = pd.to_datetime(selected_dates[1]).date()
        df = df[df['timestamp'].dt.date.between(start_date, end_date)]
    else:
        st.warning("No data available for selected filters.")
        return

    # Clean engagement fields
    df['engagement'] = (
        df.get('like_count', 0) +
        df.get('comments_count', 0) +
        df.get('save_count', 0)
    )

    df['engagement_rate'] = df['video_photo_engagement'] / df['video_photo_reach'].replace(0, pd.NA)

    # --- SECOND ROW OF SCORECARDS (FILTERED) ---
    st.markdown("### 📈 Post Metrics")
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    with kpi1:
        post_count = df[df.get('is_story', False) != True]['post_id'].nunique()
        st.metric("Total Posts", f"{post_count:,}")

    with kpi2:
        total_reach = df['video_photo_reach'].sum()
        st.metric("Total Reach", f"{int(total_reach):,}")

    with kpi3:
        follower_gain = ig_account_df['follower_count'].diff().sum() if 'follower_count' in ig_account_df.columns else 0
        st.metric("Followers Gained", f"{int(follower_gain):,}")

    with kpi4:
        total_likes = df['like_count'].sum()
        st.metric("Like Count", f"{int(total_likes):,}")

    with kpi5:
        avg_eng_rate = df['engagement_rate'].mean()
        st.metric("Engagement Rate", f"{avg_eng_rate:.1%}" if pd.notna(avg_eng_rate) else "N/A")


    # SECTION 3: Top Performing Posts Table
    st.markdown("### 🔥 Top Performing Posts")
    
    # Use correct column name for post ID
    post_id_col = "post_id" if "post_id" in df.columns else "id"
    
    # Compute engagement if not already done
    df['engagement'] = (
        df.get('like_count', 0) +
        df.get('comments_count', 0) +
        df.get('save_count', 0)
    )
    df['engagement_rate'] = df['engagement'] / df['video_photo_impressions'].replace(0, pd.NA)
    df['posted_on'] = pd.to_datetime(df['created_timestamp']).dt.date
    
    # Select top 10 by engagement
    top_posts = (
        df[[post_id_col, 'video_photo_impressions', 'engagement', 'engagement_rate', 'posted_on']]
        .sort_values(by='engagement', ascending=False)
        .dropna(subset=['video_photo_impressions'])
        .head(10)
        .copy()
    )
    
    # Format nicely
    top_posts['engagement_rate'] = top_posts['engagement_rate'].apply(lambda x: f"{x:.1%}")
    
    # Rename for display
    top_posts.columns = ['Post ID', 'Impressions', 'Engagements', 'Engagement Rate', 'Posted On']
    
    # Show table
    st.dataframe(top_posts.reset_index(drop=True))

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
