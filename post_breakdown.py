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
def pull_follows_data(dataset_id, table_id):
    # Build the table reference
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    # Query to fetch all data from the table
    query = f"SELECT * FROM `{table_ref}` WHERE ig_id = 779159629;"
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

    ig_follows_dataset_id = "client"
    ig_follows_table_id = "account_info"
    follows_df = pull_follows_data(ig_follows_dataset_id, ig_follows_table_id)

    #return all dfs
    return basic_ig_df, ig_account_df, pa_df, follows_df

def main():
    basic_ig_df, ig_account_df, pa_df, follows_df = get_data()
    st.title("📱 Social Post Breakdown")

    # --- SECTION 1: FILTERS ---
    st.markdown("### 🔧 Filter Options")
    col1, col2 = st.columns(2)

    with col1:
        # Dynamically load media types from actual data
        media_types = sorted(basic_ig_df['media_type'].dropna().unique())
        content_type = st.selectbox("Content Type", ["All"] + media_types)

    with col2:
        basic_ig_df['created_timestamp'] = pd.to_datetime(basic_ig_df['created_timestamp']).dt.date
        default_end = basic_ig_df['created_timestamp'].max()
        default_start = default_end - timedelta(days=30)
        selected_dates = st.date_input("Date Range", [default_start, default_end])

    # --- SECTION 2: SCORECARDS ---
    st.markdown("### 📊 Account Overview")
    sc1, sc2, sc3 = st.columns(3)

    with sc1:
        account_name = basic_ig_df['username'].iloc[0]
        st.metric("Account", account_name)

    with sc2:
        total_followers = follows_df.loc[follows_df['day_rank'] == 1, 'followers_count'].iloc[0]
        st.metric("Total Followers", f"{int(total_followers):,}" if pd.notna(total_followers) else "N/A")

    with sc3:
        media_count = follows_df.loc[follows_df['day_rank'] == 1, 'media_count'].iloc[0]
        st.metric("Media Count", f"{int(media_count):,}" if pd.notna(total_followers) else "N/A")

    # Filtered copy of post data
    df = basic_ig_df.copy()
    df['date'] = pd.to_datetime(df['created_timestamp']).dt.date

    account_df = ig_account_df.copy()
    account_df['follower_count'] = account_df['follower_count'].fillna(0)

    ig_post_df = basic_ig_df.copy()
    ig_post_df['posted_on'] = pd.to_datetime(ig_post_df['created_timestamp']).dt.strftime("%B %d, %Y")


    # Content type filtering
    if content_type != "All":
        df = df[df['media_type'].str.lower() == content_type.lower()]
        ig_post_df = ig_post_df[ig_post_df['media_type'].str.lower() == content_type.lower()]

    # Date filtering using standard date format
    start_date, end_date = None, None
    
    if isinstance(selected_dates, (list, tuple)) and len(selected_dates) == 2:
        start_date, end_date = selected_dates[0], selected_dates[1]
    elif isinstance(selected_dates, (datetime, pd.Timestamp)):
        start_date = end_date = selected_dates
    
    # Print debug info (optional)
    # st.write("Start:", start_date, "End:", end_date)
    
    if start_date and end_date:
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        account_df = account_df[(account_df['date'] >= start_date) & (account_df['date'] <= end_date)]
        ig_post_df = ig_post_df[(ig_post_df['created_timestamp'] >= start_date) & (ig_post_df['created_timestamp'] <= end_date)]
    else:
        st.warning("Invalid date selection.")
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
        follower_gain = account_df['follower_count'].sum() if 'follower_count' in ig_account_df.columns else 0
        st.metric("Followers Gained", f"{int(follower_gain):,}")
        st.markdown("<span style='font-size: 0.75em; color: gray;'>*Metric only tracks 2 months back</span>", unsafe_allow_html=True)

    with kpi4:
        total_likes = df['like_count'].sum()
        st.metric("Like Count", f"{int(total_likes):,}")

    with kpi5:
        avg_eng_rate = df['engagement_rate'].mean()
        st.metric("Engagement Rate", f"{avg_eng_rate:.1%}" if pd.notna(avg_eng_rate) else "N/A")

     # --- SECTION 3: Top Performing Posts Table ---
    st.markdown("### 🔥 Top Performing Posts")
    
    ig_post_df['engagement'] = (
        ig_post_df.get('like_count', 0) +
        ig_post_df.get('comments_count', 0) +
        ig_post_df.get('save_count', 0)
    )

    ig_post_df['engagement_rate'] = ig_post_df['engagement'] / ig_post_df['video_photo_impressions'].replace(0, pd.NA)
    
    post_id_col = "post_id" if "post_id" in df.columns else "id"

    top_posts = (
        ig_post_df[['posted_on', 'post_caption', 'video_photo_reach', 'like_count', 'video_photo_saved']]
        .sort_values(by='video_photo_reach', ascending=False)
        .dropna(subset=['video_photo_reach'])
        .head(10)
        .copy()
    )

    top_posts.columns = ['Posted On', 'Caption', 'Reach', 'Likes', 'Saves']

    st.dataframe(top_posts.reset_index(drop=True))

    # --- SECTION 4: Engagement Breakdown ---
    st.markdown("### 📈 Engagement Over Time")
    
    # Metric selection dropdown
    metric_options = {
        "Reach": "video_photo_reach",
        "Likes": "like_count",
        "Saves": "video_photo_saved",
        "Engagement Rate": "engagement_rate",
        "Followers Gained": "follower_count"
    }
    selected_metric_label = st.selectbox("Metric to display:", list(metric_options.keys()), index=0)
    selected_metric_col = metric_options[selected_metric_label]
    
    # Prepare post-level metrics
    df['timestamp'] = pd.to_datetime(df['created_timestamp'])
    df['date'] = df['timestamp'].dt.date
    
    # Ensure engagement_rate exists if selected
    if selected_metric_label == "Engagement Rate":
        df['engagement'] = (
            df.get('like_count', 0) +
            df.get('comments_count', 0) +
            df.get('save_count', 0)
        )
        df['engagement_rate'] = df['engagement'] / df['video_photo_impressions'].replace(0, pd.NA)
    
    # Followers Gained comes from ig_account_df
    if selected_metric_label == "Followers Gained":
        follower_df = account_df.copy()
        follower_df['date'] = pd.to_datetime(follower_df['date']).dt.date
        follower_df = follower_df.groupby('date')[selected_metric_col].sum().reset_index()
        plot_df = follower_df.rename(columns={selected_metric_col: 'Value'})
    else:
        plot_df = df.groupby('date')[selected_metric_col].sum().reset_index()
        plot_df = plot_df.rename(columns={selected_metric_col: 'Value'})
    
    # Plot full-width chart
    fig = px.line(
        plot_df,
        x='date',
        y='Value',
        title=f"{selected_metric_label} Over Time",
        labels={"date": "Date", "Value": selected_metric_label},
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)


    # SECTION 5: Creative Analysis
    st.markdown("### 🖼️ Creative Insights (Placeholder)")
    st.markdown("This section will show analysis of creative hooks, CTA clarity, tone, and text presence.")

if __name__ == "__main__":
    main()
