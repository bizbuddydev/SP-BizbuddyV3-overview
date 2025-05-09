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
from collections import defaultdict
import statsmodels.api as sm

# Set page components
st.set_page_config(page_title="SP Bizz Overview", layout="wide", page_icon="📱")

# Define links to other pages
PAGES = {
    "📊 Overview": "https://sp-bizz-overview.streamlit.app/",
    "🪧 Meta Ads Breakdown": "https://sp-bizz-ad.streamlit.app/",
    "📱 Organic Ig Breakdown": "https://sp-bizz-organic.streamlit.app/",
}

# Sidebar navigation
st.sidebar.title("Navigation")
for page, url in PAGES.items():
    st.sidebar.markdown(f"[**{page}**]({url})", unsafe_allow_html=True)

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


def compute_hashtag_performance(df, hashtag_col='hashtags', metric_col='reach'):
    performance_dict = defaultdict(list)

    for idx, row in df.iterrows():
        hashtags = row.get(hashtag_col)
        metric_value = row.get(metric_col)

        # Try to parse stringified lists
        if isinstance(hashtags, str):
            try:
                hashtags = ast.literal_eval(hashtags)
            except Exception:
                hashtags = []

        if isinstance(hashtags, list) and pd.notnull(metric_value):
            for tag in hashtags:
                performance_dict[str(tag).lower()].append(metric_value)

    if not performance_dict:
        st.write("⚠️ No hashtags matched or none had valid reach values.")
        return pd.DataFrame(columns=['hashtag', 'count', f'avg_{metric_col}'])

    result = pd.DataFrame([
        {'hashtag': tag, 'count': len(values), f'avg_{metric_col}': np.mean(values)}
        for tag, values in performance_dict.items()
    ])

    return result.sort_values(by=f'avg_{metric_col}', ascending=False).reset_index(drop=True)


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
    
    # Normalize timestamps
    df['post_date'] = pd.to_datetime(df['created_timestamp']).dt.normalize()
    post_lines = df[['post_date', 'post_caption']].drop_duplicates().copy()
    
    # Create hover text: Date + start of caption
    post_lines['hover'] = post_lines.apply(
        lambda row: f"{row['post_date'].strftime('%b %d, %Y')}<br>{row['post_caption'][:50]}..." if pd.notna(row['post_caption']) else f"{row['post_date'].strftime('%b %d, %Y')}<br>No caption",
        axis=1
    )
    
    # Add full-height vertical lines
    for date in post_lines['post_date']:
        fig.add_vline(
            x=date,
            line_dash="dot",
            line_color="gray",
            opacity=0.3
        )
    
    # Add invisible scatter points for hovertext
    fig.add_trace(go.Scatter(
        x=post_lines['post_date'],
        y=[plot_df['Value'].max()] * len(post_lines),
        mode="markers",
        marker=dict(size=8, color="rgba(0,0,0,0)"),
        hovertext=post_lines['hover'],
        hoverinfo="text",
        showlegend=False
    ))
    st.plotly_chart(fig, use_container_width=True)


    # SECTION 5: Creative Analysis
    st.markdown("### Creative Insights")
    st.write("Below is data extracted from videos and Reels on this account. Full post analysis is coming soon as we continue development.")
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown("#### 📊 Performance by Creative Element")
        
        creative_options = {
            "Post Theme": "general_theme",
            "Main Imagery": "imagery_group",
            "Background Imagery": "background_imagery"
        }
    
        selected_creative = st.selectbox("Break down reach by:", list(creative_options.keys()))
        creative_col = creative_options[selected_creative]
    
        if creative_col in pa_df.columns and 'video_photo_reach' in pa_df.columns:
            reach_summary = (
                pa_df
                .groupby(creative_col)
                .agg(Average_Reach=('video_photo_reach', 'mean'), Post_Count=('video_photo_reach', 'count'))
                .reset_index()
                .rename(columns={creative_col: selected_creative})
                .sort_values('Average_Reach', ascending=False)
            )

    
            fig_creative = px.bar(
                reach_summary,
                x=selected_creative,
                y='Average_Reach',
                hover_data={'Post_Count': True},
                title=f"Average Reach by {selected_creative}",
                template='plotly_white'
            )
            st.plotly_chart(fig_creative, use_container_width=True)
        else:
            st.info("Creative column or reach data missing in `pa_df`.")
    
    with col_right:
        # Make sure all needed columns exist
        X_OPTIONS = ['video_len', 'shot_count', 'object_count', 'caption_length', 'avg_shot_len']
        st.markdown("### 🎥 Reach vs. Creative Attributes")
        
        selected_x = st.selectbox("Choose a variable to compare with Reach:", X_OPTIONS)
        
        # Filter out rows with missing data in either selected or reach
        filtered_df = pa_df[[selected_x, 'video_photo_reach']].dropna()
        
        fig = px.scatter(
            filtered_df,
            x=selected_x,
            y='video_photo_reach',
            trendline='ols',
            title=f"Reach vs. {selected_x}",
            labels={selected_x: selected_x.replace('_', ' ').title(), 'reach': 'Reach'},
            opacity=0.7
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
if __name__ == "__main__":
    main()
