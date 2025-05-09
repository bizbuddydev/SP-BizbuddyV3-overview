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


def draw_metric_card_from_df(df, metric_col, label, color="green", days=30):
    """
    Draws a Streamlit metric card with a sparkline and 30-day period-over-period delta.

    Args:
        df: DataFrame with 'date' column and the metric.
        metric_col: Column name to use for the metric.
        label: Metric label to display.
        color: Sparkline color.
        days: Number of days per period (default 30).
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    # Get today and two time windows
    today = df['date'].max()
    start_current = today - timedelta(days=days)
    start_previous = start_current - timedelta(days=days)

    # Filter for current and previous periods
    current_period = df[(df['date'] > start_current) & (df['date'] <= today)]
    previous_period = df[(df['date'] > start_previous) & (df['date'] <= start_current)]

    # Calculate sums or averages
    current_value = current_period[metric_col].sum()
    previous_value = previous_period[metric_col].sum()

    # Handle divide-by-zero case
    if previous_value == 0:
        delta_pct = 0
    else:
        delta_pct = ((current_value - previous_value) / previous_value) * 100

    delta_text = f"{delta_pct:+.1f}%"
    # Build sparkline from daily values over current period
    spark_data = (
        current_period
        .groupby('date')[metric_col]
        .sum()
        .sort_index()
    )

    # Draw card
    col1, col2 = st.columns([1, 2])

    with col1:
        st.markdown(f"**{label}**")
        st.markdown(f"<h3 style='margin-bottom: 0'>{int(current_value):,}</h3>", unsafe_allow_html=True)
        st.markdown(f"<span style='color: {color};'>{delta_text}</span>", unsafe_allow_html=True)

    with col2:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=spark_data.index,
            y=spark_data.values,
            mode='lines',
            line=dict(color="blue", width=2),
            showlegend=False
        ))
        fig.update_layout(
            height=60,
            margin=dict(l=0, r=0, t=0, b=0),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True, key=f"{label}_sparkline")


# Main Streamlit app
def main():

    st.title("Stay Pineapple Social Performance Dash")

    # Get data
    basic_ad_df, basic_adset_df, basic_campaign_df, basic_demo_df, basic_ig_df, ig_account_df, pa_df = get_data()

    # Normalize today and define time periods
    today = pd.to_datetime("today").normalize()
    last_30_days = (today - timedelta(days=30)).date()
    prev_30_days = (today - timedelta(days=60)).date()

    # Convert dates
    basic_ad_df['date'] = pd.to_datetime(basic_ad_df['date']).dt.date
    basic_ig_df['date'] = pd.to_datetime(basic_ig_df['created_timestamp']).dt.date

    # Filter into current and previous periods
    basic_ad_df = basic_ad_df[basic_ad_df["date"] >= last_30_days]
    ad_previous = basic_ad_df[(basic_ad_df["date"] < last_30_days) & (basic_ad_df["date"] >= prev_30_days)]

    # Filter into current and previous periods
    basic_ig_df = basic_ig_df[basic_ig_df["date"] >= last_30_days]
    basic_ig_df = basic_ig_df[(basic_ig_df["date"] < last_30_days) & (basic_ig_df["date"] >= prev_30_days)]

    # Build Basic Scorecards
    ad_overview, post_overview = st.columns(2)

    with ad_overview:
        st.subheader("Recent Ad Performance")
        st.write("Last 30 Days")

        ad_sc1, ad_sc2, ad_sc3 = st.columns(3)

        with ad_sc1:
            current_impressions = basic_ad_df["impressions"].sum()
            previous_impressions = ad_previous["impressions"].sum()
            delta_impressions = ((current_impressions - previous_impressions) / previous_impressions * 100) if previous_impressions > 0 else 0
            st.metric("Total Impressions", f"{int(current_impressions):,}", delta=f"{delta_impressions:+.1f}%")

        with ad_sc2:
            current_clicks = basic_ad_df["inline_link_clicks"].sum()
            previous_clicks = ad_previous["inline_link_clicks"].sum()
            current_ctr = (current_clicks / current_impressions * 100) if current_impressions > 0 else 0
            previous_ctr = (previous_clicks / previous_impressions * 100) if previous_impressions > 0 else 0
            delta_ctr = current_ctr - previous_ctr
            st.metric("Click-Through Rate", f"{current_ctr:.1f}%", delta=f"{delta_ctr:+.1f}%")

        with ad_sc3:
            current_spend = basic_ad_df["spend"].sum()
            previous_spend = ad_previous["spend"].sum()
            delta_spend = ((current_spend - previous_spend) / previous_spend * 100) if previous_spend > 0 else 0
            st.metric("Spend", f"${int(current_spend):,}", delta=f"{delta_spend:+.1f}%")


    with post_overview:
        st.subheader("Recent Organic Performance")
        st.write("Last 30 Days")
        # Create 3 equally spaced columns
        ig_sc1, ig_sc2, ig_sc3 = st.columns(3)

        with ig_sc1:
            total_posts = basic_ig_df["post_id"].nunique()  # Or use 'media_id' depending on schema
            st.metric(label="Total Posts", value=f"{total_posts:,}", delta="+7%")
        
        with ig_sc2:
            likes = basic_ig_df["like_count"].sum()
            st.metric(label="Like Count", value=f"{likes}", delta="-30%")
        
        with ig_sc3:
            post_comments = int(basic_ig_df.get("comment_count", pd.Series([0])).sum())  # Optional if exists
            st.metric(label="Comments", value=f"{post_comments:,}", delta="+4%")


    # Step 1: Ensure 'date' is datetime
    basic_ad_df['date'] = pd.to_datetime(basic_ad_df['date'])
    
    # Step 2: Group and compute CPC
    bar_data = basic_ad_df.groupby('date')[['spend', 'inline_link_clicks']].sum().reset_index()
    bar_data['CPC'] = bar_data['spend'] / bar_data['inline_link_clicks']
    
    # Step 3: Melt for bar chart
    bar_melted = bar_data.melt(id_vars='date', value_vars=['spend', 'inline_link_clicks'],
                               var_name='Metric', value_name='Value')

    col1, col2 = st.columns(2)
    
    # Step 4: Create dual-axis chart
    with col1:
        st.subheader("Bar + Line Chart: Daily Spend, Clicks, and CPC")
        
        fig = go.Figure()
    
        # Add bar traces
        for metric in ['spend', 'inline_link_clicks']:
            df_metric = bar_melted[bar_melted['Metric'] == metric]
            fig.add_trace(go.Bar(
                x=df_metric['date'],
                y=df_metric['Value'],
                name=metric,
                yaxis='y1'
            ))
    
        fig.add_trace(go.Scatter(
        x=bar_data['date'],
        y=bar_data['CPC'],
        name='CPC',
        mode='lines+markers',
        line=dict(color='green', width=3, shape='spline'),
        yaxis='y2'
        ))
        
        # Update layout to make the CPC axis tighter
        fig.update_layout(
            template='plotly_white',
            height=500,
            barmode='group',
            xaxis=dict(title="Date"),
            yaxis=dict(title="Spend / Clicks", side='left'),
            yaxis2=dict(
                title="CPC ($)",
                overlaying='y',
                side='right',
                showgrid=False,
                range=[0, bar_data['CPC'].max() * 1.4]  # tighter range to lift the line visually
            ),
            legend=dict(x=0.01, y=0.99)
        )
    
        st.plotly_chart(fig, use_container_width=True)
    
        with col2:
            st.subheader("Pie Chart: Demographic Breakdown")

            # Step 1: Let user choose Breakdown (dimension)
            breakdown_options = basic_demo_df['Breakdown'].unique().tolist()
            selected_breakdown = st.selectbox("Break down spend by:", breakdown_options)
            
            # Step 2: Filter data
            filtered_demo = basic_demo_df[basic_demo_df['Breakdown'] == selected_breakdown]
            
            # Step 3: Group and sum spend
            demo_summary = filtered_demo.groupby('Group')['spend'].sum().reset_index()
            demo_summary.columns = ['Category', 'Value']
            
            # Step 4: Plot
            fig2 = px.pie(
                demo_summary,
                names='Category',
                values='Value',
                height=500,
                template='plotly_white',
                title=f"Spend by {selected_breakdown}"
            )
            fig2.update_traces(textinfo='percent+label')
            fig2.update_traces(textinfo='none')  # disables labels on the pie slices
            st.plotly_chart(fig2, use_container_width=True)

    # Layout
    col3, col4 = st.columns([1, 2])
    
    with col3:
        st.subheader("Organic Performance")
        metric_col1 = "reach"
        metric_col2 = "follower_count"
        metric_col3 = "video_photo_saved"
        
        label1 = "Reach"
        label2 = "Followers Gained"
        label3 = "Saves"
        
        draw_metric_card_from_df(ig_account_df, metric_col1, label1, color="green", days=30)
        draw_metric_card_from_df(ig_account_df, metric_col2, label2, color="green", days=30)
        draw_metric_card_from_df(basic_ig_df, metric_col3, label3, color="green", days=30)
        
    with col4:
        st.subheader("Follower Count")

        today = ig_account_df['date'].max()
        start_current = today - timedelta(days=30)
    
        # Filter for current and previous periods
        current_period_df = ig_account_df[(ig_account_df['date'] > start_current) & (ig_account_df['date'] <= today)]
        
        current_period_df['Date'] = pd.to_datetime(current_period_df['date'])
        #ig_account_df['Follows'] = ig_account_df['follower_count']
        current_period_df = (current_period_df.groupby('Date', as_index=False)['follower_count'].max().sort_values('Date'))
        # Create the line chart
        fig3 = px.line(
            current_period_df,
            x='Date',
            y='follower_count',
            title='Follower Growth Over Time',
            markers=True,
            template='plotly_white'
        )
        
        fig3.update_layout(
            height=400,
            margin=dict(l=10, r=10, t=40, b=10),
        )
        
        # Display the chart
        st.plotly_chart(fig3, use_container_width=True)


if __name__ == "__main__":
    main()
