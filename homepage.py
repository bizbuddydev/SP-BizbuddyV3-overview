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
    ig_account_df = pull_ig_account_insights(ig_dataset_id, ig_table_id)

    #Get analyzed posts
    client_dataset_id = "client"
    client_table_id = "sp_analyzed_posts"
    pa_df = pull_post_analysis(client_dataset_id, client_table_id)

    #return all dfs
    return basic_ad_df, basic_adset_df, basic_campaign_df, basic_demo_df, basic_ig_df, ig_account_df, pa_df

# Sample data
bar_data = pd.DataFrame({
    'Day': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
    'Metric A': [10, 12, 8, 15, 9, 7, 11],
    'Metric B': [8, 10, 6, 14, 11, 9, 10]
})

pie_data = {
    'Category': ['Category A', 'Category B', 'Category C', 'Category D'],
    'Value': [40, 25, 20, 15]
}
# Sample data
value = 1245
delta = "+5%"
spark_data = [900, 950, 1100, 1230, 1245]

# Sample data
follower_data = pd.DataFrame({
    'Date': pd.date_range(start='2024-04-01', periods=14, freq='D'),
    'Followers': [1020, 1040, 1055, 1068, 1075, 1090, 1102, 1115, 1130, 1142, 1155, 1170, 1180, 1190]
})

def draw_metric_card(label, value, delta, spark_data, color="green"):
    col1, col2 = st.columns([1, 2])  # Label/value vs sparkline

    with col1:
        st.markdown(f"**{label}**")
        st.markdown(f"<h3 style='margin-bottom: 0'>{value}</h3>", unsafe_allow_html=True)
        st.markdown(f"<span style='color: {color};'>{delta}</span>", unsafe_allow_html=True)

    with col2:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=spark_data,
            mode='lines',
            line=dict(color=color, width=2),
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
        st.plotly_chart(fig, use_container_width=True)

# Main Streamlit app
def main():

    st.title("Stay Pineapple Social Performance Dash")

    #Get data
    basic_ad_df, basic_adset_df, basic_campaign_df, basic_demo_df, basic_ig_df, ig_account_df, pa_df = get_data()

    # Normalize today and get 30 days ago as a date (not Timestamp)
    today = pd.to_datetime("today").normalize()
    last_30_days = (today - timedelta(days=30)).date()  # <-- this is key
    
    # Convert IG datetime to date
    basic_ig_df['date'] = pd.to_datetime(basic_ig_df['created_timestamp']).dt.date
    
    # Convert ad datetime to date (if needed)
    basic_ad_df['date'] = pd.to_datetime(basic_ad_df['date']).dt.date
    
    # Now this comparison will work
    basic_ad_df = basic_ad_df[basic_ad_df["date"] >= last_30_days]
    basic_ig_df = basic_ig_df[basic_ig_df["date"] >= last_30_days]

    #Build Basic Scorecards
    ad_overview, post_overview = st.columns(2)
    with ad_overview:
        st.subheader("Recent Ad Performance")
        st.write("Last 30 Days")
        # Create 3 equally spaced columns
        ad_sc1, ad_sc2, ad_sc3 = st.columns(3)

        with ad_sc1:
            impressions = int(basic_ad_df["impressions"].sum())
            st.metric(label="Total Impressions", value=f"{impressions:,}", delta="+5%")  # You can replace delta later

        with ad_sc2:
            total_clicks = basic_ad_df["inline_link_clicks"].sum()
            ctr = (total_clicks / impressions) * 100 if impressions > 0 else 0
            st.metric(label="Click-Through Rate", value=f"{ctr:.1f}%", delta="-0.3%")

        with ad_sc3:
            conversions = int(basic_ad_df["spend"].sum())
            st.metric(label="Spend", value=f"{conversions:,}", delta="+12%")


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
        draw_metric_card("Clicks", 1245, "+5%", [900, 950, 1100, 1230, 1245], "green")
        draw_metric_card("Leads", 300, "-3%", [320, 310, 305, 295, 300], "red")
        draw_metric_card("Revenue", "$9.4K", "+12%", [8000, 8400, 8800, 9200, 9400], "blue")
        
    with col4:
        st.subheader("Follower Count")
        # Create the line chart
        fig = px.line(
            follower_data,
            x='Date',
            y='Followers',
            title='Follower Growth Over Time',
            markers=True,
            template='plotly_white'
        )
        
        fig.update_layout(
            height=400,
            margin=dict(l=10, r=10, t=40, b=10),
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
