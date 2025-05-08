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

# Sample mock data
def get_sample_data():
    dates = pd.date_range(start="2024-04-01", periods=14)
    data = pd.DataFrame({
        "Date": dates.tolist() * 3,
        "Campaign": ["A"]*14 + ["B"]*14 + ["C"]*14,
        "Ad Set": ["Set 1", "Set 2", "Set 3"] * 14,
        "Placement": ["Feed", "Stories", "Reels"] * 14,
        "Location": ["NY", "CA", "TX"] * 14,
        "Spend": [100 + i for i in range(14)] * 3,
        "Impressions": [10000 + i * 100 for i in range(14)] * 3,
        "Clicks": [200 + i * 10 for i in range(14)] * 3,
        "Conversions": [10 + (i % 5) for i in range(14)] * 3,
    })
    data["CTR"] = (data["Clicks"] / data["Impressions"]) * 100
    data["CPA"] = data["Spend"] / data["Conversions"]
    return data


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

# Layout
def main():

    basic_ad_df, basic_adset_df, basic_campaign_df, basic_demo_df, basic_ig_df, ig_account_df, pa_df = get_data()

    df = get_sample_data()

    st.title("📊 Ad Performance Overview")

    # === REAL DATA SOURCES (assumed already loaded globally) ===
    # basic_campaign_df, basic_adset_df, basic_ad_df, basic_demo_df

    # === Breakdown options mapping ===
    breakdown_options = {
        "Campaign": {
            "df": basic_campaign_df,
            "group_col": "campaign_name"
        },
        "Ad Set": {
            "df": basic_adset_df,
            "group_col": "adset_name"
        },
        "Ad": {
            "df": basic_ad_df,
            "group_col": "ad_name"
        },
        "Age": {
            "df": basic_demo_df,
            "group_col": "Group",
            "filter_on": "Breakdown",
            "filter_value": "Age"
        },
        "Region": {
            "df": basic_demo_df,
            "group_col": "Group",
            "filter_on": "Breakdown",
            "filter_value": "Region"
        }
    }

    # === User selects breakdown ===
    selected_breakdown = st.selectbox("Break down by:", list(breakdown_options.keys()))
    breakdown_info = breakdown_options[selected_breakdown]
    df = breakdown_info["df"].copy()
    group_col = breakdown_info["group_col"]
    
    # === Optional demo filtering ===
    if "filter_on" in breakdown_info:
        df = df[df[breakdown_info["filter_on"]] == breakdown_info["filter_value"]]
    
    selected_dates = st.date_input("Select date range:", [min_date, max_date])

    # Convert to list if needed
    if isinstance(selected_dates, (datetime, pd.Timestamp)):
        # Only one date was selected (or returned), treat it as both start and end
        start_date = end_date = selected_dates
    elif isinstance(selected_dates, (list, tuple)) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        st.warning("Please select a valid date range.")
        return  # stop the app gracefully if date range is invalid

    
    # === Multiselect breakdown filter ===
    group_values = df[group_col].dropna().unique().tolist()
    with st.expander(f"🔍 Filter by {selected_breakdown} values", expanded=False):
        selected_groups = st.multiselect(
            f"Select one or more {selected_breakdown} values:",
            options=group_values,
            default=group_values
        )
    df = df[df[group_col].isin(selected_groups)]


    # === KPI summary ===
    st.markdown("### 📌 Summary Metrics")
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("Total Spend", f"${df['spend'].sum():,.0f}")
    with kpi_col2:
        st.metric("Total Impressions", f"{df['impressions'].sum():,.0f}")
    with kpi_col3:
        st.metric("Total Link Clicks", f"{df['inline_link_clicks'].sum():,.0f}")

    # === Time Series Chart ===
    st.markdown("### 📈 Spend Over Time")
    daily_summary = (
        df.groupby(["date", group_col])
        .agg({
            "spend": "sum",
            "impressions": "sum",
            "inline_link_clicks": "sum"
        })
        .reset_index()
    )

    fig = px.line(
        daily_summary,
        x="date",
        y="spend",
        color=group_col,
        title=f"Spend Over Time by {selected_breakdown}",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### 📊 General Performance Overview")

    # Dimension selection
    dimension_options = ["Campaign", "Ad Set", "Placement", "Location"]
    selected_dimension = st.selectbox("Break down by:", dimension_options)

    # Dynamic value filter for the selected dimension
    unique_values = df[selected_dimension].unique().tolist()
    selected_value = st.selectbox(f"Filter by {selected_dimension}:", ["All"] + unique_values)

    # Apply filter if not 'All'
    if selected_value != "All":
        df = df[df[selected_dimension] == selected_value]

    # --- Date Filter ---
    st.markdown("### 📅 Filter by Date")
    min_date = df["Date"].min()
    max_date = df["Date"].max()
    start_date, end_date = st.date_input("Select date range:", [min_date, max_date])

    # Filter by date
    df = df[(df["Date"] >= pd.to_datetime(start_date)) & (df["Date"] <= pd.to_datetime(end_date))]

    # --- KPI summary ---
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("Total Spend", f"${df['Spend'].sum():,.0f}")
    with kpi_col2:
        st.metric("Total Impressions", f"{df['Impressions'].sum():,.0f}")
    with kpi_col3:
        st.metric("Total Conversions", f"{df['Conversions'].sum():,.0f}")

    # --- Time Series + Table Side-by-Side ---
    st.markdown("### 📈 Performance Over Time")
    left_col, right_col = st.columns([2, 1])

    daily_summary = (
        df.groupby("Date")
        .agg({
            "Spend": "sum",
            "Impressions": "sum",
            "Clicks": "sum",
            "Conversions": "sum",
            "CTR": "mean",
            "CPA": "mean"
        })
        .reset_index()
    )

    with left_col:
        fig = px.line(
            daily_summary,
            x="Date",
            y="Spend",
            title="Daily Spend",
            template="plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)

    with right_col:
        st.markdown("#### Daily Metrics")
        st.dataframe(daily_summary.style.format({
            "Spend": "${:,.0f}",
            "Impressions": "{:,.0f}",
            "Clicks": "{:,.0f}",
            "Conversions": "{:,.0f}",
            "CTR": "{:.2f}%",
            "CPA": "${:.2f}"
        }))

        st.markdown("---")
    st.markdown("### 🔍 Granular Breakdown")

    col_left, col_right = st.columns(2)

    # --- LEFT: Demographic Breakdown ---
    with col_left:
        st.subheader("Demographic Breakdown")

        breakdown_options = ["Age", "Gender", "Location", "Placement"]
        selected_demo = st.selectbox("Select Demographic Dimension:", breakdown_options)

        # Simulate demographic values (normally this would be broken out)
        # For demo purposes, we'll just randomly assign categories
        import numpy as np
        np.random.seed(42)
        df_demo = df.copy()
        df_demo[selected_demo] = np.random.choice(["Group A", "Group B", "Group C"], size=len(df))

        grouped_demo = (
            df_demo.groupby(selected_demo)
            .agg({
                "Spend": "sum",
                "Impressions": "sum",
                "Clicks": "sum",
                "Conversions": "sum",
                "CTR": "mean",
                "CPA": "mean"
            })
            .reset_index()
        )

        fig_demo = px.bar(
            grouped_demo,
            x=selected_demo,
            y="Conversions",
            color=selected_demo,
            title=f"Conversions by {selected_demo}",
            template="plotly_white"
        )
        st.plotly_chart(fig_demo, use_container_width=True)

    # --- RIGHT: Delivery Insights ---
    with col_right:
        st.subheader("Delivery Insights")

        # Static or sample delivery insights
        st.markdown("**Learning Phase**")
        st.info("2 of 6 ad sets are still in the learning phase.")

        st.markdown("**Auction Overlap Rate**")
        st.warning("High overlap detected in Ad Set B and C.")

        st.markdown("**Ad Fatigue Risk**")
        st.success("No signs of creative fatigue.")

        st.markdown("**Quality Ranking**")
        st.write("- Ad A: Above Average")
        st.write("- Ad B: Average")
        st.write("- Ad C: Below Average")

        st.markdown("---")
    st.markdown("### 🎨 Creative Performance Breakdown")

    col_left, col_right = st.columns(2)

    # --- LEFT: Boosted Post vs Ad ---
    with col_left:
        st.subheader("Boosted Post vs Ad")

        st.markdown("**Boosted Post Performance**")
        st.markdown("- Impressions: 25,000")
        st.markdown("- Clicks: 950")
        st.markdown("- CTR: 3.8%")
        st.markdown("- Conversions: 72")

        st.markdown("**Ad Performance**")
        st.markdown("- Impressions: 30,000")
        st.markdown("- Clicks: 870")
        st.markdown("- CTR: 2.9%")
        st.markdown("- Conversions: 88")

        st.info("✅ Boosted Post had higher CTR, but the Ad drove more conversions.")

    # --- RIGHT: Video Watch-Through Rate ---
    with col_right:
        st.subheader("Video Completion Rates")

        st.markdown("Placeholder chart showing % of users who watched:")
        st.markdown("- 25% of video")
        st.markdown("- 50% of video")
        st.markdown("- 75% of video")
        st.markdown("- 100% of video")

        # Placeholder stacked bar chart
        import plotly.graph_objects as go
        fig_vid = go.Figure(data=[
            go.Bar(name='Video A', x=['25%', '50%', '75%', '100%'], y=[1000, 800, 500, 300]),
            go.Bar(name='Video B', x=['25%', '50%', '75%', '100%'], y=[1100, 900, 700, 400])
        ])
        fig_vid.update_layout(
            barmode='group',
            title="Watch-Through Rates by Video",
            template="plotly_white"
        )
        st.plotly_chart(fig_vid, use_container_width=True)



if __name__ == "__main__":
    main()
