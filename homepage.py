# ads_campaigns
#     └── ads_adsets
#             └── ads_ads
#                     └── ads_creatives
#                         ↘️   ig_posts (if object_story_id = media_id)

# ads_ads
#     └── ads_insights

# ig_posts
#     └── ig_post_insights

# ig_stories
#     └── ig_story_insights


import streamlit as st
import pandas as pd
from google.cloud import bigquery  # If you're using BigQuery
import requests  # If you're calling the Graph API directly
import json
from google.oauth2 import service_account

st.set_page_config(page_title="SP Bizz Overview", layout="wide", page_icon="📊")

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

    #Get ig posts
    ig_dataset_id = "instagram_business_instagram_business"
    ig_table_id = "instagram_business__posts"
    basic_ig_df = pull_ig_insights(ig_dataset_id, ig_table_id)

    #Get analyzed posts
    client_dataset_id = "client"
    client_table_id = "sp_analyzed_posts"
    pa_df = pull_post_analysis(client_dataset_id, client_table_id)

    #return all dfs
    return basic_ad_df, basic_adset_df, basic_campaign_df, basic_ig_df, pa_df


# Main Streamlit app
def main():
    st.title("Meta Graph API Dashboard")
    st.write("This app displays data pulled from the Meta Graph API.")

    #Get data
    basic_ad_df, basic_adset_df, basic_campaign_df, basic_ig_df, pa_df = get_data()

    #Build Basic Scorecards
    ad_overview, post_overview = st.columns(2)
    with ad_overview:
        st.subheader("Recent Ad Performance")
        st.write("Last 30 Days")
        # Create 3 equally spaced columns
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(label="Total Impressions", value="1.2M", delta="+5%")

        with col2:
            st.metric(label="Click-Through Rate", value="2.1%", delta="-0.3%")

        with col3:
            st.metric(label="Conversions", value="8,213", delta="+12%")

    with post_overview:
        st.subheader("Recent Organic Performance")
        st.write("Last 30 Days")


if __name__ == "__main__":
    main()
