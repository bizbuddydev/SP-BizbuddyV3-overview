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
PAGE_ID = 12101296

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
    query = f"SELECT * FROM `{table_ref}` WHERE account_id = {PAGE_ID}"
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



# Main Streamlit app
def main():
    st.title("Meta Graph API Dashboard")
    st.write("This app displays data pulled from the Meta Graph API.")

    #Get basic ads
    ad_dataset_id = "facebook_ads"
    ad_table_id = "basic_ad"
    st.header("Basic Ads Test")
    basic_ad_df = pull_ad_data(ad_dataset_id, ad_table_id)
    st.dataframe(basic_ad_df)
    st.divider()

    #Get ad set
    adset_dataset_id = "facebook_ads"
    adset_table_id = "basic_ad_set"
    st.header("Basic Ad Set Test")
    basic_adset_df = pull_ad_data(dataset_id, table_id)
    st.dataframe(basic_adset_df)

if __name__ == "__main__":
    main()
