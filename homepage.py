import streamlit as st
import pandas as pd
from google.cloud import bigquery  # If you're using BigQuery
import requests  # If you're calling the Graph API directly
import json

# Initialize BigQuery client
client = bigquery.Client()

from google.cloud import bigquery
import pandas as pd
import streamlit as st

# Initialize BigQuery client
client = bigquery.Client(project="bizbuddydemo-v3")

def fetch_table_data_by_page(table_id: str, page_id: str, limit: int = 1000):
    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE page_id = @page_id
        LIMIT {limit}
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("page_id", "STRING", page_id)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_id}: {e}")
        return pd.DataFrame()

# Main Streamlit app
def main():
    st.title("Meta Graph API Dashboard")
    st.write("This app displays data pulled from the Meta Graph API.")

    #Get basic ads
    table_id = "bizbuddydemo-v3.facebook_ads.basic_ad"
    page_id = 12101296
    st.write("Basic Ads Test")
    fetch_table_data_by_page(table_id, page_id)
    

if __name__ == "__main__":
    main()
