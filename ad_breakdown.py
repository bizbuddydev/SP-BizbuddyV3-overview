import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

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

# Layout
def main():
    st.set_page_config(page_title="Ad Performance Dashboard", layout="wide")
    st.title("Ad Performance Overview")

    df = get_sample_data()

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

    # KPI summary
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("Total Spend", f"${df['Spend'].sum():,.0f}")
    with kpi_col2:
        st.metric("Total Impressions", f"{df['Impressions'].sum():,.0f}")
    with kpi_col3:
        st.metric("Total Conversions", f"{df['Conversions'].sum():,.0f}")

    # Time series chart
    st.markdown("### 📈 Performance Over Time")
    fig = px.line(
        df.groupby("Date").sum(numeric_only=True).reset_index(),
        x="Date",
        y="Spend",
        title="Spend Over Time",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
