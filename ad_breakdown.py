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


if __name__ == "__main__":
    main()
