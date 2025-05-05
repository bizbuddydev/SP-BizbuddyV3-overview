import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

def main():
    st.set_page_config(page_title="Social Post Breakdown", layout="wide")
    st.title("📱 Social Post Breakdown")

    # SECTION 1: Filters
    st.markdown("### 🔧 Filter Options")
    col1, col2, col3 = st.columns(3)
    with col1:
        platform = st.selectbox("Platform", ["Instagram", "Facebook"])
    with col2:
        content_type = st.selectbox("Content Type", ["Image", "Video", "Carousel", "Reel"])
    with col3:
        date_range = st.date_input("Date Range")

    # SECTION 2: Top-Level Metrics
    st.markdown("### 📊 Post-Level Summary Metrics")
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    with kpi_col1:
        st.metric("Total Posts", "62")
    with kpi_col2:
        st.metric("Total Impressions", "230,000")
    with kpi_col3:
        st.metric("Avg Engagement Rate", "3.2%")

    # SECTION 3: Top Performing Posts Table
    st.markdown("### 🔥 Top Performing Posts")
    st.dataframe(pd.DataFrame({
        "Post ID": [101, 102, 103],
        "Impressions": [12000, 11500, 11000],
        "Engagements": [540, 470, 460],
        "Engagement Rate": ["4.5%", "4.1%", "4.2%"],
        "Posted On": ["2024-04-10", "2024-04-08", "2024-04-06"]
    }))

    # SECTION 4: Engagement Breakdown
    st.markdown("### 📈 Engagement Over Time")
    col4, col5 = st.columns([2, 1])
    with col4:
        # Placeholder line chart
        st.plotly_chart(px.line(
            x=pd.date_range(start="2024-04-01", periods=10),
            y=[100, 200, 180, 250, 300, 280, 260, 240, 210, 190],
            labels={"x": "Date", "y": "Engagements"},
            title="Daily Engagement Trend",
            template="plotly_white"
        ), use_container_width=True)
    with col5:
        st.markdown("##### Engagement by Type")
        st.bar_chart(pd.DataFrame({
            "Likes": [400],
            "Comments": [120],
            "Shares": [80],
            "Saves": [40]
        }))

    # SECTION 5: Creative Analysis
    st.markdown("### 🖼️ Creative Insights (Placeholder)")
    st.markdown("This section will show analysis of creative hooks, CTA clarity, tone, and text presence.")

if __name__ == "__main__":
    main()
