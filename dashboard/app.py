"""
Streamlit dashboard for KennelOS Analytics.

Run with:
  streamlit run dashboard/app.py
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Ensure project root is on sys.path so `from etl import db` works when
# Streamlit's current working directory is `dashboard/`.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl import db
import plotly.express as px


@st.cache_data
def load_table(table_name: str):
    engine = db.get_engine()
    try:
        df = pd.read_sql_table(table_name, engine)
        return df
    except Exception:
        return pd.DataFrame()


def main():
    st.title("KennelOS Analytics Dashboard")

    st.sidebar.header("Filters")
    table = st.sidebar.selectbox("Select table", ['daily_summary', 'pet_activities', 'environment', 'staff_logs'])

    df = load_table(table)
    if df.empty:
        st.warning(f"No data available for table '{table}'. Run the ETL pipeline first.")
        return

    st.subheader(f"{table.replace('_', ' ').title()}")
    st.dataframe(df.head(200))

    if table == 'daily_summary':
        st.header("Daily Trends")
        df['date'] = pd.to_datetime(df['date'])
        fig = px.line(df.sort_values('date'), x='date', y=['total_activities', 'total_activity_minutes'], markers=True)
        st.plotly_chart(fig, use_container_width=True)

    if table == 'pet_activities':
        st.header("Activity by Type")
        fig = px.histogram(df, x='activity_type', title='Activity Type Distribution')
        st.plotly_chart(fig, use_container_width=True)

    if table == 'environment':
        st.header("Environment Averages by Date")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        daily = df.groupby(df['timestamp'].dt.date).agg({'temperature_f':'mean','humidity_percent':'mean','noise_level_db':'mean'}).reset_index()
        daily = daily.rename(columns={'timestamp':'date'})
        fig = px.line(daily, x=daily.columns[0], y=daily.columns[1:], markers=True)
        st.plotly_chart(fig, use_container_width=True)

    if table == 'staff_logs':
        st.header("Staff Productivity")
        fig = px.box(df, x='shift_type', y='tasks_per_hour', points='all')
        st.plotly_chart(fig, use_container_width=True)


if __name__ == '__main__':
    main()
