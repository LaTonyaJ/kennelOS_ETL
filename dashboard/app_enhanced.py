"""
KennelOS Analytics Dashboard - Enhanced with Comprehensive Analysis

This dashboard provides detailed insights into:
- Pet Wellness Metrics
- Environmental Monitoring  
- Operations Management

Run with:
  streamlit run dashboard/app.py
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Ensure project root is on sys.path so `from etl import db` works when
# Streamlit's current working directory is `dashboard/`.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl import db
import plotly.express as px

# Import analytics modules
try:
    from analytics import (
        PetWellnessAnalyzer, 
        EnvironmentalAnalyzer, 
        OperationsAnalyzer, 
        ChartBuilder
    )
except ImportError:
    st.error("Analytics modules not found. Please ensure the analytics package is properly installed.")
    st.stop()


@st.cache_data
def load_table(table_name: str):
    engine = db.get_engine()
    try:
        df = pd.read_sql_table(table_name, engine)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data
def load_all_data():
    """Load all required data for analytics."""
    return {
        'pet_activities': load_table('pet_activities'),
        'environment': load_table('environment'),
        'staff_logs': load_table('staff_logs'),
        'daily_summary': load_table('daily_summary')
    }


def display_metric_cards(metrics, title):
    """Display a row of metric cards."""
    st.subheader(title)
    cols = st.columns(len(metrics))
    
    for i, metric in enumerate(metrics):
        with cols[i]:
            st.metric(
                label=metric['title'],
                value=metric['value'],
                delta=metric.get('subtitle', None)
            )


def render_pet_wellness_dashboard(data):
    """Render Pet Wellness Analytics section."""
    st.header("🐾 Pet Wellness Analytics")
    
    if data['pet_activities'].empty:
        st.warning("No pet activity data available.")
        return
    
    # Initialize analyzer
    wellness_analyzer = PetWellnessAnalyzer(data['pet_activities'])
    chart_builder = ChartBuilder()
    
    # Analysis period selector
    col1, col2 = st.columns([1, 3])
    with col1:
        analysis_days = st.selectbox(
            "Analysis Period",
            options=[7, 14, 30, 90],
            index=2,
            format_func=lambda x: f"Last {x} days"
        )
    
    # Get analytics
    activity_analysis = wellness_analyzer.get_avg_activity_time_per_pet(days=analysis_days)
    feeding_analysis = wellness_analyzer.get_feeding_frequency_analysis(days=analysis_days)
    weight_analysis = wellness_analyzer.get_weight_trend_analysis(days=analysis_days)
    wellness_summary = wellness_analyzer.get_pet_wellness_summary()
    
    # Key Metrics
    metrics = [
        {
            'title': 'Total Pets',
            'value': wellness_summary['total_pets'],
            'subtitle': f"{len(activity_analysis)} active"
        },
        {
            'title': 'Activity Wellness Rate',
            'value': f"{wellness_summary['activity_wellness_rate']:.1f}%",
            'subtitle': 'Optimal activity levels'
        },
        {
            'title': 'Feeding Wellness Rate', 
            'value': f"{wellness_summary['feeding_wellness_rate']:.1f}%",
            'subtitle': 'Regular feeding schedule'
        },
        {
            'title': 'Avg Activity/Pet',
            'value': f"{activity_analysis['avg_daily_minutes'].mean():.0f} min",
            'subtitle': 'Daily average'
        }
    ]
    display_metric_cards(metrics, "Pet Wellness Overview")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Activity Levels by Pet")
        if not activity_analysis.empty:
            wellness_chart = chart_builder.create_pet_wellness_chart(activity_analysis)
            st.plotly_chart(wellness_chart, use_container_width=True)
    
    with col2:
        st.subheader("Activity Distribution")
        if not data['pet_activities'].empty:
            activity_chart = chart_builder.create_pet_activity_chart(data['pet_activities'])
            st.plotly_chart(activity_chart, use_container_width=True)
    
    # Detailed Tables
    with st.expander("📊 Detailed Pet Activity Analysis"):
        st.write("**Activity Time per Pet**")
        st.dataframe(activity_analysis)
        
        if not feeding_analysis.get('detailed_per_pet', pd.DataFrame()).empty:
            st.write("**Feeding Frequency Analysis**")
            st.dataframe(feeding_analysis['detailed_per_pet'])
    
    # Alerts and Recommendations
    pets_needing_attention = wellness_summary.get('pets_needing_attention', pd.DataFrame())
    if not pets_needing_attention.empty:
        st.warning("⚠️ Pets Requiring Attention")
        st.dataframe(pets_needing_attention[['pet_name', 'avg_daily_minutes', 'activity_status']])


def render_environmental_dashboard(data):
    """Render Environmental Analytics section."""
    st.header("🌡️ Environmental Insights")
    
    if data['environment'].empty:
        st.warning("No environmental data available.")
        return
    
    # Initialize analyzer
    env_analyzer = EnvironmentalAnalyzer(data['environment'])
    chart_builder = ChartBuilder()
    
    # Analysis period selector
    col1, col2 = st.columns([1, 3])
    with col1:
        analysis_days = st.selectbox(
            "Analysis Period",
            options=[7, 14, 30, 90],
            index=1,
            format_func=lambda x: f"Last {x} days",
            key="env_period"
        )
    
    # Get analytics
    temp_humidity = env_analyzer.get_temperature_humidity_averages(days=analysis_days)
    noise_analysis = env_analyzer.get_noise_level_alerts(days=analysis_days)
    
    if not data['pet_activities'].empty:
        correlation_analysis = env_analyzer.get_temperature_activity_correlation(
            data['pet_activities'], days=analysis_days
        )
    else:
        correlation_analysis = {}
    
    # Key Metrics
    overall_stats = temp_humidity['overall']
    metrics = [
        {
            'title': 'Avg Temperature',
            'value': f"{overall_stats['avg_temperature']:.1f}°F",
            'subtitle': f"±{overall_stats['temp_std']:.1f}°F"
        },
        {
            'title': 'Avg Humidity',
            'value': f"{overall_stats['avg_humidity']:.1f}%",
            'subtitle': f"±{overall_stats['humidity_std']:.1f}%"
        },
        {
            'title': 'Avg Noise Level',
            'value': f"{overall_stats['avg_noise']:.1f}dB",
            'subtitle': f"±{overall_stats['noise_std']:.1f}dB"
        },
        {
            'title': 'Noise Alerts',
            'value': noise_analysis['total_alerts'],
            'subtitle': f"{noise_analysis['alerts_per_day']:.1f}/day avg"
        }
    ]
    display_metric_cards(metrics, "Environmental Conditions")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Environmental Trends")
        env_trends_chart = chart_builder.create_environmental_trends_chart(data['environment'])
        st.plotly_chart(env_trends_chart, use_container_width=True)
    
    with col2:
        if correlation_analysis and 'error' not in correlation_analysis:
            st.subheader("Temperature-Activity Correlation")
            correlation_chart = chart_builder.create_correlation_chart(correlation_analysis)
            st.plotly_chart(correlation_chart, use_container_width=True)
        else:
            st.subheader("Noise Pattern Heatmap")
            noise_heatmap = chart_builder.create_noise_alert_heatmap(data['environment'])
            st.plotly_chart(noise_heatmap, use_container_width=True)
    
    # Detailed Analysis
    with st.expander("📊 Detailed Environmental Analysis"):
        if not temp_humidity['by_section'].empty:
            st.write("**Conditions by Kennel Section**")
            st.dataframe(temp_humidity['by_section'])
        
        if noise_analysis['peak_noise_hours']:
            st.write("**Peak Noise Hours**")
            st.json(noise_analysis['peak_noise_hours'])
    
    # Alerts
    if noise_analysis['critical_alerts'] > 0:
        st.error(f"🚨 {noise_analysis['critical_alerts']} Critical Noise Alerts in the last {analysis_days} days")
    elif noise_analysis['total_alerts'] > 10:
        st.warning(f"⚠️ {noise_analysis['total_alerts']} Noise Alerts - Consider investigating noise sources")


def render_operations_dashboard(data):
    """Render Operations Analytics section."""
    st.header("⚙️ Operations Overview")
    
    if data['staff_logs'].empty or data['pet_activities'].empty:
        st.warning("Insufficient data for operations analysis.")
        return
    
    # Initialize analyzer
    ops_analyzer = OperationsAnalyzer(data['staff_logs'], data['pet_activities'])
    chart_builder = ChartBuilder()
    
    # Analysis period selector
    col1, col2 = st.columns([1, 3])
    with col1:
        analysis_days = st.selectbox(
            "Analysis Period",
            options=[7, 14, 30, 90],
            index=2,
            format_func=lambda x: f"Last {x} days",
            key="ops_period"
        )
    
    # Get analytics
    grooming_analysis = ops_analyzer.get_grooming_cleaning_frequency(days=analysis_days)
    staff_analysis = ops_analyzer.get_staff_performance_analysis(days=analysis_days)
    alert_analysis = ops_analyzer.get_alert_trends_analysis(days=analysis_days)
    ops_summary = ops_analyzer.get_operations_summary()
    
    # Key Metrics
    if 'error' not in grooming_analysis and 'error' not in staff_analysis:
        metrics = [
            {
                'title': 'Operations Score',
                'value': f"{ops_summary['operations_score']:.0f}/100",
                'subtitle': 'Overall performance'
            },
            {
                'title': 'Grooming Compliance',
                'value': f"{grooming_analysis.get('grooming_schedule_compliance', 0):.0f}%",
                'subtitle': f"{grooming_analysis.get('pets_overdue_grooming', 0)} pets overdue"
            },
            {
                'title': 'Staff Performance',
                'value': f"{staff_analysis.get('avg_tasks_per_hour_kennel', 0):.2f}",
                'subtitle': 'Avg tasks/hour'
            },
            {
                'title': 'Health Alerts',
                'value': alert_analysis.get('total_health_alerts', 0),
                'subtitle': f"{alert_analysis.get('avg_health_alerts_per_day', 0):.1f}/day avg"
            }
        ]
        display_metric_cards(metrics, "Operations Performance")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        if 'error' not in grooming_analysis:
            st.subheader("Grooming Schedule Status")
            grooming_chart = chart_builder.create_grooming_schedule_chart(
                grooming_analysis.get('detailed_per_pet', pd.DataFrame())
            )
            st.plotly_chart(grooming_chart, use_container_width=True)
    
    with col2:
        if 'error' not in staff_analysis:
            st.subheader("Staff Performance Distribution")
            staff_chart = chart_builder.create_staff_performance_chart(
                staff_analysis.get('detailed_staff_metrics', pd.DataFrame())
            )
            st.plotly_chart(staff_chart, use_container_width=True)
    
    # Feeding Patterns
    feeding_data = data['pet_activities'][data['pet_activities']['activity_type'] == 'feeding']
    if not feeding_data.empty:
        st.subheader("Feeding Schedule Analysis")
        feeding_chart = chart_builder.create_feeding_pattern_chart(feeding_data)
        st.plotly_chart(feeding_chart, use_container_width=True)
    
    # Detailed Analysis
    with st.expander("📊 Detailed Operations Analysis"):
        if 'error' not in staff_analysis:
            st.write("**Staff Performance Details**")
            st.dataframe(staff_analysis.get('detailed_staff_metrics', pd.DataFrame()))
        
        if 'error' not in grooming_analysis:
            st.write("**Pets Needing Grooming**")
            pets_needing_grooming = grooming_analysis.get('pets_needing_attention', pd.DataFrame())
            if not pets_needing_grooming.empty:
                st.dataframe(pets_needing_grooming)
    
    # Recommendations
    recommendations = ops_summary.get('key_recommendations', [])
    if recommendations:
        st.subheader("📝 Operational Recommendations")
        for i, rec in enumerate(recommendations, 1):
            st.write(f"{i}. {rec}")


def render_legacy_dashboard(data):
    """Render the original dashboard for comparison."""
    st.header("📋 Raw Data Tables")
    
    st.sidebar.header("Table Selection")
    table = st.sidebar.selectbox(
        "Select table", 
        ['daily_summary', 'pet_activities', 'environment', 'staff_logs']
    )

    df = data.get(table, pd.DataFrame())
    if df.empty:
        st.warning(f"No data available for table '{table}'. Run the ETL pipeline first.")
        return

    st.subheader(f"{table.replace('_', ' ').title()}")
    st.dataframe(df.head(200))

    # Original charts
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


def main():
    st.set_page_config(
        page_title="KennelOS Analytics", 
        page_icon="🐕", 
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🐕 KennelOS Analytics Dashboard")
    st.markdown("*Comprehensive insights for kennel operations and pet wellness*")
    
    # Load data
    with st.spinner("Loading data..."):
        data = load_all_data()
    
    # Check if we have any data
    has_data = any(not df.empty for df in data.values())
    if not has_data:
        st.error("No data available. Please run the ETL pipeline first: `python main.py`")
        st.stop()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Dashboard",
        ["Pet Wellness", "Environmental Insights", "Operations Overview", "Raw Data"]
    )
    
    # Render selected dashboard
    if page == "Pet Wellness":
        render_pet_wellness_dashboard(data)
    elif page == "Environmental Insights":
        render_environmental_dashboard(data)
    elif page == "Operations Overview":
        render_operations_dashboard(data)
    elif page == "Raw Data":
        render_legacy_dashboard(data)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("*Last updated:*")
    st.sidebar.markdown(f"*{datetime.now().strftime('%Y-%m-%d %H:%M')}*")


if __name__ == '__main__':
    main()