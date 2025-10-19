"""
KennelOS Analytics Dashboard - Simplified Version

This is a working version that demonstrates the analytics architecture
without requiring the analytics module imports.
"""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl import db
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


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


def calculate_pet_wellness_metrics(pet_activities_df, days=30):
    """Calculate pet wellness metrics."""
    cutoff_date = datetime.now() - timedelta(days=days)
    
    # Filter recent data
    if 'timestamp' in pet_activities_df.columns:
        pet_activities_df['timestamp'] = pd.to_datetime(pet_activities_df['timestamp'])
        recent_df = pet_activities_df[pet_activities_df['timestamp'] >= cutoff_date].copy()
    else:
        recent_df = pet_activities_df.copy()
    
    if recent_df.empty:
        return {}
    
    # Daily activity per pet
    if 'date' in recent_df.columns:
        recent_df['date'] = pd.to_datetime(recent_df['date'])
        daily_activity = recent_df.groupby(['pet_id', 'pet_name', 'date']).agg({
            'duration_minutes': 'sum',
            'activity_type': 'count'
        }).reset_index()
        
        # Calculate averages
        pet_averages = daily_activity.groupby(['pet_id', 'pet_name']).agg({
            'duration_minutes': 'mean',
            'activity_type': 'mean'
        }).round(2).reset_index()
        
        pet_averages.columns = ['pet_id', 'pet_name', 'avg_daily_minutes', 'avg_daily_activities']
        
        # Feeding analysis
        feeding_df = recent_df[recent_df['activity_type'] == 'feeding']
        feeding_counts = feeding_df.groupby(['pet_id', 'pet_name']).size().reset_index(name='total_feedings')
        
        return {
            'activity_analysis': pet_averages,
            'feeding_analysis': feeding_counts,
            'total_pets': len(pet_averages),
            'total_activities': len(recent_df),
            'activity_types': recent_df['activity_type'].value_counts().to_dict()
        }
    
    return {}


def calculate_environmental_metrics(environment_df, days=30):
    """Calculate environmental metrics."""
    cutoff_date = datetime.now() - timedelta(days=days)
    
    if 'timestamp' in environment_df.columns:
        environment_df['timestamp'] = pd.to_datetime(environment_df['timestamp'])
        recent_df = environment_df[environment_df['timestamp'] >= cutoff_date].copy()
    else:
        recent_df = environment_df.copy()
    
    if recent_df.empty:
        return {}
    
    return {
        'avg_temperature': recent_df['temperature_f'].mean(),
        'avg_humidity': recent_df['humidity_percent'].mean(),
        'avg_noise': recent_df['noise_level_db'].mean(),
        'temp_range': (recent_df['temperature_f'].min(), recent_df['temperature_f'].max()),
        'humidity_range': (recent_df['humidity_percent'].min(), recent_df['humidity_percent'].max()),
        'noise_alerts': len(recent_df[recent_df['noise_level_db'] > 45]),
        'total_readings': len(recent_df)
    }


def calculate_operations_metrics(staff_logs_df, pet_activities_df, days=30):
    """Calculate operations metrics."""
    cutoff_date = datetime.now() - timedelta(days=days)
    
    # Staff analysis
    if 'shift_start' in staff_logs_df.columns:
        staff_logs_df['shift_start'] = pd.to_datetime(staff_logs_df['shift_start'])
        recent_staff = staff_logs_df[staff_logs_df['shift_start'] >= cutoff_date].copy()
    else:
        recent_staff = staff_logs_df.copy()
    
    # Activity analysis
    if 'timestamp' in pet_activities_df.columns:
        pet_activities_df['timestamp'] = pd.to_datetime(pet_activities_df['timestamp'])
        recent_activities = pet_activities_df[pet_activities_df['timestamp'] >= cutoff_date].copy()
    else:
        recent_activities = pet_activities_df.copy()
    
    results = {}
    
    if not recent_staff.empty:
        results.update({
            'total_staff': recent_staff['staff_id'].nunique(),
            'avg_tasks_per_hour': recent_staff['tasks_per_hour'].mean() if 'tasks_per_hour' in recent_staff.columns else 0,
            'total_shifts': len(recent_staff)
        })
    
    if not recent_activities.empty:
        grooming_activities = recent_activities[recent_activities['activity_type'] == 'grooming']
        medical_activities = recent_activities[recent_activities['activity_type'] == 'medical']
        
        results.update({
            'grooming_sessions': len(grooming_activities),
            'medical_checks': len(medical_activities),
            'pets_groomed': grooming_activities['pet_id'].nunique() if not grooming_activities.empty else 0
        })
    
    return results


def render_pet_wellness_dashboard(data):
    """Render Pet Wellness Analytics section."""
    st.header("🐾 Pet Wellness Analytics")
    
    if data['pet_activities'].empty:
        st.warning("No pet activity data available.")
        return
    
    # Analysis period selector
    col1, col2 = st.columns([1, 3])
    with col1:
        analysis_days = st.selectbox(
            "Analysis Period",
            options=[7, 14, 30, 90],
            index=2,
            format_func=lambda x: f"Last {x} days"
        )
    
    # Calculate metrics
    wellness_metrics = calculate_pet_wellness_metrics(data['pet_activities'], days=analysis_days)
    
    if not wellness_metrics:
        st.warning("Unable to calculate wellness metrics.")
        return
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Pets", wellness_metrics.get('total_pets', 0))
    
    with col2:
        st.metric("Total Activities", wellness_metrics.get('total_activities', 0))
    
    with col3:
        if 'activity_analysis' in wellness_metrics:
            avg_activity = wellness_metrics['activity_analysis']['avg_daily_minutes'].mean()
            st.metric("Avg Daily Activity", f"{avg_activity:.0f} min")
        else:
            st.metric("Avg Daily Activity", "N/A")
    
    with col4:
        activity_types = len(wellness_metrics.get('activity_types', {}))
        st.metric("Activity Types", activity_types)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Activity Distribution")
        if 'activity_types' in wellness_metrics:
            activity_counts = wellness_metrics['activity_types']
            fig = px.pie(values=list(activity_counts.values()), 
                        names=list(activity_counts.keys()),
                        title="Activity Type Distribution")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Pet Activity Levels")
        if 'activity_analysis' in wellness_metrics:
            activity_df = wellness_metrics['activity_analysis']
            if not activity_df.empty:
                fig = px.bar(activity_df.head(10), 
                           x='pet_name', y='avg_daily_minutes',
                           title="Average Daily Activity by Pet")
                fig.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
    
    # Detailed data
    with st.expander("📊 Detailed Activity Analysis"):
        if 'activity_analysis' in wellness_metrics:
            st.dataframe(wellness_metrics['activity_analysis'])


def render_environmental_dashboard(data):
    """Render Environmental Analytics section."""
    st.header("🌡️ Environmental Insights")
    
    if data['environment'].empty:
        st.warning("No environmental data available.")
        return
    
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
    
    # Calculate metrics
    env_metrics = calculate_environmental_metrics(data['environment'], days=analysis_days)
    
    if not env_metrics:
        st.warning("Unable to calculate environmental metrics.")
        return
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Avg Temperature", f"{env_metrics.get('avg_temperature', 0):.1f}°F")
    
    with col2:
        st.metric("Avg Humidity", f"{env_metrics.get('avg_humidity', 0):.1f}%")
    
    with col3:
        st.metric("Avg Noise Level", f"{env_metrics.get('avg_noise', 0):.1f}dB")
    
    with col4:
        st.metric("Noise Alerts", env_metrics.get('noise_alerts', 0))
    
    # Environmental trends chart
    st.subheader("Environmental Trends")
    env_df = data['environment'].copy()
    
    if 'timestamp' in env_df.columns:
        env_df['timestamp'] = pd.to_datetime(env_df['timestamp'])
        env_df['date'] = env_df['timestamp'].dt.date
        
        daily_env = env_df.groupby('date').agg({
            'temperature_f': 'mean',
            'humidity_percent': 'mean',
            'noise_level_db': 'mean'
        }).reset_index()
        
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('Temperature (°F)', 'Humidity (%)', 'Noise Level (dB)'),
            vertical_spacing=0.08
        )
        
        fig.add_trace(go.Scatter(x=daily_env['date'], y=daily_env['temperature_f'],
                               name='Temperature', line=dict(color='red')), row=1, col=1)
        fig.add_trace(go.Scatter(x=daily_env['date'], y=daily_env['humidity_percent'],
                               name='Humidity', line=dict(color='blue')), row=2, col=1)
        fig.add_trace(go.Scatter(x=daily_env['date'], y=daily_env['noise_level_db'],
                               name='Noise', line=dict(color='orange')), row=3, col=1)
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


def render_operations_dashboard(data):
    """Render Operations Analytics section."""
    st.header("⚙️ Operations Overview")
    
    if data['staff_logs'].empty or data['pet_activities'].empty:
        st.warning("Insufficient data for operations analysis.")
        return
    
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
    
    # Calculate metrics
    ops_metrics = calculate_operations_metrics(data['staff_logs'], data['pet_activities'], days=analysis_days)
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Staff", ops_metrics.get('total_staff', 0))
    
    with col2:
        avg_tasks = ops_metrics.get('avg_tasks_per_hour', 0)
        st.metric("Avg Tasks/Hour", f"{avg_tasks:.2f}")
    
    with col3:
        st.metric("Grooming Sessions", ops_metrics.get('grooming_sessions', 0))
    
    with col4:
        st.metric("Medical Checks", ops_metrics.get('medical_checks', 0))
    
    # Staff performance chart
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Staff Performance")
        staff_df = data['staff_logs']
        if 'tasks_per_hour' in staff_df.columns and not staff_df.empty:
            fig = px.box(staff_df, y='tasks_per_hour', title='Tasks per Hour Distribution')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Activity Breakdown")
        activities_df = data['pet_activities']
        if not activities_df.empty:
            activity_counts = activities_df['activity_type'].value_counts()
            fig = px.bar(x=activity_counts.index, y=activity_counts.values,
                        title='Activities by Type')
            st.plotly_chart(fig, use_container_width=True)


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
    
    # Show architecture info
    with st.sidebar.expander("ℹ️ About This Dashboard"):
        st.markdown("""
        **KennelOS Analytics** provides comprehensive insights into:
        
        🐾 **Pet Wellness**
        - Activity levels and patterns
        - Feeding frequency analysis
        - Health monitoring trends
        
        🌡️ **Environmental Monitoring**
        - Temperature and humidity tracking
        - Noise level alerts
        - Comfort analysis
        
        ⚙️ **Operations Management**
        - Staff performance metrics
        - Grooming schedules
        - Operational efficiency
        
        📊 **Architecture**
        - Modular analytics components
        - Configurable thresholds
        - Easy to extend and customize
        """)


if __name__ == '__main__':
    main()