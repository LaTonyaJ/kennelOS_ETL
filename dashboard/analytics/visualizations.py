"""
Visualization Components

This module provides reusable chart and visualization components
for the KennelOS analytics dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, List, Any
from .config import CHART_COLORS


class ChartBuilder:
    """Builds standardized charts for the dashboard."""
    
    def __init__(self):
        """Initialize with standard color scheme."""
        self.colors = CHART_COLORS
    
    def create_metric_card(self, title: str, value: Any, subtitle: str = "", 
                          color: str = "primary", format_type: str = "number") -> Dict:
        """
        Create a metric card configuration.
        
        Args:
            title: Card title
            value: Main value to display
            subtitle: Optional subtitle
            color: Color theme
            format_type: How to format the value (number, percentage, duration)
            
        Returns:
            Dictionary with metric card configuration
        """
        formatted_value = self._format_value(value, format_type)
        
        return {
            'title': title,
            'value': formatted_value,
            'subtitle': subtitle,
            'color': self.colors.get(color, self.colors['primary'])
        }
    
    def create_pet_activity_chart(self, pet_activities_df: pd.DataFrame) -> go.Figure:
        """Create pet activity distribution chart."""
        activity_counts = pet_activities_df['activity_type'].value_counts()
        
        fig = px.pie(
            values=activity_counts.values,
            names=activity_counts.index,
            title="Pet Activity Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(showlegend=True, height=400)
        
        return fig
    
    def create_pet_wellness_chart(self, wellness_data: pd.DataFrame) -> go.Figure:
        """Create pet wellness status chart."""
        fig = px.scatter(
            wellness_data,
            x='avg_daily_minutes',
            y='avg_daily_activities', 
            color='activity_status',
            hover_data=['pet_name'],
            title="Pet Activity Levels",
            labels={
                'avg_daily_minutes': 'Average Daily Activity (minutes)',
                'avg_daily_activities': 'Average Daily Activities',
                'activity_status': 'Activity Status'
            },
            color_discrete_map={
                'optimal': self.colors['success'],
                'low': self.colors['warning'], 
                'high': self.colors['danger']
            }
        )
        
        fig.update_layout(height=500)
        return fig
    
    def create_environmental_trends_chart(self, env_df: pd.DataFrame) -> go.Figure:
        """Create environmental conditions trend chart."""
        # Group by date for daily averages
        if 'timestamp' in env_df.columns:
            env_df['date'] = pd.to_datetime(env_df['timestamp']).dt.date
        
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
        
        # Temperature
        fig.add_trace(
            go.Scatter(
                x=daily_env['date'], 
                y=daily_env['temperature_f'],
                name='Temperature',
                line=dict(color=self.colors['danger']),
                mode='lines+markers'
            ),
            row=1, col=1
        )
        
        # Humidity
        fig.add_trace(
            go.Scatter(
                x=daily_env['date'], 
                y=daily_env['humidity_percent'],
                name='Humidity',
                line=dict(color=self.colors['info']),
                mode='lines+markers'
            ),
            row=2, col=1
        )
        
        # Noise
        fig.add_trace(
            go.Scatter(
                x=daily_env['date'], 
                y=daily_env['noise_level_db'],
                name='Noise Level',
                line=dict(color=self.colors['warning']),
                mode='lines+markers'
            ),
            row=3, col=1
        )
        
        fig.update_layout(
            height=600,
            title_text="Environmental Conditions Trends",
            showlegend=False
        )
        
        return fig
    
    def create_staff_performance_chart(self, staff_df: pd.DataFrame) -> go.Figure:
        """Create staff performance visualization."""
        fig = px.box(
            staff_df,
            x='performance_rating',
            y='avg_tasks_per_hour',
            color='performance_rating',
            title="Staff Performance Distribution",
            labels={
                'avg_tasks_per_hour': 'Tasks per Hour',
                'performance_rating': 'Performance Rating'
            },
            color_discrete_map={
                'excellent': self.colors['success'],
                'satisfactory': self.colors['info'],
                'below_target': self.colors['warning']
            }
        )
        
        fig.update_layout(height=400)
        return fig
    
    def create_noise_alert_heatmap(self, hourly_noise_df: pd.DataFrame) -> go.Figure:
        """Create noise level heatmap by hour and day."""
        if 'timestamp' in hourly_noise_df.columns:
            hourly_noise_df['hour'] = pd.to_datetime(hourly_noise_df['timestamp']).dt.hour
            hourly_noise_df['day'] = pd.to_datetime(hourly_noise_df['timestamp']).dt.day_name()
        
        pivot_data = hourly_noise_df.pivot_table(
            values='noise_level_db',
            index='day',
            columns='hour',
            aggfunc='mean'
        )
        
        fig = px.imshow(
            pivot_data,
            aspect='auto',
            title="Noise Levels by Day and Hour",
            labels={'color': 'Noise Level (dB)', 'x': 'Hour of Day', 'y': 'Day of Week'},
            color_continuous_scale='Reds'
        )
        
        fig.update_layout(height=400)
        return fig
    
    def create_correlation_chart(self, correlation_data: Dict) -> go.Figure:
        """Create temperature-activity correlation chart."""
        activity_by_temp = correlation_data.get('activity_by_temperature_range', {})
        
        if not activity_by_temp:
            # Return empty figure
            fig = go.Figure()
            fig.add_annotation(
                text="No correlation data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
        
        temp_ranges = list(activity_by_temp.get('total_activity_minutes', {}).keys())
        activity_values = list(activity_by_temp.get('total_activity_minutes', {}).values())
        
        fig = px.bar(
            x=temp_ranges,
            y=activity_values,
            title=f"Activity by Temperature Range (Correlation: {correlation_data.get('temperature_activity_correlation', 0):.3f})",
            labels={'x': 'Temperature Range', 'y': 'Average Activity Minutes'},
            color=activity_values,
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(height=400)
        return fig
    
    def create_grooming_schedule_chart(self, grooming_data: pd.DataFrame) -> go.Figure:
        """Create grooming schedule compliance chart."""
        if grooming_data.empty:
            fig = go.Figure()
            fig.add_annotation(
                text="No grooming data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
        
        fig = px.histogram(
            grooming_data,
            x='days_since_last_groom',
            nbins=20,
            title="Days Since Last Grooming Distribution",
            labels={'days_since_last_groom': 'Days Since Last Grooming', 'count': 'Number of Pets'},
            color_discrete_sequence=[self.colors['info']]
        )
        
        # Add reference lines for target and alert thresholds
        fig.add_vline(x=7, line_dash="dash", line_color=self.colors['success'], 
                     annotation_text="Target (7 days)")
        fig.add_vline(x=10, line_dash="dash", line_color=self.colors['danger'], 
                     annotation_text="Alert (10 days)")
        
        fig.update_layout(height=400)
        return fig
    
    def create_feeding_pattern_chart(self, feeding_data: pd.DataFrame) -> go.Figure:
        """Create feeding pattern analysis chart."""
        if feeding_data.empty:
            fig = go.Figure()
            fig.add_annotation(
                text="No feeding data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
        
        hourly_feeding = feeding_data.groupby('hour').size().reset_index(name='feeding_count')
        
        fig = px.bar(
            hourly_feeding,
            x='hour',
            y='feeding_count',
            title="Feeding Activity by Hour",
            labels={'hour': 'Hour of Day', 'feeding_count': 'Number of Feedings'},
            color='feeding_count',
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(height=400)
        fig.update_xaxis(tickmode='linear', tick0=0, dtick=2)
        
        return fig
    
    def _format_value(self, value: Any, format_type: str) -> str:
        """Format value based on type."""
        if value is None:
            return "N/A"
        
        try:
            if format_type == "percentage":
                return f"{float(value):.1f}%"
            elif format_type == "duration":
                return f"{float(value):.0f} min"
            elif format_type == "number":
                if isinstance(value, float):
                    return f"{value:.1f}"
                else:
                    return str(value)
            else:
                return str(value)
        except (ValueError, TypeError):
            return str(value)