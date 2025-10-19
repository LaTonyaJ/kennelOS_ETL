"""
Environmental Analytics

This module analyzes environmental monitoring data to provide insights into
kennel conditions, identify patterns, and detect potential issues.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from .config import ENVIRONMENT, ANALYSIS_WINDOWS


class EnvironmentalAnalyzer:
    """Analyzes environmental conditions and trends."""
    
    def __init__(self, environment_df: pd.DataFrame):
        """
        Initialize with environment monitoring data.
        
        Args:
            environment_df: DataFrame with environmental data
        """
        self.df = environment_df.copy()
        if 'timestamp' in self.df.columns:
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        if 'date' in self.df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'])
    
    def get_temperature_humidity_averages(self, days: int = None, by_section: bool = True) -> Dict:
        """
        Calculate temperature and humidity averages.
        
        Args:
            days: Number of days to analyze
            by_section: Whether to break down by kennel section
            
        Returns:
            Dictionary with environmental averages
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_df = self.df[self.df['timestamp'] >= cutoff_date].copy()
        
        if by_section and 'kennel_section' in recent_df.columns:
            # Analysis by kennel section
            section_stats = recent_df.groupby('kennel_section').agg({
                'temperature_f': ['mean', 'std', 'min', 'max'],
                'humidity_percent': ['mean', 'std', 'min', 'max'],
                'noise_level_db': ['mean', 'std', 'min', 'max']
            }).round(2)
            
            # Flatten column names
            section_stats.columns = [f"{col[1]}_{col[0]}" for col in section_stats.columns]
            section_stats = section_stats.reset_index()
            
            # Add comfort ratings for each section
            section_stats['temp_comfort_rating'] = section_stats.apply(
                lambda row: self._rate_temperature_comfort(row['mean_temperature_f']), axis=1
            )
            section_stats['humidity_comfort_rating'] = section_stats.apply(
                lambda row: self._rate_humidity_comfort(row['mean_humidity_percent']), axis=1
            )
        else:
            section_stats = pd.DataFrame()
        
        # Overall averages
        overall_stats = {
            'avg_temperature': recent_df['temperature_f'].mean(),
            'avg_humidity': recent_df['humidity_percent'].mean(),
            'avg_noise': recent_df['noise_level_db'].mean(),
            'temp_std': recent_df['temperature_f'].std(),
            'humidity_std': recent_df['humidity_percent'].std(),
            'noise_std': recent_df['noise_level_db'].std(),
            'total_readings': len(recent_df),
            'days_analyzed': days
        }
        
        return {
            'overall': overall_stats,
            'by_section': section_stats,
            'comfort_summary': self._get_comfort_summary(recent_df)
        }
    
    def get_noise_level_alerts(self, days: int = None) -> Dict:
        """
        Analyze noise levels and identify alerts.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with noise analysis and alerts
        """
        days = days or ANALYSIS_WINDOWS['short_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_df = self.df[self.df['timestamp'] >= cutoff_date].copy()
        
        # Define noise thresholds
        normal_max = ENVIRONMENT['noise']['normal_max']
        alert_threshold = ENVIRONMENT['noise']['alert_threshold']
        critical_threshold = ENVIRONMENT['noise']['critical_threshold']
        
        # Categorize noise levels
        recent_df['noise_category'] = recent_df['noise_level_db'].apply(
            lambda x: self._categorize_noise_level(x)
        )
        
        # Count alerts by hour and day
        hourly_alerts = recent_df[
            recent_df['noise_level_db'] > alert_threshold
        ].groupby(['date', 'hour']).size().reset_index(name='alert_count')
        
        # Daily alert summary
        daily_alerts = recent_df.groupby(['date', 'noise_category']).size().unstack(fill_value=0)
        
        # Peak noise times
        hourly_avg = recent_df.groupby('hour')['noise_level_db'].mean().round(2)
        peak_hours = hourly_avg[hourly_avg > alert_threshold].sort_values(ascending=False)
        
        return {
            'total_alerts': len(recent_df[recent_df['noise_level_db'] > alert_threshold]),
            'critical_alerts': len(recent_df[recent_df['noise_level_db'] > critical_threshold]),
            'alerts_per_day': (len(recent_df[recent_df['noise_level_db'] > alert_threshold]) / days),
            'hourly_alerts': hourly_alerts,
            'daily_breakdown': daily_alerts,
            'peak_noise_hours': peak_hours.to_dict(),
            'noise_distribution': recent_df['noise_category'].value_counts().to_dict(),
            'recent_alerts': recent_df[recent_df['noise_level_db'] > alert_threshold].tail(10)
        }
    
    def get_temperature_activity_correlation(self, pet_activities_df: pd.DataFrame, days: int = None) -> Dict:
        """
        Analyze correlation between temperature and pet activity.
        
        Args:
            pet_activities_df: DataFrame with pet activity data
            days: Number of days to analyze
            
        Returns:
            Dictionary with correlation analysis
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Prepare environment data
        env_hourly = self.df[self.df['timestamp'] >= cutoff_date].groupby(['date', 'hour']).agg({
            'temperature_f': 'mean',
            'humidity_percent': 'mean',
            'noise_level_db': 'mean'
        }).reset_index()
        
        # Prepare activity data
        activities_df = pet_activities_df.copy()
        if 'timestamp' in activities_df.columns:
            activities_df['timestamp'] = pd.to_datetime(activities_df['timestamp'])
        if 'date' in activities_df.columns:
            activities_df['date'] = pd.to_datetime(activities_df['date'])
        
        activity_hourly = activities_df[
            activities_df['timestamp'] >= cutoff_date
        ].groupby(['date', 'hour']).agg({
            'duration_minutes': 'sum',
            'pet_id': 'count'
        }).reset_index()
        activity_hourly.columns = ['date', 'hour', 'total_activity_minutes', 'activity_count']
        
        # Merge datasets
        merged_df = pd.merge(
            env_hourly, 
            activity_hourly, 
            on=['date', 'hour'], 
            how='inner'
        )
        
        if merged_df.empty:
            return {'error': 'No matching data found for correlation analysis'}
        
        # Calculate correlations
        temp_activity_corr = merged_df['temperature_f'].corr(merged_df['total_activity_minutes'])
        temp_count_corr = merged_df['temperature_f'].corr(merged_df['activity_count'])
        humidity_activity_corr = merged_df['humidity_percent'].corr(merged_df['total_activity_minutes'])
        
        # Temperature ranges and activity levels
        merged_df['temp_range'] = merged_df['temperature_f'].apply(self._categorize_temperature_range)
        activity_by_temp = merged_df.groupby('temp_range').agg({
            'total_activity_minutes': 'mean',
            'activity_count': 'mean'
        }).round(2)
        
        return {
            'temperature_activity_correlation': round(temp_activity_corr, 3),
            'temperature_count_correlation': round(temp_count_corr, 3),
            'humidity_activity_correlation': round(humidity_activity_corr, 3),
            'activity_by_temperature_range': activity_by_temp.to_dict(),
            'optimal_temperature_range': self._find_optimal_temperature_range(merged_df),
            'data_points': len(merged_df),
            'correlation_strength': self._interpret_correlation(temp_activity_corr)
        }
    
    def get_environmental_summary(self, pet_activities_df: pd.DataFrame = None) -> Dict:
        """Get comprehensive environmental summary."""
        temp_humidity = self.get_temperature_humidity_averages()
        noise_analysis = self.get_noise_level_alerts()
        
        summary = {
            'environmental_conditions': temp_humidity,
            'noise_monitoring': noise_analysis,
            'overall_comfort_score': self._calculate_comfort_score(temp_humidity['overall'])
        }
        
        if pet_activities_df is not None:
            correlation_analysis = self.get_temperature_activity_correlation(pet_activities_df)
            summary['temperature_activity_insights'] = correlation_analysis
        
        return summary
    
    def _rate_temperature_comfort(self, temp: float) -> str:
        """Rate temperature comfort level."""
        config = ENVIRONMENT['temperature']
        if config['optimal_min'] <= temp <= config['optimal_max']:
            return 'optimal'
        elif config['alert_min'] <= temp <= config['alert_max']:
            return 'acceptable'
        else:
            return 'poor'
    
    def _rate_humidity_comfort(self, humidity: float) -> str:
        """Rate humidity comfort level."""
        config = ENVIRONMENT['humidity']
        if config['optimal_min'] <= humidity <= config['optimal_max']:
            return 'optimal'
        elif config['alert_min'] <= humidity <= config['alert_max']:
            return 'acceptable'
        else:
            return 'poor'
    
    def _categorize_noise_level(self, noise: float) -> str:
        """Categorize noise level."""
        config = ENVIRONMENT['noise']
        if noise <= config['normal_max']:
            return 'normal'
        elif noise <= config['alert_threshold']:
            return 'elevated'
        elif noise <= config['critical_threshold']:
            return 'high'
        else:
            return 'critical'
    
    def _categorize_temperature_range(self, temp: float) -> str:
        """Categorize temperature into ranges."""
        if temp < 65:
            return 'cold'
        elif temp < 72:
            return 'cool'
        elif temp <= 78:
            return 'optimal'
        elif temp <= 82:
            return 'warm'
        else:
            return 'hot'
    
    def _get_comfort_summary(self, df: pd.DataFrame) -> Dict:
        """Calculate overall comfort summary."""
        if 'temp_comfort' in df.columns and 'humidity_comfort' in df.columns:
            temp_comfort = df['temp_comfort'].value_counts().to_dict()
            humidity_comfort = df['humidity_comfort'].value_counts().to_dict()
            noise_comfort = df['noise_comfort'].value_counts().to_dict() if 'noise_comfort' in df.columns else {}
        else:
            # Calculate comfort based on values
            temp_comfort = df['temperature_f'].apply(
                lambda x: self._rate_temperature_comfort(x)
            ).value_counts().to_dict()
            humidity_comfort = df['humidity_percent'].apply(
                lambda x: self._rate_humidity_comfort(x)
            ).value_counts().to_dict()
            noise_comfort = df['noise_level_db'].apply(
                lambda x: self._categorize_noise_level(x)
            ).value_counts().to_dict()
        
        return {
            'temperature_comfort_distribution': temp_comfort,
            'humidity_comfort_distribution': humidity_comfort,
            'noise_level_distribution': noise_comfort
        }
    
    def _find_optimal_temperature_range(self, merged_df: pd.DataFrame) -> str:
        """Find temperature range with highest activity."""
        activity_by_temp = merged_df.groupby('temp_range')['total_activity_minutes'].mean()
        return activity_by_temp.idxmax() if not activity_by_temp.empty else 'unknown'
    
    def _interpret_correlation(self, correlation: float) -> str:
        """Interpret correlation strength."""
        abs_corr = abs(correlation)
        if abs_corr >= 0.7:
            return 'strong'
        elif abs_corr >= 0.4:
            return 'moderate'
        elif abs_corr >= 0.2:
            return 'weak'
        else:
            return 'negligible'
    
    def _calculate_comfort_score(self, overall_stats: Dict) -> float:
        """Calculate overall comfort score (0-100)."""
        temp_score = 100 if ENVIRONMENT['temperature']['optimal_min'] <= overall_stats['avg_temperature'] <= ENVIRONMENT['temperature']['optimal_max'] else 50
        humidity_score = 100 if ENVIRONMENT['humidity']['optimal_min'] <= overall_stats['avg_humidity'] <= ENVIRONMENT['humidity']['optimal_max'] else 50  
        noise_score = 100 if overall_stats['avg_noise'] <= ENVIRONMENT['noise']['normal_max'] else 50
        
        return round((temp_score + humidity_score + noise_score) / 3, 1)