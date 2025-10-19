"""
Pet Wellness Analytics

This module analyzes pet activity, feeding, and health trends to provide
insights into individual pet wellness and overall kennel population health.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from .config import PET_WELLNESS, ANALYSIS_WINDOWS


class PetWellnessAnalyzer:
    """Analyzes pet wellness metrics and trends."""
    
    def __init__(self, pet_activities_df: pd.DataFrame):
        """
        Initialize with pet activities data.
        
        Args:
            pet_activities_df: DataFrame with pet activity data
        """
        self.df = pet_activities_df.copy()
        if 'timestamp' in self.df.columns:
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        if 'date' in self.df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'])
    
    def get_avg_activity_time_per_pet(self, days: int = None) -> pd.DataFrame:
        """
        Calculate average activity time per pet.
        
        Args:
            days: Number of days to analyze (default: last 30 days)
            
        Returns:
            DataFrame with pet_id, pet_name, avg_daily_minutes, total_activities
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Filter recent data
        recent_df = self.df[self.df['timestamp'] >= cutoff_date].copy()
        
        # Calculate daily activity per pet
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
        
        # Add wellness flags
        pet_averages['activity_status'] = pet_averages['avg_daily_minutes'].apply(
            self._categorize_activity_level
        )
        
        return pet_averages.sort_values('avg_daily_minutes', ascending=False)
    
    def get_feeding_frequency_analysis(self, days: int = None) -> Dict:
        """
        Analyze feeding patterns and frequency.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with feeding metrics
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        feeding_df = self.df[
            (self.df['activity_type'] == 'feeding') & 
            (self.df['timestamp'] >= cutoff_date)
        ].copy()
        
        # Daily feeding counts per pet
        daily_feeding = feeding_df.groupby(['pet_id', 'pet_name', 'date']).size().reset_index(name='feedings_per_day')
        
        # Average feedings per pet
        avg_feedings = daily_feeding.groupby(['pet_id', 'pet_name']).agg({
            'feedings_per_day': ['mean', 'std', 'min', 'max']
        }).round(2)
        
        avg_feedings.columns = ['avg_feedings_per_day', 'feeding_consistency', 'min_daily', 'max_daily']
        avg_feedings = avg_feedings.reset_index()
        
        # Add feeding status flags
        avg_feedings['feeding_status'] = avg_feedings['avg_feedings_per_day'].apply(
            self._categorize_feeding_frequency
        )
        
        # Overall statistics
        overall_stats = {
            'total_pets_analyzed': len(avg_feedings),
            'avg_feedings_across_kennel': avg_feedings['avg_feedings_per_day'].mean(),
            'pets_with_irregular_feeding': len(avg_feedings[avg_feedings['feeding_status'] != 'normal']),
            'detailed_per_pet': avg_feedings
        }
        
        return overall_stats
    
    def get_weight_trend_analysis(self, days: int = None) -> Dict:
        """
        Analyze weight trends using medical activity notes as proxy.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with weight trend insights
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        medical_df = self.df[
            (self.df['activity_type'] == 'medical') & 
            (self.df['timestamp'] >= cutoff_date)
        ].copy()
        
        # Extract weight mentions from notes (basic pattern matching)
        weight_mentions = medical_df[
            medical_df['notes'].str.contains('weight|Weight', na=False, case=False)
        ].copy()
        
        # Group by pet for trend analysis
        weight_checks_per_pet = weight_mentions.groupby(['pet_id', 'pet_name']).agg({
            'timestamp': ['count', 'min', 'max'],
            'notes': 'first'
        })
        
        weight_checks_per_pet.columns = ['total_weight_checks', 'first_check', 'latest_check', 'latest_notes']
        weight_checks_per_pet = weight_checks_per_pet.reset_index()
        
        # Calculate days between checks
        weight_checks_per_pet['days_between_checks'] = (
            weight_checks_per_pet['latest_check'] - weight_checks_per_pet['first_check']
        ).dt.days
        
        analysis = {
            'pets_with_weight_monitoring': len(weight_checks_per_pet),
            'total_weight_checks': weight_mentions.shape[0],
            'avg_checks_per_pet': weight_checks_per_pet['total_weight_checks'].mean() if not weight_checks_per_pet.empty else 0,
            'detailed_per_pet': weight_checks_per_pet,
            'recent_weight_activities': weight_mentions.sort_values('timestamp', ascending=False).head(10)
        }
        
        return analysis
    
    def get_pet_wellness_summary(self) -> Dict:
        """Get comprehensive pet wellness summary."""
        activity_analysis = self.get_avg_activity_time_per_pet()
        feeding_analysis = self.get_feeding_frequency_analysis()
        weight_analysis = self.get_weight_trend_analysis()
        
        # Calculate overall wellness indicators
        total_pets = len(activity_analysis)
        active_pets = len(activity_analysis[activity_analysis['activity_status'] == 'optimal'])
        well_fed_pets = len(feeding_analysis['detailed_per_pet'][
            feeding_analysis['detailed_per_pet']['feeding_status'] == 'normal'
        ])
        
        return {
            'total_pets': total_pets,
            'activity_wellness_rate': (active_pets / total_pets * 100) if total_pets > 0 else 0,
            'feeding_wellness_rate': (well_fed_pets / total_pets * 100) if total_pets > 0 else 0,
            'pets_needing_attention': activity_analysis[
                activity_analysis['activity_status'].isin(['low', 'high'])
            ],
            'activity_details': activity_analysis,
            'feeding_details': feeding_analysis,
            'weight_monitoring': weight_analysis
        }
    
    def _categorize_activity_level(self, avg_minutes: float) -> str:
        """Categorize pet activity level."""
        if avg_minutes < PET_WELLNESS['min_activity_minutes_per_day']:
            return 'low'
        elif avg_minutes > PET_WELLNESS['max_activity_minutes_per_day']:
            return 'high'
        else:
            return 'optimal'
    
    def _categorize_feeding_frequency(self, avg_feedings: float) -> str:
        """Categorize feeding frequency."""
        if avg_feedings < PET_WELLNESS['min_feeding_times_per_day']:
            return 'infrequent'
        elif avg_feedings > PET_WELLNESS['max_feeding_times_per_day']:
            return 'excessive'
        else:
            return 'normal'