"""
Operations Analytics

This module analyzes operational aspects including staff performance,
grooming schedules, feeding schedules, and alert patterns.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from .config import OPERATIONS, ANALYSIS_WINDOWS


class OperationsAnalyzer:
    """Analyzes kennel operations and staff performance."""
    
    def __init__(self, staff_logs_df: pd.DataFrame, pet_activities_df: pd.DataFrame):
        """
        Initialize with staff and activity data.
        
        Args:
            staff_logs_df: DataFrame with staff log data
            pet_activities_df: DataFrame with pet activity data
        """
        self.staff_df = staff_logs_df.copy()
        self.activities_df = pet_activities_df.copy()
        
        # Ensure datetime columns
        for df in [self.staff_df, self.activities_df]:
            for col in df.columns:
                if 'timestamp' in col or 'shift_start' in col or 'shift_end' in col:
                    df[col] = pd.to_datetime(df[col])
        
        if 'date' in self.activities_df.columns:
            self.activities_df['date'] = pd.to_datetime(self.activities_df['date'])
    
    def get_grooming_cleaning_frequency(self, days: int = None) -> Dict:
        """
        Analyze grooming and cleaning frequency patterns.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with grooming and cleaning metrics
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Filter grooming activities
        grooming_df = self.activities_df[
            (self.activities_df['activity_type'] == 'grooming') & 
            (self.activities_df['timestamp'] >= cutoff_date)
        ].copy()
        
        if grooming_df.empty:
            return {'error': 'No grooming data found for the specified period'}
        
        # Grooming frequency per pet
        pet_grooming = grooming_df.groupby(['pet_id', 'pet_name']).agg({
            'timestamp': ['count', 'min', 'max'],
            'duration_minutes': 'mean'
        })
        
        pet_grooming.columns = ['total_grooming_sessions', 'first_groom', 'latest_groom', 'avg_duration']
        pet_grooming = pet_grooming.reset_index()
        
        # Calculate days between grooming sessions
        pet_grooming['days_since_last_groom'] = (
            datetime.now() - pet_grooming['latest_groom']
        ).dt.days
        
        pet_grooming['grooming_frequency'] = pet_grooming.apply(
            lambda row: (row['latest_groom'] - row['first_groom']).days / max(1, row['total_grooming_sessions'] - 1)
            if row['total_grooming_sessions'] > 1 else 0, axis=1
        )
        
        # Identify pets needing grooming
        target_days = OPERATIONS['grooming_frequency']['target_days_between']
        alert_days = OPERATIONS['grooming_frequency']['alert_days_overdue']
        
        pets_needing_grooming = pet_grooming[
            pet_grooming['days_since_last_groom'] > target_days
        ]
        
        pets_overdue_grooming = pet_grooming[
            pet_grooming['days_since_last_groom'] > alert_days
        ]
        
        # Daily grooming patterns
        daily_grooming = grooming_df.groupby('date').agg({
            'pet_id': 'count',
            'duration_minutes': ['sum', 'mean'],
            'staff_id': 'nunique'
        })
        
        daily_grooming.columns = ['grooming_sessions', 'total_minutes', 'avg_duration', 'staff_involved']
        
        return {
            'total_grooming_sessions': len(grooming_df),
            'pets_groomed': len(pet_grooming),
            'avg_sessions_per_pet': pet_grooming['total_grooming_sessions'].mean(),
            'avg_grooming_duration': grooming_df['duration_minutes'].mean(),
            'pets_needing_grooming': len(pets_needing_grooming),
            'pets_overdue_grooming': len(pets_overdue_grooming),
            'grooming_schedule_compliance': (1 - len(pets_overdue_grooming) / len(pet_grooming)) * 100 if len(pet_grooming) > 0 else 0,
            'detailed_per_pet': pet_grooming,
            'daily_patterns': daily_grooming,
            'pets_needing_attention': pets_overdue_grooming[['pet_id', 'pet_name', 'days_since_last_groom']]
        }
    
    def get_staff_performance_analysis(self, days: int = None) -> Dict:
        """
        Analyze staff performance metrics.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with staff performance insights
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_staff = self.staff_df[
            self.staff_df['shift_start'] >= cutoff_date
        ].copy()
        
        if recent_staff.empty:
            return {'error': 'No staff data found for the specified period'}
        
        # Staff performance metrics
        staff_performance = recent_staff.groupby(['staff_id', 'staff_name']).agg({
            'tasks_completed': ['sum', 'mean'],
            'shift_duration_hours': ['sum', 'mean'],
            'tasks_per_hour': 'mean',
            'shift_start': 'count'
        }).round(2)
        
        staff_performance.columns = [
            'total_tasks', 'avg_tasks_per_shift', 'total_hours', 
            'avg_hours_per_shift', 'avg_tasks_per_hour', 'total_shifts'
        ]
        staff_performance = staff_performance.reset_index()
        
        # Performance categories
        min_performance = OPERATIONS['staff_performance']['min_tasks_per_hour']
        target_performance = OPERATIONS['staff_performance']['target_tasks_per_hour']
        
        staff_performance['performance_rating'] = staff_performance['avg_tasks_per_hour'].apply(
            lambda x: self._rate_staff_performance(x)
        )
        
        # Shift type analysis
        shift_analysis = recent_staff.groupby(['staff_id', 'shift_type']).agg({
            'tasks_per_hour': 'mean',
            'tasks_completed': 'sum'
        }).reset_index()
        
        # Activity involvement analysis (from pet activities)
        staff_activities = self.activities_df[
            self.activities_df['timestamp'] >= cutoff_date
        ].groupby('staff_id').agg({
            'pet_id': 'count',
            'duration_minutes': 'sum',
            'activity_type': lambda x: x.value_counts().to_dict()
        }).reset_index()
        
        staff_activities.columns = ['staff_id', 'total_activities', 'total_activity_minutes', 'activity_breakdown']
        
        # Merge staff performance with activity data
        combined_performance = pd.merge(
            staff_performance, 
            staff_activities, 
            on='staff_id', 
            how='left'
        ).fillna(0)
        
        return {
            'total_staff_analyzed': len(staff_performance),
            'avg_tasks_per_hour_kennel': staff_performance['avg_tasks_per_hour'].mean(),
            'top_performers': staff_performance.nlargest(5, 'avg_tasks_per_hour'),
            'staff_needing_support': staff_performance[
                staff_performance['performance_rating'] == 'below_target'
            ],
            'shift_type_performance': shift_analysis.groupby('shift_type')['tasks_per_hour'].mean().to_dict(),
            'detailed_staff_metrics': combined_performance,
            'performance_distribution': staff_performance['performance_rating'].value_counts().to_dict()
        }
    
    def get_alert_trends_analysis(self, days: int = None) -> Dict:
        """
        Analyze alert trends from various sources.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with alert trend analysis
        """
        days = days or ANALYSIS_WINDOWS['medium_term']
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_activities = self.activities_df[
            self.activities_df['timestamp'] >= cutoff_date
        ].copy()
        
        # Health alerts (medical activities)
        health_alerts = recent_activities[
            recent_activities['activity_type'] == 'medical'
        ].copy()
        
        # Feeding delay alerts (feeding activities outside normal hours)
        feeding_activities = recent_activities[
            recent_activities['activity_type'] == 'feeding'
        ].copy()
        
        # Analyze feeding schedule adherence
        feeding_activities['feeding_hour'] = feeding_activities['hour']
        normal_feeding_hours = [7, 8, 12, 13, 17, 18]  # Typical feeding times
        
        feeding_delays = feeding_activities[
            ~feeding_activities['feeding_hour'].isin(normal_feeding_hours)
        ]
        
        # Daily alert counts
        daily_health_alerts = health_alerts.groupby('date').size()
        daily_feeding_delays = feeding_delays.groupby('date').size()
        
        # Alert patterns by hour
        hourly_health_alerts = health_alerts.groupby('hour').size()
        hourly_feeding_issues = feeding_delays.groupby('hour').size()
        
        # Pet-specific alert analysis
        pet_health_alerts = health_alerts.groupby(['pet_id', 'pet_name']).size().reset_index(name='health_alert_count')
        pet_feeding_issues = feeding_delays.groupby(['pet_id', 'pet_name']).size().reset_index(name='feeding_issue_count')
        
        # Staff involved in alerts
        staff_alert_handling = health_alerts.groupby('staff_id').size().reset_index(name='health_alerts_handled')
        
        return {
            'total_health_alerts': len(health_alerts),
            'total_feeding_delays': len(feeding_delays),
            'avg_health_alerts_per_day': len(health_alerts) / days,
            'avg_feeding_delays_per_day': len(feeding_delays) / days,
            'peak_health_alert_hours': hourly_health_alerts.nlargest(3).to_dict(),
            'peak_feeding_issue_hours': hourly_feeding_issues.nlargest(3).to_dict(),
            'pets_with_frequent_health_alerts': pet_health_alerts[pet_health_alerts['health_alert_count'] > 2],
            'pets_with_feeding_issues': pet_feeding_issues[pet_feeding_issues['feeding_issue_count'] > 1],
            'staff_alert_response': staff_alert_handling,
            'daily_health_trend': daily_health_alerts.to_dict(),
            'daily_feeding_trend': daily_feeding_delays.to_dict(),
            'alert_summary': {
                'health_alerts_trend': 'increasing' if daily_health_alerts.tail(3).mean() > daily_health_alerts.head(3).mean() else 'stable',
                'feeding_delays_trend': 'increasing' if daily_feeding_delays.tail(3).mean() > daily_feeding_delays.head(3).mean() else 'stable'
            }
        }
    
    def get_operations_summary(self) -> Dict:
        """Get comprehensive operations summary."""
        grooming_analysis = self.get_grooming_cleaning_frequency()
        staff_analysis = self.get_staff_performance_analysis()
        alert_analysis = self.get_alert_trends_analysis()
        
        # Calculate overall operations score
        grooming_score = grooming_analysis.get('grooming_schedule_compliance', 0)
        staff_score = self._calculate_staff_score(staff_analysis)
        alert_score = self._calculate_alert_score(alert_analysis)
        
        overall_score = (grooming_score + staff_score + alert_score) / 3
        
        return {
            'operations_score': round(overall_score, 1),
            'grooming_operations': grooming_analysis,
            'staff_performance': staff_analysis,
            'alert_management': alert_analysis,
            'key_recommendations': self._generate_recommendations(
                grooming_analysis, staff_analysis, alert_analysis
            )
        }
    
    def _rate_staff_performance(self, tasks_per_hour: float) -> str:
        """Rate staff performance level."""
        config = OPERATIONS['staff_performance']
        if tasks_per_hour >= config['target_tasks_per_hour']:
            return 'excellent'
        elif tasks_per_hour >= config['min_tasks_per_hour']:
            return 'satisfactory'
        else:
            return 'below_target'
    
    def _calculate_staff_score(self, staff_analysis: Dict) -> float:
        """Calculate overall staff performance score."""
        if 'error' in staff_analysis:
            return 50.0
        
        performance_dist = staff_analysis.get('performance_distribution', {})
        total_staff = sum(performance_dist.values())
        
        if total_staff == 0:
            return 50.0
        
        excellent = performance_dist.get('excellent', 0)
        satisfactory = performance_dist.get('satisfactory', 0)
        
        score = ((excellent * 100) + (satisfactory * 70)) / total_staff
        return min(100, score)
    
    def _calculate_alert_score(self, alert_analysis: Dict) -> float:
        """Calculate alert management score."""
        if 'error' in alert_analysis:
            return 50.0
        
        # Lower alert rates = higher scores
        health_alerts_per_day = alert_analysis.get('avg_health_alerts_per_day', 0)
        feeding_delays_per_day = alert_analysis.get('avg_feeding_delays_per_day', 0)
        
        # Base score of 100, reduce based on alert frequency
        health_penalty = min(30, health_alerts_per_day * 5)
        feeding_penalty = min(20, feeding_delays_per_day * 10)
        
        return max(0, 100 - health_penalty - feeding_penalty)
    
    def _generate_recommendations(self, grooming: Dict, staff: Dict, alerts: Dict) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Grooming recommendations
        if not grooming.get('error') and grooming.get('pets_overdue_grooming', 0) > 0:
            recommendations.append(f"Schedule grooming for {grooming['pets_overdue_grooming']} overdue pets")
        
        # Staff recommendations
        if not staff.get('error'):
            below_target = len(staff.get('staff_needing_support', []))
            if below_target > 0:
                recommendations.append(f"Provide additional training/support to {below_target} staff members")
        
        # Alert recommendations
        if not alerts.get('error'):
            if alerts.get('avg_health_alerts_per_day', 0) > 2:
                recommendations.append("Review health monitoring protocols - high alert frequency detected")
            
            if alerts.get('avg_feeding_delays_per_day', 0) > 1:
                recommendations.append("Optimize feeding schedules to reduce delays")
        
        if not recommendations:
            recommendations.append("Operations are running smoothly - maintain current standards")
        
        return recommendations