"""
Configuration for KennelOS Analytics

This file contains configurable parameters for analysis thresholds,
time windows, and other analytical settings. Modify these values
to adjust analysis behavior without changing core logic.
"""

# Time analysis windows (in days)
ANALYSIS_WINDOWS = {
    'short_term': 7,   # 1 week
    'medium_term': 30, # 1 month  
    'long_term': 90    # 3 months
}

# Pet wellness thresholds
PET_WELLNESS = {
    'min_activity_minutes_per_day': 60,
    'max_activity_minutes_per_day': 180,
    'min_feeding_times_per_day': 2,
    'max_feeding_times_per_day': 4,
    'weight_change_alert_threshold': 5.0  # percentage
}

# Environmental thresholds
ENVIRONMENT = {
    'temperature': {
        'optimal_min': 68.0,  # °F
        'optimal_max': 78.0,  # °F
        'alert_min': 60.0,
        'alert_max': 85.0
    },
    'humidity': {
        'optimal_min': 40.0,  # %
        'optimal_max': 60.0,  # %
        'alert_min': 30.0,
        'alert_max': 80.0
    },
    'noise': {
        'normal_max': 40.0,   # dB
        'alert_threshold': 45.0,
        'critical_threshold': 50.0
    }
}

# Operations thresholds
OPERATIONS = {
    'staff_performance': {
        'min_tasks_per_hour': 0.8,
        'target_tasks_per_hour': 1.2,
        'max_tasks_per_hour': 2.0
    },
    'grooming_frequency': {
        'target_days_between': 7,  # weekly grooming target
        'alert_days_overdue': 10
    },
    'feeding_schedule': {
        'expected_interval_hours': 8,
        'late_threshold_minutes': 30
    }
}

# Chart colors and styling
CHART_COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e', 
    'success': '#2ca02c',
    'warning': '#ff9800',
    'danger': '#d62728',
    'info': '#17a2b8'
}

# Analysis refresh intervals (in minutes)
REFRESH_INTERVALS = {
    'real_time': 5,
    'frequent': 15,
    'standard': 60,
    'daily': 1440
}