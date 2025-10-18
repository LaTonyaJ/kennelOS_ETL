"""
Transform module for KennelOS ETL pipeline.
Handles data cleaning, validation, and transformation.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation and cleaning operations."""
    
    def __init__(self):
        pass
    
    def transform_pet_activities(self, activities: List[Dict[str, Any]]) -> pd.DataFrame:
        """Transform pet activity data."""
        logger.info("Transforming pet activity data...")
        
        if not activities:
            logger.warning("No pet activity data to transform")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(activities)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Add derived fields
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.day_name()
        
        # Validate duration (should be positive)
        df['duration_minutes'] = pd.to_numeric(df['duration_minutes'], errors='coerce')
        df = df[df['duration_minutes'] > 0]
        
        # Clean activity types (standardize)
        df['activity_type'] = df['activity_type'].str.lower().str.strip()
        
        logger.info(f"Transformed {len(df)} pet activity records")
        return df
    
    def transform_environment_data(self, env_df: pd.DataFrame) -> pd.DataFrame:
        """Transform environment monitoring data."""
        logger.info("Transforming environment data...")
        
        if env_df.empty:
            logger.warning("No environment data to transform")
            return pd.DataFrame()
        
        df = env_df.copy()
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Add time-based features
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        
        # Validate numeric columns
        numeric_cols = ['temperature_f', 'humidity_percent', 'noise_level_db']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove rows with invalid measurements
        df = df.dropna(subset=numeric_cols)
        
        # Add comfort level indicators
        df['temp_comfort'] = df['temperature_f'].apply(self._categorize_temperature)
        df['humidity_comfort'] = df['humidity_percent'].apply(self._categorize_humidity)
        df['noise_comfort'] = df['noise_level_db'].apply(self._categorize_noise)
        
        logger.info(f"Transformed {len(df)} environment records")
        return df
    
    def transform_staff_logs(self, staff_df: pd.DataFrame) -> pd.DataFrame:
        """Transform staff log data."""
        logger.info("Transforming staff log data...")
        
        if staff_df.empty:
            logger.warning("No staff log data to transform")
            return pd.DataFrame()
        
        df = staff_df.copy()
        
        # Convert timestamps
        df['shift_start'] = pd.to_datetime(df['shift_start'])
        df['shift_end'] = pd.to_datetime(df['shift_end'])
        
        # Calculate shift duration
        df['shift_duration_hours'] = (df['shift_end'] - df['shift_start']).dt.total_seconds() / 3600
        
        # Add shift type
        df['shift_type'] = df['shift_start'].dt.hour.apply(self._categorize_shift)
        
        # Validate tasks completed
        df['tasks_completed'] = pd.to_numeric(df['tasks_completed'], errors='coerce')
        df = df[df['tasks_completed'] >= 0]
        
        # Calculate productivity metric
        df['tasks_per_hour'] = df['tasks_completed'] / df['shift_duration_hours']
        
        logger.info(f"Transformed {len(df)} staff log records")
        return df
    
    def _categorize_temperature(self, temp: float) -> str:
        """Categorize temperature comfort level."""
        if pd.isna(temp):
            return 'unknown'
        elif temp < 65:
            return 'cold'
        elif temp <= 78:
            return 'comfortable'
        else:
            return 'warm'
    
    def _categorize_humidity(self, humidity: float) -> str:
        """Categorize humidity comfort level."""
        if pd.isna(humidity):
            return 'unknown'
        elif humidity < 30:
            return 'dry'
        elif humidity <= 60:
            return 'comfortable'
        else:
            return 'humid'
    
    def _categorize_noise(self, noise: float) -> str:
        """Categorize noise comfort level."""
        if pd.isna(noise):
            return 'unknown'
        elif noise < 35:
            return 'quiet'
        elif noise <= 45:
            return 'normal'
        else:
            return 'loud'
    
    def _categorize_shift(self, hour: int) -> str:
        """Categorize shift type based on start hour."""
        if 5 <= hour < 14:
            return 'morning'
        elif 14 <= hour < 22:
            return 'afternoon'
        else:
            return 'night'
    
    def create_daily_summary(self, pet_df: pd.DataFrame, env_df: pd.DataFrame, 
                           staff_df: pd.DataFrame) -> pd.DataFrame:
        """Create a daily summary combining all data sources."""
        logger.info("Creating daily summary...")
        
        summaries = []
        
        # Get unique dates from all sources
        dates = set()
        if not pet_df.empty:
            dates.update(pet_df['date'].unique())
        if not env_df.empty:
            dates.update(env_df['date'].unique())
        if not staff_df.empty:
            dates.update(pd.to_datetime(staff_df['shift_start']).dt.date.unique())
        
        for date in sorted(dates):
            summary = {'date': date}
            
            # Pet activity summary
            day_activities = pet_df[pet_df['date'] == date] if not pet_df.empty else pd.DataFrame()
            summary['total_activities'] = len(day_activities)
            summary['total_activity_minutes'] = day_activities['duration_minutes'].sum() if not day_activities.empty else 0
            summary['unique_pets'] = day_activities['pet_id'].nunique() if not day_activities.empty else 0
            
            # Environment summary
            day_env = env_df[env_df['date'] == date] if not env_df.empty else pd.DataFrame()
            if not day_env.empty:
                summary['avg_temperature'] = day_env['temperature_f'].mean()
                summary['avg_humidity'] = day_env['humidity_percent'].mean()
                summary['avg_noise'] = day_env['noise_level_db'].mean()
            else:
                summary['avg_temperature'] = None
                summary['avg_humidity'] = None
                summary['avg_noise'] = None
            
            # Staff summary
            day_staff = staff_df[pd.to_datetime(staff_df['shift_start']).dt.date == date] if not staff_df.empty else pd.DataFrame()
            summary['staff_shifts'] = len(day_staff)
            summary['total_tasks'] = day_staff['tasks_completed'].sum() if not day_staff.empty else 0
            
            summaries.append(summary)
        
        summary_df = pd.DataFrame(summaries)
        logger.info(f"Created daily summary with {len(summary_df)} days")
        return summary_df
    
    def transform_all_data(self, raw_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Transform all data sources."""
        logger.info("Starting full data transformation...")
        
        transformed_data = {}
        
        # Transform individual datasets
        transformed_data['pet_activities'] = self.transform_pet_activities(
            raw_data.get('pet_activities', [])
        )
        transformed_data['environment'] = self.transform_environment_data(
            raw_data.get('environment', pd.DataFrame())
        )
        transformed_data['staff_logs'] = self.transform_staff_logs(
            raw_data.get('staff_logs', pd.DataFrame())
        )
        
        # Create summary
        transformed_data['daily_summary'] = self.create_daily_summary(
            transformed_data['pet_activities'],
            transformed_data['environment'],
            transformed_data['staff_logs']
        )
        
        logger.info("Data transformation completed")
        return transformed_data


if __name__ == "__main__":
    # Test the transformer with sample data
    from extract import DataExtractor
    
    extractor = DataExtractor()
    transformer = DataTransformer()
    
    raw_data = extractor.extract_all_data()
    transformed_data = transformer.transform_all_data(raw_data)
    
    print("Transformation Summary:")
    for key, df in transformed_data.items():
        print(f"{key}: {len(df)} records")