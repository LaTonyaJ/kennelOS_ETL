"""
Load module for KennelOS ETL pipeline.
Handles loading transformed data to various destinations.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any
import logging
from datetime import datetime
from . import db

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Handles loading transformed data to various destinations."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def load_to_csv(self, df: pd.DataFrame, filename: str) -> bool:
        """Load DataFrame to CSV file."""
        try:
            output_path = self.output_dir / filename
            logger.info(f"Loading data to {output_path}")
            
            df.to_csv(output_path, index=False)
            logger.info(f"Successfully loaded {len(df)} records to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to {filename}: {e}")
            return False
    
    def load_to_json(self, data: Any, filename: str) -> bool:
        """Load data to JSON file."""
        try:
            output_path = self.output_dir / filename
            logger.info(f"Loading data to {output_path}")
            
            # Convert pandas objects to serializable format
            if isinstance(data, pd.DataFrame):
                json_data = data.to_dict('records')
            else:
                json_data = data
            
            with open(output_path, 'w') as file:
                json.dump(json_data, file, indent=2, default=str)
                
            logger.info(f"Successfully loaded data to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to {filename}: {e}")
            return False

    def load_to_db(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', db_url: str | None = None) -> bool:
        """Load a DataFrame to a SQL database table using SQLAlchemy engine."""
        try:
            engine = db.get_engine(db_url=db_url)
            logger.info(f"Writing {len(df)} records to DB table '{table_name}'")

            # Use to_sql for quick persistence
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)

            logger.info(f"Successfully wrote to DB table '{table_name}'")
            return True
        except Exception as e:
            logger.error(f"Error writing to DB table {table_name}: {e}")
            return False
    
    def load_summary_report(self, transformed_data: Dict[str, pd.DataFrame]) -> bool:
        """Create and load a summary report."""
        try:
            report_path = self.output_dir / "summary_report.txt"
            logger.info(f"Creating summary report at {report_path}")
            
            with open(report_path, 'w') as file:
                file.write("KennelOS Analytics - ETL Summary Report\n")
                file.write("="*50 + "\n")
                file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                for dataset_name, df in transformed_data.items():
                    file.write(f"{dataset_name.upper()}\n")
                    file.write("-" * len(dataset_name) + "\n")
                    
                    if not df.empty:
                        file.write(f"Total Records: {len(df)}\n")
                        file.write(f"Columns: {', '.join(df.columns)}\n")
                        
                        # Add specific insights based on dataset
                        if dataset_name == 'pet_activities':
                            file.write(f"Unique Pets: {df['pet_id'].nunique()}\n")
                            file.write(f"Activity Types: {', '.join(df['activity_type'].unique())}\n")
                            file.write(f"Total Activity Time: {df['duration_minutes'].sum()} minutes\n")
                            
                        elif dataset_name == 'environment':
                            file.write(f"Temperature Range: {df['temperature_f'].min():.1f}°F - {df['temperature_f'].max():.1f}°F\n")
                            file.write(f"Humidity Range: {df['humidity_percent'].min():.1f}% - {df['humidity_percent'].max():.1f}%\n")
                            file.write(f"Noise Range: {df['noise_level_db'].min():.1f}dB - {df['noise_level_db'].max():.1f}dB\n")
                            
                        elif dataset_name == 'staff_logs':
                            file.write(f"Total Staff: {df['staff_id'].nunique()}\n")
                            file.write(f"Total Tasks Completed: {df['tasks_completed'].sum()}\n")
                            file.write(f"Average Tasks per Hour: {df['tasks_per_hour'].mean():.2f}\n")
                            
                        elif dataset_name == 'daily_summary':
                            file.write(f"Date Range: {df['date'].min()} to {df['date'].max()}\n")
                            file.write(f"Total Activities Across All Days: {df['total_activities'].sum()}\n")
                            file.write(f"Total Unique Pets: {df['unique_pets'].sum()}\n")
                    else:
                        file.write("No data available\n")
                    
                    file.write("\n")
            
            logger.info("Summary report created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating summary report: {e}")
            return False
    
    def load_all_data(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, bool]:
        """Load all transformed data to appropriate formats."""
        logger.info("Starting data loading process...")
        
        results = {}
        
        # Load each dataset to CSV and DB
        for dataset_name, df in transformed_data.items():
            if not df.empty:
                csv_filename = f"{dataset_name}.csv"
                results[f"{dataset_name}_csv"] = self.load_to_csv(df, csv_filename)
                
                # Also create JSON for some datasets
                if dataset_name in ['pet_activities', 'daily_summary']:
                    json_filename = f"{dataset_name}.json"
                    results[f"{dataset_name}_json"] = self.load_to_json(df, json_filename)

                # Also persist to DB (overwrite daily_summary, append others)
                db_table = dataset_name
                if_exists_mode = 'replace' if dataset_name == 'daily_summary' else 'append'
                results[f"{dataset_name}_db"] = self.load_to_db(df, db_table, if_exists=if_exists_mode)
        
        # Create summary report
        results['summary_report'] = self.load_summary_report(transformed_data)
        
        # Log overall results
        successful_loads = sum(1 for success in results.values() if success)
        total_loads = len(results)
        
        logger.info(f"Loading completed: {successful_loads}/{total_loads} successful")
        
        return results
    
    def create_data_quality_report(self, transformed_data: Dict[str, pd.DataFrame]) -> bool:
        """Create a data quality assessment report."""
        try:
            quality_path = self.output_dir / "data_quality_report.txt"
            logger.info(f"Creating data quality report at {quality_path}")
            
            with open(quality_path, 'w') as file:
                file.write("KennelOS Analytics - Data Quality Report\n")
                file.write("="*50 + "\n")
                file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                for dataset_name, df in transformed_data.items():
                    file.write(f"{dataset_name.upper()} QUALITY ASSESSMENT\n")
                    file.write("-" * (len(dataset_name) + 18) + "\n")
                    
                    if not df.empty:
                        # Basic stats
                        file.write(f"Total Records: {len(df)}\n")
                        file.write(f"Total Columns: {len(df.columns)}\n")
                        
                        # Missing values
                        missing_data = df.isnull().sum()
                        if missing_data.any():
                            file.write("\nMissing Values:\n")
                            for col, count in missing_data[missing_data > 0].items():
                                percentage = (count / len(df)) * 100
                                file.write(f"  {col}: {count} ({percentage:.1f}%)\n")
                        else:
                            file.write("\nNo missing values detected\n")
                        
                        # Duplicate records
                        duplicates = df.duplicated().sum()
                        file.write(f"\nDuplicate Records: {duplicates}\n")
                        
                        # Data type info
                        file.write("\nData Types:\n")
                        for col, dtype in df.dtypes.items():
                            file.write(f"  {col}: {dtype}\n")
                        
                    else:
                        file.write("Dataset is empty - no quality assessment available\n")
                    
                    file.write("\n" + "="*50 + "\n\n")
            
            logger.info("Data quality report created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating data quality report: {e}")
            return False


if __name__ == "__main__":
    # Test the loader
    from extract import DataExtractor
    from transform import DataTransformer
    
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()
    
    # Run full pipeline
    raw_data = extractor.extract_all_data()
    transformed_data = transformer.transform_all_data(raw_data)
    load_results = loader.load_all_data(transformed_data)
    loader.create_data_quality_report(transformed_data)
    
    print("Load Results:")
    for operation, success in load_results.items():
        status = "SUCCESS" if success else "FAILED"
        print(f"  {operation}: {status}")