"""
Extract module for KennelOS ETL pipeline.
Handles data extraction from various sources.
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various file formats."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        
    def extract_json_data(self, filename: str) -> List[Dict[str, Any]]:
        """Extract data from JSON file."""
        try:
            file_path = self.data_dir / filename
            logger.info(f"Extracting data from {file_path}")
            
            with open(file_path, 'r') as file:
                data = json.load(file)
                
            logger.info(f"Successfully extracted {len(data)} records from {filename}")
            return data
            
        except FileNotFoundError:
            logger.error(f"File {filename} not found in {self.data_dir}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from {filename}: {e}")
            return []
    
    def extract_csv_data(self, filename: str) -> pd.DataFrame:
        """Extract data from CSV file."""
        try:
            file_path = self.data_dir / filename
            logger.info(f"Extracting data from {file_path}")
            
            df = pd.read_csv(file_path)
            logger.info(f"Successfully extracted {len(df)} rows from {filename}")
            return df
            
        except FileNotFoundError:
            logger.error(f"File {filename} not found in {self.data_dir}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading CSV from {filename}: {e}")
            return pd.DataFrame()
    
    def extract_pet_activities(self) -> List[Dict[str, Any]]:
        """Extract pet activity data."""
        return self.extract_json_data("pet_activity.json")
    
    def extract_environment_data(self) -> pd.DataFrame:
        """Extract environment monitoring data."""
        return self.extract_csv_data("environment.csv")
    
    def extract_staff_logs(self) -> pd.DataFrame:
        """Extract staff log data."""
        return self.extract_csv_data("staff_logs.csv")
    
    def extract_all_data(self) -> Dict[str, Any]:
        """Extract all available data sources."""
        logger.info("Starting full data extraction...")
        
        data = {
            'pet_activities': self.extract_pet_activities(),
            'environment': self.extract_environment_data(),
            'staff_logs': self.extract_staff_logs()
        }
        
        logger.info("Data extraction completed")
        return data


if __name__ == "__main__":
    # Test the extractor
    extractor = DataExtractor()
    all_data = extractor.extract_all_data()
    
    print("Extraction Summary:")
    print(f"Pet Activities: {len(all_data['pet_activities'])} records")
    print(f"Environment Data: {len(all_data['environment'])} records")
    print(f"Staff Logs: {len(all_data['staff_logs'])} records")