#!/usr/bin/env python3
"""
KennelOS Analytics - ETL Pipeline Main Module

This is the main entry point for the KennelOS ETL (Extract, Transform, Load) pipeline.
It orchestrates the complete data processing workflow from raw data files to 
processed analytics outputs.

Usage:
    python main.py

The pipeline will:
1. Extract data from source files (JSON, CSV)
2. Transform and clean the data
3. Load the processed data to output files
4. Generate summary and quality reports
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add the etl directory to the Python path for imports
sys.path.append(str(Path(__file__).parent / "etl"))

try:
    from etl.extract import DataExtractor
    from etl.transform import DataTransformer
    from etl.load import DataLoader
except ImportError as e:
    print(f"Error importing ETL modules: {e}")
    print("Make sure all required dependencies are installed:")
    print("  pip install pandas")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """
    Execute the complete ETL pipeline.
    
    Returns:
        bool: True if pipeline completes successfully, False otherwise
    """
    logger.info("="*60)
    logger.info("Starting KennelOS Analytics ETL Pipeline")
    logger.info(f"Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    try:
        # Initialize ETL components
        logger.info("Initializing ETL components...")
        extractor = DataExtractor(data_dir="data")
        transformer = DataTransformer()
        loader = DataLoader(output_dir="output")
        
        # Step 1: Extract
        logger.info("\n" + "="*30)
        logger.info("STEP 1: DATA EXTRACTION")
        logger.info("="*30)
        
        raw_data = extractor.extract_all_data()
        
        if not any(raw_data.values()):
            logger.error("No data extracted. Check if data files exist and are readable.")
            return False
        
        # Step 2: Transform
        logger.info("\n" + "="*30)
        logger.info("STEP 2: DATA TRANSFORMATION")
        logger.info("="*30)
        
        transformed_data = transformer.transform_all_data(raw_data)
        
        # Step 3: Load
        logger.info("\n" + "="*30)
        logger.info("STEP 3: DATA LOADING")
        logger.info("="*30)
        
        load_results = loader.load_all_data(transformed_data)
        
        # Generate additional reports
        logger.info("\n" + "="*30)
        logger.info("STEP 4: REPORT GENERATION")
        logger.info("="*30)
        
        quality_report_success = loader.create_data_quality_report(transformed_data)
        
        # Summary
        logger.info("\n" + "="*30)
        logger.info("PIPELINE SUMMARY")
        logger.info("="*30)
        
        successful_loads = sum(1 for success in load_results.values() if success)
        total_loads = len(load_results)
        
        logger.info(f"Data Loading: {successful_loads}/{total_loads} operations successful")
        logger.info(f"Quality Report: {'SUCCESS' if quality_report_success else 'FAILED'}")
        
        # Check if pipeline was successful
        pipeline_success = (successful_loads == total_loads) and quality_report_success
        
        if pipeline_success:
            logger.info("✅ ETL Pipeline completed successfully!")
            logger.info("Check the 'output' directory for processed data and reports.")
        else:
            logger.warning("⚠️  ETL Pipeline completed with some issues.")
            logger.warning("Check the logs above for details on failed operations.")
        
        logger.info(f"Pipeline finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60)
        
        return pipeline_success
        
    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed with error: {e}")
        logger.error("Check the error details above and ensure all dependencies are installed.")
        return False


def check_dependencies():
    """
    Check if required dependencies are available.
    
    Returns:
        bool: True if all dependencies are available, False otherwise
    """
    logger.info("Checking dependencies...")
    
    missing_deps = []
    
    try:
        import pandas
        logger.info("✅ pandas is available")
    except ImportError:
        missing_deps.append("pandas")
    
    if missing_deps:
        logger.error("❌ Missing required dependencies:")
        for dep in missing_deps:
            logger.error(f"  - {dep}")
        logger.error("\nInstall missing dependencies with:")
        logger.error("  pip install " + " ".join(missing_deps))
        return False
    
    logger.info("✅ All dependencies are available")
    return True


def check_data_files():
    """
    Check if required data files exist.
    
    Returns:
        bool: True if all required files exist, False otherwise
    """
    logger.info("Checking data files...")
    
    required_files = [
        "data/pet_activity.json",
        "data/environment.csv", 
        "data/staff_logs.csv"
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
        else:
            logger.info(f"✅ {file_path} exists")
    
    if missing_files:
        logger.error("❌ Missing required data files:")
        for file_path in missing_files:
            logger.error(f"  - {file_path}")
        return False
    
    logger.info("✅ All required data files are available")
    return True


def main():
    """Main entry point for the ETL pipeline."""
    logger.info("KennelOS Analytics ETL Pipeline")
    logger.info("Initializing...")
    
    # Pre-flight checks
    if not check_dependencies():
        sys.exit(1)
    
    if not check_data_files():
        logger.error("Please ensure all required data files are present before running the pipeline.")
        sys.exit(1)
    
    # Run the pipeline
    success = run_etl_pipeline()
    
    if success:
        print("\n🎉 Pipeline completed successfully!")
        print("📁 Check the 'output' directory for your processed data and reports.")
        sys.exit(0)
    else:
        print("\n❌ Pipeline failed. Check the logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()