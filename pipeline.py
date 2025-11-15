#!/usr/bin/env python3

import logging
import time
from datetime import datetime
from pathlib import Path

from extract.extract_data import extract_data
from transform.clean_data import clean_data
from transform.enrich_data import enrich_data
from transform.calculate_risks import calculate_risks
from transform.geographic_analysis import analyze_geography
from transform.aggregate_data import aggregate_data
from load.export_data import export_data
from utils import config

logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def print_header():
    print("\n" + "="*80)
    print("PRF TRAFFIC ACCIDENT DATA PIPELINE")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python Pipeline v1.0")
    print("="*80 + "\n")


def print_footer(start_time: float):
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print("\n" + "="*80)
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*80)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total time: {minutes}m {seconds}s")
    print(f"\nOutput directory: {config.FINAL_DIR}")
    print("\n✓ All files ready for dashboard!")
    print("="*80 + "\n")


def run_pipeline():
    start_time = time.time()
    
    try:
        print_header()
        
        logger.info("Stage 1/7: EXTRACT")
        df = extract_data()
        
        logger.info("Stage 2/7: CLEAN")
        df = clean_data(df)
        
        logger.info("Stage 3/7: ENRICH")
        df = enrich_data(df)
        
        logger.info("Stage 4/7: CALCULATE RISKS")
        df = calculate_risks(df)
        
        logger.info("Stage 5/7: GEOGRAPHIC ANALYSIS")
        df, clusters, segments = analyze_geography(df)
        
        logger.info("Stage 6/7: AGGREGATE")
        aggregated = aggregate_data(df, clusters, segments)
        
        logger.info("Stage 7/7: LOAD & EXPORT")
        export_data(df, aggregated)
        
        print_footer(start_time)
        logger.info("✓ Pipeline completed successfully")
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"✗ File not found: {e}")
        print(f"\nERROR: {e}")
        print("\nMake sure the raw data file exists at:")
        print(f"   {config.RAW_FILE}")
        return 1
        
    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}", exc_info=True)
        print(f"\nERROR: Pipeline failed")
        print(f"   {e}")
        print("\nCheck pipeline.log for details")
        return 1


def main():
    exit_code = run_pipeline()
    exit(exit_code)


if __name__ == "__main__":
    main()
