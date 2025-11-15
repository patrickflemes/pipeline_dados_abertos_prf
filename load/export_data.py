import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
from utils import config
from utils.helpers import save_dataframe

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def export_data(df: pd.DataFrame, aggregated: dict):
    logger.info("="*80)
    logger.info("LOAD PHASE - Exporting final data")
    logger.info("="*80)
    
    config.FINAL_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info("\n1. Exporting main detailed file...")
    save_dataframe(df, config.OUTPUT_FILES['detailed'], "accidents_detailed")
    
    logger.info("\n2. Exporting risk by time...")
    if 'risk_time' in aggregated and not aggregated['risk_time'].empty:
        save_dataframe(aggregated['risk_time'], config.OUTPUT_FILES['risk_time'], "risk_by_time")
    
    logger.info("\n3. Exporting risk by location...")
    if 'risk_location' in aggregated and not aggregated['risk_location'].empty:
        save_dataframe(aggregated['risk_location'], config.OUTPUT_FILES['risk_location'], "risk_by_location")
    
    logger.info("\n4. Exporting highway segments...")
    if 'segments' in aggregated and not aggregated['segments'].empty:
        save_dataframe(aggregated['segments'], config.OUTPUT_FILES['highway_segments'], "highway_segments_risk")
    
    logger.info("\n5. Exporting worst scenarios...")
    if 'scenarios' in aggregated and not aggregated['scenarios'].empty:
        save_dataframe(aggregated['scenarios'], config.OUTPUT_FILES['worst_scenarios'], "worst_scenarios")
    
    logger.info("\n6. Exporting danger rankings...")
    if 'rankings' in aggregated and not aggregated['rankings'].empty:
        save_dataframe(aggregated['rankings'], config.OUTPUT_FILES['danger_rankings'], "danger_rankings")
    
    logger.info("\n7. Exporting map points...")
    if 'map_points' in aggregated and not aggregated['map_points'].empty:
        save_dataframe(aggregated['map_points'], config.OUTPUT_FILES['map_points'], "accidents_map_points")
    
    logger.info("\n8. Exporting heatmap clusters...")
    if 'heatmap' in aggregated and not aggregated['heatmap'].empty:
        save_dataframe(aggregated['heatmap'], config.OUTPUT_FILES['heatmap_clusters'], "accident_heatmap_clusters")
    
    logger.info("\n9. Exporting daily calendar...")
    if 'daily' in aggregated and not aggregated['daily'].empty:
        save_dataframe(aggregated['daily'], config.OUTPUT_FILES['daily_calendar'], "daily_risk_calendar")
    
    logger.info("\n10. Exporting worst answers...")
    if 'answers' in aggregated and not aggregated['answers'].empty:
        save_dataframe(aggregated['answers'], config.OUTPUT_FILES['worst_answers'], "worst_answers")
    
    logger.info("\n11. Creating metadata file...")
    create_metadata(df, aggregated)
    
    print_export_summary(df, aggregated)
    
    logger.info("\n" + "="*80)
    logger.info("✓ EXPORT COMPLETE - All files ready!")
    logger.info("="*80)


def create_metadata(df: pd.DataFrame, aggregated: dict):
    metadata = {
        'pipeline_version': '1.0',
        'created_at': datetime.now().isoformat(),
        'data_summary': {
            'total_accidents': int(len(df)),
            'total_deaths': int(df['mortos'].sum()) if 'mortos' in df.columns else 0,
            'total_injuries': int(df['feridos'].sum()) if 'feridos' in df.columns else 0,
            'date_range': {
                'start': df['date'].min().strftime('%Y-%m-%d') if 'date' in df.columns else None,
                'end': df['date'].max().strftime('%Y-%m-%d') if 'date' in df.columns else None
            },
            'geographic_coverage': {
                'states': int(df['uf'].nunique()) if 'uf' in df.columns else 0,
                'highways': int(df['br'].nunique()) if 'br' in df.columns else 0,
                'cities': int(df['municipio'].nunique()) if 'municipio' in df.columns else 0
            }
        },
        'output_files': {
            'accidents_detailed': {
                'rows': len(df),
                'columns': len(df.columns),
                'file': config.OUTPUT_FILES['detailed'].name
            }
        },
        'data_quality': {
            'completeness_pct': float((1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100),
            'valid_coordinates_count': int(df['valid_coords'].sum()) if 'valid_coords' in df.columns else 0
        }
    }
    
    for key, df_agg in aggregated.items():
        if isinstance(df_agg, pd.DataFrame) and not df_agg.empty:
            metadata['output_files'][key] = {
                'rows': len(df_agg),
                'columns': len(df_agg.columns)
            }
    
    metadata_file = config.FINAL_DIR / "metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    logger.info(f"   ✓ Created metadata file: {metadata_file.name}")


def print_export_summary(df: pd.DataFrame, aggregated: dict):
    print("\n" + "="*80)
    print("EXPORT SUMMARY")
    print("="*80)
    
    print(f"\nOutput Directory: {config.FINAL_DIR}")
    print(f"\nFiles Created:")
    
    for key, filepath in config.OUTPUT_FILES.items():
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            print(f"   ✓ {filepath.name:35} ({size_mb:6.2f} MB)")
        else:
            print(f"   ✗ {filepath.name:35} (not created)")
    
    metadata_file = config.FINAL_DIR / "metadata.json"
    if metadata_file.exists():
        print(f"   ✓ {metadata_file.name:35}")
    
    print(f"\nData Summary:")
    print(f"   Total accidents: {len(df):,}")
    if 'mortos' in df.columns:
        print(f"   Total deaths: {df['mortos'].sum():,}")
    if 'feridos' in df.columns:
        print(f"   Total injuries: {df['feridos'].sum():,}")
    if 'date' in df.columns:
        print(f"   Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    
    print("\n" + "="*80)
    print("✓ All files exported successfully!")
    print("="*80)


if __name__ == "__main__":
    print("Export module - ready to use in pipeline")
