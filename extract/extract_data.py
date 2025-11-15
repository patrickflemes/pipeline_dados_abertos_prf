import pandas as pd
import logging
from pathlib import Path
from utils import config
from utils.helpers import load_dataframe, create_directory_structure

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def extract_data() -> pd.DataFrame:
    logger.info("="*80)
    logger.info("EXTRACT PHASE - Loading raw data")
    logger.info("="*80)
    
    create_directory_structure()
    
    if not config.RAW_FILE.exists():
        raise FileNotFoundError(f"Raw data file not found: {config.RAW_FILE}")
    
    logger.info(f"Reading file: {config.RAW_FILE}")
    logger.info(f"Encoding: {config.ENCODING}, Separator: '{config.CSV_SEPARATOR}'")
    
    try:
        df = pd.read_csv(
            config.RAW_FILE,
            sep=config.CSV_SEPARATOR,
            encoding=config.ENCODING,
            low_memory=False
        )
        
        logger.info(f"✓ Successfully loaded {len(df):,} records")
        logger.info(f"✓ Found {len(df.columns)} columns")
        
        validate_raw_data(df)
        print_extraction_summary(df)
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Failed to extract data: {e}")
        raise


def validate_raw_data(df: pd.DataFrame):
    logger.info("\nValidating raw data...")
    
    expected_columns = [
        'id', 'data_inversa', 'dia_semana', 'horario', 'uf', 'br', 'km',
        'municipio', 'causa_acidente', 'tipo_acidente', 'classificacao_acidente',
        'fase_dia', 'condicao_metereologica', 'tipo_pista', 'tracado_via',
        'pessoas', 'mortos', 'feridos_leves', 'feridos_graves', 'feridos',
        'ilesos', 'veiculos', 'latitude', 'longitude'
    ]
    
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"Missing expected columns: {missing_columns}")
    else:
        logger.info("✓ All expected columns present")
    
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        logger.warning(f"Found {empty_rows} completely empty rows")
    
    duplicate_ids = df['id'].duplicated().sum()
    if duplicate_ids > 0:
        logger.warning(f"Found {duplicate_ids} duplicate IDs")
    else:
        logger.info("✓ No duplicate IDs found")
    
    if 'data_inversa' in df.columns:
        try:
            df['_temp_date'] = pd.to_datetime(df['data_inversa'], format='%Y-%m-%d', errors='coerce')
            min_date = df['_temp_date'].min()
            max_date = df['_temp_date'].max()
            logger.info(f"✓ Date range: {min_date.date()} to {max_date.date()}")
            df.drop('_temp_date', axis=1, inplace=True)
        except:
            logger.warning("Could not determine date range")
    
    if all(col in df.columns for col in ['mortos', 'feridos', 'ilesos', 'pessoas']):
        total_deaths = df['mortos'].sum()
        total_injuries = df['feridos'].sum()
        total_unharmed = df['ilesos'].sum()
        logger.info(f"✓ Total deaths: {total_deaths:,}")
        logger.info(f"✓ Total injuries: {total_injuries:,}")
        logger.info(f"✓ Total unharmed: {total_unharmed:,}")
    
    logger.info("✓ Validation complete")


def print_extraction_summary(df: pd.DataFrame):
    print("\n" + "="*80)
    print("EXTRACTION SUMMARY")
    print("="*80)
    
    print(f"\n Dataset Size:")
    print(f"   Total records: {len(df):,}")
    print(f"   Total columns: {len(df.columns)}")
    print(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    print(f"\n📋 Completeness:")
    total_cells = len(df) * len(df.columns)
    missing_cells = df.isnull().sum().sum()
    completeness = ((total_cells - missing_cells) / total_cells) * 100
    print(f"   Data quality: {completeness:.2f}%")
    print(f"   Missing values: {missing_cells:,} / {total_cells:,}")
    
    if 'classificacao_acidente' in df.columns:
        print(f"\n  Severity Distribution:")
        severity_counts = df['classificacao_acidente'].value_counts()
        for severity, count in severity_counts.items():
            pct = count / len(df) * 100
            print(f"   {severity}: {count:,} ({pct:.1f}%)")
    
    if 'uf' in df.columns:
        print(f"\n  Geographic Coverage:")
        print(f"   States: {df['uf'].nunique()}")
        if 'br' in df.columns:
            print(f"   Highways: {df['br'].nunique()}")
        if 'municipio' in df.columns:
            print(f"   Cities: {df['municipio'].nunique()}")
    
    print("\n" + "="*80)
    print("✓ Extract phase complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    df = extract_data()
    print(f"\n✓ Successfully extracted {len(df):,} records")
    print(f"✓ Columns: {list(df.columns[:10])}...")
