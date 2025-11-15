import pandas as pd
import numpy as np
import logging
from utils import config
from utils.helpers import convert_decimal_comma_to_dot, normalize_text, save_dataframe

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("="*80)
    logger.info("CLEAN PHASE - Cleaning and standardizing data")
    logger.info("="*80)
    
    df = df.copy()
    initial_count = len(df)
    
    logger.info("\n1. Converting numeric fields...")
    df = convert_numeric_fields(df)
    
    logger.info("\n2. Parsing dates and times...")
    df = parse_datetime_fields(df)
    
    logger.info("\n3. Standardizing text fields...")
    df = standardize_text_fields(df)
    
    logger.info("\n4. Handling missing values...")
    df = handle_missing_values(df)
    
    logger.info("\n5. Validating cleaned data...")
    validate_cleaned_data(df)
    
    final_count = len(df)
    logger.info(f"\n✓ Cleaning complete: {initial_count:,} → {final_count:,} records")
    
    save_dataframe(df, config.CLEANED_FILE, "cleaned data")
    
    return df


def convert_numeric_fields(df: pd.DataFrame) -> pd.DataFrame:
    if 'km' in df.columns:
        logger.info("   Converting km field...")
        df['km'] = df['km'].apply(convert_decimal_comma_to_dot)
    
    if 'latitude' in df.columns:
        logger.info("   Converting latitude field...")
        df['latitude'] = df['latitude'].apply(convert_decimal_comma_to_dot)
    
    if 'longitude' in df.columns:
        logger.info("   Converting longitude field...")
        df['longitude'] = df['longitude'].apply(convert_decimal_comma_to_dot)
    
    int_fields = ['id', 'br', 'pessoas', 'mortos', 'feridos_leves', 
                  'feridos_graves', 'ilesos', 'ignorados', 'feridos', 'veiculos']
    
    for field in int_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).astype(int)
    
    logger.info(f"   ✓ Converted {len([f for f in int_fields if f in df.columns]) + 3} numeric fields")
    
    return df


def parse_datetime_fields(df: pd.DataFrame) -> pd.DataFrame:
    if 'data_inversa' in df.columns:
        logger.info("   Parsing date field...")
        df['date'] = pd.to_datetime(df['data_inversa'], format='%Y-%m-%d', errors='coerce')
        
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day_of_month'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek
        df['week_of_year'] = df['date'].dt.isocalendar().week
    
    if 'horario' in df.columns:
        logger.info("   Parsing time field...")
        df['time'] = pd.to_datetime(df['horario'], format='%H:%M:%S', errors='coerce').dt.time
        
        df['hour'] = pd.to_datetime(df['horario'], format='%H:%M:%S', errors='coerce').dt.hour
    
    if 'date' in df.columns and 'horario' in df.columns:
        logger.info("   Creating datetime field...")
        df['datetime'] = pd.to_datetime(
            df['data_inversa'] + ' ' + df['horario'],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce'
        )
    
    logger.info("   ✓ Parsed date and time fields")
    
    return df


def standardize_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    text_fields = ['uf', 'municipio', 'causa_acidente', 'tipo_acidente',
                   'classificacao_acidente', 'fase_dia', 'sentido_via',
                   'condicao_metereologica', 'tipo_pista', 'tracado_via',
                   'uso_solo', 'regional', 'delegacia', 'uop', 'dia_semana']
    
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].apply(normalize_text)
    
    logger.info(f"   ✓ Standardized {len([f for f in text_fields if f in df.columns])} text fields")
    
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    numeric_fields = ['pessoas', 'mortos', 'feridos_leves', 'feridos_graves',
                      'ilesos', 'ignorados', 'feridos', 'veiculos']
    
    for field in numeric_fields:
        if field in df.columns:
            df[field] = df[field].fillna(0)
    
    text_fields = ['regional', 'delegacia', 'uop']
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].fillna('Desconhecido')
    
    if 'classificacao_acidente' in df.columns:
        most_common = df['classificacao_acidente'].mode()[0] if len(df['classificacao_acidente'].mode()) > 0 else 'Desconhecido'
        df['classificacao_acidente'] = df['classificacao_acidente'].fillna(most_common)
    
    missing_summary = df.isnull().sum()
    remaining_missing = missing_summary[missing_summary > 0]
    
    if len(remaining_missing) > 0:
        logger.info(f"   Remaining missing values:")
        for col, count in remaining_missing.items():
            logger.info(f"      {col}: {count:,} ({count/len(df)*100:.2f}%)")
    else:
        logger.info("   ✓ No critical missing values")
    
    return df


def validate_cleaned_data(df: pd.DataFrame):
    numeric_checks = ['km', 'latitude', 'longitude', 'pessoas', 'mortos']
    for field in numeric_checks:
        if field in df.columns:
            if not pd.api.types.is_numeric_dtype(df[field]):
                logger.warning(f"{field} is not numeric type")
            else:
                logger.info(f"   ✓ {field} is numeric")
    
    datetime_checks = ['date', 'hour', 'datetime']
    for field in datetime_checks:
        if field not in df.columns:
            logger.warning(f"Missing {field} field")
        else:
            logger.info(f"   ✓ {field} field created")
    
    if 'latitude' in df.columns and 'longitude' in df.columns:
        valid_coords = (
            (df['latitude'].between(-35, 5)) &
            (df['longitude'].between(-75, -30)) &
            (df['latitude'] != 0) &
            (df['longitude'] != 0)
        )
        valid_count = valid_coords.sum()
        valid_pct = valid_count / len(df) * 100
        logger.info(f"   ✓ Valid coordinates: {valid_count:,} ({valid_pct:.1f}%)")
    
    logger.info("   ✓ Validation complete")


if __name__ == "__main__":
    from extract.extract_data import extract_data
    
    df_raw = extract_data()
    df_clean = clean_data(df_raw)
    
    print(f"\n✓ Cleaned data shape: {df_clean.shape}")
    print(f"✓ Cleaned data types:\n{df_clean.dtypes}")
