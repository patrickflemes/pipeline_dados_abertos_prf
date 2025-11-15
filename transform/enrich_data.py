import pandas as pd
import numpy as np
import logging
from utils import config
from utils.helpers import (get_brazilian_region, get_time_period, is_rush_hour,
                            categorize_cause, get_marker_color, get_marker_size,
                            calculate_severity_score, create_popup_html, create_tooltip_text,
                            save_dataframe, get_day_of_week_pt, get_month_name_pt)

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("="*80)
    logger.info("ENRICH PHASE - Adding calculated fields")
    logger.info("="*80)
    
    df = df.copy()
    
    logger.info("\n1. Adding temporal dimensions...")
    df = add_temporal_dimensions(df)
    
    logger.info("\n2. Adding geographic categories...")
    df = add_geographic_categories(df)
    
    logger.info("\n3. Adding accident characteristics...")
    df = add_accident_characteristics(df)
    
    logger.info("\n4. Adding risk flags...")
    df = add_risk_flags(df)
    
    logger.info("\n5. Adding map visualization fields...")
    df = add_map_visualization_fields(df)
    
    logger.info("\n6. Calculating severity scores...")
    df = add_severity_scores(df)
    
    logger.info(f"\n✓ Enrichment complete: {df.shape[1]} total columns")
    
    save_dataframe(df, config.ENRICHED_FILE, "enriched data")
    
    return df


def add_temporal_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    if 'month' in df.columns:
        df['quarter'] = df['month'].apply(lambda x: (x-1)//3 + 1 if pd.notna(x) else np.nan)
    
    if 'month' in df.columns:
        df['month_name'] = df['month'].apply(get_month_name_pt)
    
    if 'day_of_week' in df.columns:
        df['day_of_week_name'] = df['date'].dt.day_name()
        df['day_of_week_name_pt'] = df['day_of_week_name'].apply(get_day_of_week_pt)
    
    if 'day_of_week' in df.columns:
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    if 'hour' in df.columns:
        df['time_period'] = df['hour'].apply(get_time_period)
    
    if 'hour' in df.columns:
        df['is_rush_hour'] = df['hour'].apply(is_rush_hour).astype(int)
    
    if 'hour' in df.columns:
        df['is_business_hours'] = df['hour'].between(8, 18).astype(int)
    
    if 'hour' in df.columns:
        df['time_bucket_4h'] = (df['hour'] // 4) * 4
        df['time_bucket_4h'] = df['time_bucket_4h'].astype(str) + '-' + (df['time_bucket_4h'] + 4).astype(str)
    
    logger.info(f"   ✓ Added {8} temporal dimension fields")
    
    return df


def add_geographic_categories(df: pd.DataFrame) -> pd.DataFrame:
    if 'uf' in df.columns:
        df['state_region'] = df['uf'].apply(get_brazilian_region)
    
    if 'municipio' in df.columns:
        df['city_normalized'] = df['municipio'].str.upper().str.strip()
    
    if 'km' in df.columns:
        df['km_rounded'] = (df['km'] // 10) * 10
    
    if 'uso_solo' in df.columns:
        df['is_urban'] = (df['uso_solo'].str.lower() == 'sim').astype(int)
    
    logger.info(f"   ✓ Added {4} geographic category fields")
    
    return df


def add_accident_characteristics(df: pd.DataFrame) -> pd.DataFrame:
    if 'causa_acidente' in df.columns:
        df['cause_category'] = df['causa_acidente'].apply(categorize_cause)
    
    if 'classificacao_acidente' in df.columns:
        severity_map = {
            'Com Vítimas Fatais': 3,
            'Com Vítimas Feridas': 2,
            'Sem Vítimas': 1
        }
        df['severity_code'] = df['classificacao_acidente'].map(severity_map).fillna(0)
    
    if 'condicao_metereologica' in df.columns:
        df['weather_clear'] = df['condicao_metereologica'].str.contains('Claro|Sol', case=False, na=False).astype(int)
        df['weather_rain'] = df['condicao_metereologica'].str.contains('Chuva|Garoa', case=False, na=False).astype(int)
        df['weather_fog'] = df['condicao_metereologica'].str.contains('Nevoeiro|Neblina', case=False, na=False).astype(int)
    
    if 'tracado_via' in df.columns:
        df['has_curve'] = df['tracado_via'].str.contains('Curva', case=False, na=False).astype(int)
        df['has_slope'] = df['tracado_via'].str.contains('Aclive|Declive', case=False, na=False).astype(int)
        df['has_intersection'] = df['tracado_via'].str.contains('Interse', case=False, na=False).astype(int)
    
    logger.info(f"   ✓ Added {9} accident characteristic fields")
    
    return df


def add_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    if 'causa_acidente' in df.columns:
        df['alcohol_involved'] = df['causa_acidente'].str.contains('lcool', case=False, na=False).astype(int)
    
    if 'causa_acidente' in df.columns:
        df['driver_asleep'] = df['causa_acidente'].str.contains('Dormindo', case=False, na=False).astype(int)
    
    if 'causa_acidente' in df.columns:
        df['speed_related'] = df['causa_acidente'].str.contains('Velocidade', case=False, na=False).astype(int)
    
    if 'causa_acidente' in df.columns:
        df['mechanical_failure'] = df['causa_acidente'].str.contains('mec.+nica|el.+trica|pneu|freio', case=False, na=False).astype(int)
    
    if 'causa_acidente' in df.columns:
        df['weather_related'] = df['causa_acidente'].str.contains('Chuva|Pista|gua|neblina', case=False, na=False).astype(int)
    
    if 'fase_dia' in df.columns:
        df['is_night'] = df['fase_dia'].str.contains('Noite', case=False, na=False).astype(int)
    
    if 'condicao_metereologica' in df.columns and 'fase_dia' in df.columns:
        df['poor_visibility'] = (
            (df['condicao_metereologica'].str.contains('Nevoeiro|Neblina|Chuva', case=False, na=False)) |
            (df['fase_dia'].str.contains('Noite', case=False, na=False))
        ).astype(int)
    
    logger.info(f"   ✓ Added {7} risk flag fields")
    
    return df


def add_map_visualization_fields(df: pd.DataFrame) -> pd.DataFrame:
    if 'classificacao_acidente' in df.columns:
        df['marker_color'] = df['classificacao_acidente'].apply(get_marker_color)
    
    if 'pessoas' in df.columns:
        df['marker_size'] = df['pessoas'].apply(get_marker_size)
    
    df['marker_opacity'] = 0.7
    
    if all(col in df.columns for col in ['br', 'km', 'mortos', 'feridos']):
        df['tooltip_text'] = df.apply(create_tooltip_text, axis=1)
    
    required_cols = ['br', 'km', 'municipio', 'uf', 'data_inversa', 'horario',
                     'tipo_acidente', 'mortos', 'feridos', 'causa_acidente', 'condicao_metereologica']
    if all(col in df.columns for col in required_cols):
        logger.info("   Creating popup HTML (this may take a moment)...")
        df['popup_html'] = df.apply(create_popup_html, axis=1)
    
    logger.info(f"   ✓ Added {5} map visualization fields")
    
    return df


def add_severity_scores(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ['mortos', 'feridos_graves', 'feridos_leves', 'pessoas']
    if all(col in df.columns for col in required_cols):
        df['severity_score'] = df.apply(calculate_severity_score, axis=1)
    
    if 'mortos' in df.columns and 'pessoas' in df.columns:
        df['fatality_rate'] = np.where(
            df['pessoas'] > 0,
            (df['mortos'] / df['pessoas']) * 100,
            0
        )
    
    if 'feridos' in df.columns and 'pessoas' in df.columns:
        df['injury_rate'] = np.where(
            df['pessoas'] > 0,
            (df['feridos'] / df['pessoas']) * 100,
            0
        )
    
    logger.info(f"   ✓ Added {3} severity score fields")
    
    return df


if __name__ == "__main__":
    from extract.extract_data import extract_data
    from transform.clean_data import clean_data
    
    df_raw = extract_data()
    df_clean = clean_data(df_raw)
    df_enriched = enrich_data(df_clean)
    
    print(f"\n✓ Enriched data shape: {df_enriched.shape}")
    print(f"\n✓ New columns added:")
    new_cols = [col for col in df_enriched.columns if col not in df_clean.columns]
    for col in new_cols:
        print(f"   - {col}")
