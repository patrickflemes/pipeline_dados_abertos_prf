import pandas as pd
import numpy as np
import logging
from utils import config
from utils.helpers import save_dataframe

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def calculate_risks(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("="*80)
    logger.info("RISK CALCULATION PHASE - Computing risk scores")
    logger.info("="*80)
    
    df = df.copy()
    
    logger.info("\n1. Calculating time risk scores...")
    df = calculate_time_risk_scores(df)
    
    logger.info("\n2. Calculating location risk scores...")
    df = calculate_location_risk_scores(df)
    
    logger.info("\n3. Calculating condition risk scores...")
    df = calculate_condition_risk_scores(df)
    
    logger.info("\n4. Calculating composite risk scores...")
    df = calculate_composite_risk_score(df)
    
    logger.info("\n5. Calculating probability indices...")
    df = calculate_probability_indices(df)
    
    logger.info("\n6. Assigning danger rankings...")
    df = assign_rankings(df)
    
    logger.info("\n7. Identifying high-risk accidents...")
    df = identify_high_risk(df)
    
    logger.info("\n✓ Risk calculation complete")
    
    return df


def calculate_time_risk_scores(df: pd.DataFrame) -> pd.DataFrame:
    if 'hour' in df.columns:
        hour_stats = df.groupby('hour').agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum'
        }).rename(columns={'id': 'accidents'})
        
        hour_stats['fatality_rate'] = hour_stats['mortos'] / hour_stats['accidents'] * 100
        
        avg_accidents = hour_stats['accidents'].mean()
        avg_fatality = hour_stats['fatality_rate'].mean()
        
        hour_stats['risk_score'] = (
            (hour_stats['accidents'] / avg_accidents) * 50 +
            (hour_stats['fatality_rate'] / avg_fatality) * 50
        )
        
        df['hour_risk_score'] = df['hour'].map(hour_stats['risk_score']).fillna(50)
    
    if 'day_of_week' in df.columns:
        dow_stats = df.groupby('day_of_week').agg({
            'id': 'count',
            'mortos': 'sum'
        }).rename(columns={'id': 'accidents'})
        
        dow_stats['fatality_rate'] = dow_stats['mortos'] / dow_stats['accidents'] * 100
        avg_accidents = dow_stats['accidents'].mean()
        avg_fatality = dow_stats['fatality_rate'].mean()
        
        dow_stats['risk_score'] = (
            (dow_stats['accidents'] / avg_accidents) * 50 +
            (dow_stats['fatality_rate'] / avg_fatality) * 50
        )
        
        df['day_risk_score'] = df['day_of_week'].map(dow_stats['risk_score']).fillna(50)
    
    if 'hour_risk_score' in df.columns and 'day_risk_score' in df.columns:
        df['time_risk_score'] = (df['hour_risk_score'] + df['day_risk_score']) / 2
    elif 'hour_risk_score' in df.columns:
        df['time_risk_score'] = df['hour_risk_score']
    else:
        df['time_risk_score'] = 50
    
    logger.info("   ✓ Calculated time risk scores")
    
    return df


def calculate_location_risk_scores(df: pd.DataFrame) -> pd.DataFrame:
    if 'br' in df.columns:
        highway_stats = df.groupby('br').agg({
            'id': 'count',
            'mortos': 'sum',
            'km': 'nunique'
        }).rename(columns={'id': 'accidents', 'km': 'km_coverage'})
        
        highway_stats['accidents_per_km'] = highway_stats['accidents'] / highway_stats['km_coverage'].replace(0, 1)
        highway_stats['fatality_rate'] = highway_stats['mortos'] / highway_stats['accidents'] * 100
        
        avg_per_km = highway_stats['accidents_per_km'].mean()
        avg_fatality = highway_stats['fatality_rate'].mean()
        
        highway_stats['risk_score'] = (
            (highway_stats['accidents_per_km'] / avg_per_km) * 50 +
            (highway_stats['fatality_rate'] / avg_fatality) * 50
        )
        
        df['highway_risk_score'] = df['br'].map(highway_stats['risk_score']).fillna(50)
    
    if 'uf' in df.columns:
        state_stats = df.groupby('uf').agg({
            'id': 'count',
            'mortos': 'sum'
        }).rename(columns={'id': 'accidents'})
        
        state_stats['fatality_rate'] = state_stats['mortos'] / state_stats['accidents'] * 100
        avg_accidents = state_stats['accidents'].mean()
        avg_fatality = state_stats['fatality_rate'].mean()
        
        state_stats['risk_score'] = (
            (state_stats['accidents'] / avg_accidents) * 50 +
            (state_stats['fatality_rate'] / avg_fatality) * 50
        )
        
        df['state_risk_score'] = df['uf'].map(state_stats['risk_score']).fillna(50)
    
    if 'highway_risk_score' in df.columns and 'state_risk_score' in df.columns:
        df['location_risk_score'] = (df['highway_risk_score'] + df['state_risk_score']) / 2
    elif 'highway_risk_score' in df.columns:
        df['location_risk_score'] = df['highway_risk_score']
    else:
        df['location_risk_score'] = 50
    
    logger.info("   ✓ Calculated location risk scores")
    
    return df


def calculate_condition_risk_scores(df: pd.DataFrame) -> pd.DataFrame:
    if 'condicao_metereologica' in df.columns:
        weather_stats = df.groupby('condicao_metereologica').agg({
            'id': 'count',
            'mortos': 'sum'
        }).rename(columns={'id': 'accidents'})
        
        weather_stats['fatality_rate'] = weather_stats['mortos'] / weather_stats['accidents'] * 100
        avg_fatality = weather_stats['fatality_rate'].mean()
        
        weather_stats['risk_score'] = (weather_stats['fatality_rate'] / avg_fatality) * 100
        
        df['weather_risk_score'] = df['condicao_metereologica'].map(weather_stats['risk_score']).fillna(50)
    
    if 'tipo_pista' in df.columns:
        road_stats = df.groupby('tipo_pista').agg({
            'id': 'count',
            'mortos': 'sum'
        }).rename(columns={'id': 'accidents'})
        
        road_stats['fatality_rate'] = road_stats['mortos'] / road_stats['accidents'] * 100
        avg_fatality = road_stats['fatality_rate'].mean()
        
        road_stats['risk_score'] = (road_stats['fatality_rate'] / avg_fatality) * 100
        
        df['road_risk_score'] = df['tipo_pista'].map(road_stats['risk_score']).fillna(50)
    
    risk_scores = [col for col in ['weather_risk_score', 'road_risk_score'] if col in df.columns]
    if risk_scores:
        df['condition_risk_score'] = df[risk_scores].mean(axis=1)
    else:
        df['condition_risk_score'] = 50
    
    logger.info("   ✓ Calculated condition risk scores")
    
    return df


def calculate_composite_risk_score(df: pd.DataFrame) -> pd.DataFrame:
    df['composite_risk_score'] = (
        df['location_risk_score'] * 0.4 +
        df['time_risk_score'] * 0.3 +
        df['condition_risk_score'] * 0.3
    )
    
    if 'mortos' in df.columns:
        df['composite_risk_score'] = np.where(
            df['mortos'] > 0,
            np.minimum(df['composite_risk_score'] * 1.2, 100),
            df['composite_risk_score']
        )
    
    logger.info("   ✓ Calculated composite risk scores")
    
    return df


def calculate_probability_indices(df: pd.DataFrame) -> pd.DataFrame:
    df['accident_probability_index'] = 1.0
    
    if 'mortos' in df.columns:
        df['fatality_probability'] = np.where(df['mortos'] > 0, 1, 0)
    
    if 'feridos' in df.columns and 'pessoas' in df.columns:
        df['injury_probability'] = np.where(
            df['pessoas'] > 0,
            (df['feridos'] / df['pessoas']) * 100,
            0
        )
    
    if 'severity_score' in df.columns:
        df['danger_percentile'] = df['severity_score'].rank(pct=True) * 100
    
    logger.info("   ✓ Calculated probability indices")
    
    return df


def assign_rankings(df: pd.DataFrame) -> pd.DataFrame:
    if 'hour' in df.columns:
        hour_danger = df.groupby('hour')['id'].count().rank(ascending=False)
        df['hour_danger_rank'] = df['hour'].map(hour_danger)
    
    if 'day_of_week' in df.columns:
        day_danger = df.groupby('day_of_week')['id'].count().rank(ascending=False)
        df['day_danger_rank'] = df['day_of_week'].map(day_danger)
    
    if 'uf' in df.columns:
        state_danger = df.groupby('uf')['id'].count().rank(ascending=False)
        df['state_danger_rank'] = df['uf'].map(state_danger)
    
    if 'br' in df.columns:
        highway_danger = df.groupby('br')['id'].count().rank(ascending=False)
        df['highway_danger_rank'] = df['br'].map(highway_danger)
    
    logger.info("   ✓ Assigned danger rankings")
    
    return df


def identify_high_risk(df: pd.DataFrame) -> pd.DataFrame:
    if 'composite_risk_score' in df.columns:
        threshold = df['composite_risk_score'].quantile(0.80)
        df['is_high_risk'] = (df['composite_risk_score'] >= threshold).astype(int)
        
        high_risk_count = df['is_high_risk'].sum()
        logger.info(f"   ✓ Identified {high_risk_count:,} high-risk accidents ({high_risk_count/len(df)*100:.1f}%)")
    
    return df


if __name__ == "__main__":
    from extract.extract_data import extract_data
    from transform.clean_data import clean_data
    from transform.enrich_data import enrich_data
    
    df_raw = extract_data()
    df_clean = clean_data(df_raw)
    df_enriched = enrich_data(df_clean)
    df_risks = calculate_risks(df_enriched)
    
    print(f"\n✓ Risk calculation complete")
    print(f"✓ Data shape: {df_risks.shape}")
    print(f"\n✓ Risk score summary:")
    print(df_risks[['time_risk_score', 'location_risk_score', 'condition_risk_score', 'composite_risk_score']].describe())
