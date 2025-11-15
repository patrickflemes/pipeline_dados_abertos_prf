import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple
from utils import config

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def convert_decimal_comma_to_dot(value):
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(',', '.'))
    except (ValueError, AttributeError):
        return np.nan


def get_brazilian_region(uf: str) -> str:
    for region, states in config.BRAZILIAN_REGIONS.items():
        if uf in states:
            return region
    return 'Unknown'


def get_time_period(hour: int) -> str:
    for period, (start, end) in config.TIME_PERIODS.items():
        if start <= hour < end:
            return period
    return 'noite'


def is_rush_hour(hour: int) -> bool:
    for start, end in config.RUSH_HOURS:
        if start <= hour < end:
            return True
    return False


def categorize_cause(cause: str) -> str:
    if pd.isna(cause):
        return 'unknown'
    
    cause_lower = cause.lower()
    
    if any(keyword.lower() in cause_lower for keyword in config.HUMAN_CAUSES):
        return 'human'
    
    if any(keyword.lower() in cause_lower for keyword in config.MECHANICAL_CAUSES):
        return 'mechanical'
    
    if any(keyword.lower() in cause_lower for keyword in config.ENVIRONMENTAL_CAUSES):
        return 'environmental'
    
    return 'other'


def get_marker_color(severity_class: str) -> str:
    return config.SEVERITY_COLORS.get(severity_class, '#CCCCCC')


def get_marker_size(num_people: int) -> str:
    if num_people <= 2:
        return 'small'
    elif num_people <= 5:
        return 'medium'
    elif num_people <= 10:
        return 'large'
    else:
        return 'xlarge'


def calculate_severity_score(row: pd.Series) -> float:
    try:
        deaths = row['mortos']
        serious = row['feridos_graves']
        light = row['feridos_leves']
        total = row['pessoas']
        
        if total == 0:
            return 0.0
        
        weighted_sum = (deaths * 10) + (serious * 3) + (light * 1)
        score = (weighted_sum / total) * 10
        
        return min(score, 100.0)
    except:
        return 0.0


def create_popup_html(row: pd.Series) -> str:
    deaths_emoji = '⚠️' if row['mortos'] > 0 else ''
    
    html = f"""
    <div style="width:280px; font-family: Arial, sans-serif;">
        <h3 style="margin:0; color:#333;">BR-{row['br']} km {row['km']}</h3>
        <p style="margin:5px 0; color:#666;"><strong>{row['municipio']}, {row['uf']}</strong></p>
        <p style="margin:5px 0; color:#999; font-size:12px;">{row['data_inversa']} {row['horario']}</p>
        <hr style="border:none; border-top:1px solid #eee; margin:8px 0;">
        <p style="margin:5px 0;"><strong>Tipo:</strong> {row['tipo_acidente']}</p>
        <p style="margin:5px 0;">
            <strong>Mortos:</strong> {row['mortos']} {deaths_emoji} | 
            <strong>Feridos:</strong> {row['feridos']}
        </p>
        <p style="margin:5px 0; font-size:12px; color:#666;">
            <strong>Causa:</strong> {row['causa_acidente'][:50]}...
        </p>
        <p style="margin:5px 0; font-size:12px; color:#666;">
            <strong>Clima:</strong> {row['condicao_metereologica']}
        </p>
    </div>
    """
    return html.replace('\n', '').replace('  ', '')


def create_tooltip_text(row: pd.Series) -> str:
    if row['mortos'] > 0:
        victims = f"{row['mortos']} morte(s)"
    elif row['feridos'] > 0:
        victims = f"{row['feridos']} ferido(s)"
    else:
        victims = "sem vítimas"
    
    return f"BR-{row['br']} km {row['km']} | {victims}"


def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ''
    return ' '.join(str(text).strip().split())


def validate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Validating coordinates...")
    
    lat_min, lat_max = -35, 5
    lon_min, lon_max = -75, -30
    
    initial_count = len(df)
    
    df['valid_coords'] = (
        (df['latitude'].between(lat_min, lat_max)) &
        (df['longitude'].between(lon_min, lon_max)) &
        (df['latitude'] != 0) &
        (df['longitude'] != 0) &
        df['latitude'].notna() &
        df['longitude'].notna()
    )
    
    invalid_count = (~df['valid_coords']).sum()
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} records with invalid coordinates ({invalid_count/initial_count*100:.1f}%)")
    
    return df


def calculate_distance_km(lat1, lon1, lat2, lon2):
    R = 6371
    
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c


def create_directory_structure():
    directories = [
        config.DATA_DIR,
        config.RAW_DIR,
        config.STAGING_DIR,
        config.FINAL_DIR
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")


def save_dataframe(df: pd.DataFrame, filepath: str, description: str = ""):
    try:
        df.to_csv(filepath, index=False, encoding=config.OUTPUT_ENCODING, sep=config.OUTPUT_SEPARATOR)
        size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved {description}: {filepath.name} ({len(df):,} rows, {size_mb:.2f} MB)")
    except Exception as e:
        logger.error(f"✗ Failed to save {filepath.name}: {e}")
        raise


def load_dataframe(filepath: str, description: str = "", **kwargs) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath, **kwargs)
        logger.info(f"✓ Loaded {description}: {filepath.name} ({len(df):,} rows)")
        return df
    except Exception as e:
        logger.error(f"✗ Failed to load {filepath.name}: {e}")
        raise


def print_progress(current: int, total: int, message: str = "Processing"):
    percent = current / total * 100
    bar_length = 50
    filled = int(bar_length * current / total)
    bar = '█' * filled + '░' * (bar_length - filled)
    print(f'\r{message}: |{bar}| {percent:.1f}% ({current}/{total})', end='', flush=True)
    if current == total:
        print()


def get_day_of_week_pt(day_name: str) -> str:
    mapping = {
        'Monday': 'segunda-feira',
        'Tuesday': 'terça-feira',
        'Wednesday': 'quarta-feira',
        'Thursday': 'quinta-feira',
        'Friday': 'sexta-feira',
        'Saturday': 'sábado',
        'Sunday': 'domingo'
    }
    return mapping.get(day_name, day_name)


def get_month_name_pt(month: int) -> str:
    months = ['', 'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
              'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    return months[month] if 1 <= month <= 12 else ''
