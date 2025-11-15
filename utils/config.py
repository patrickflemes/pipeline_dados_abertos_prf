from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
FINAL_DIR = DATA_DIR / "final"

RAW_FILE = RAW_DIR / "raw.csv"

CLEANED_FILE = STAGING_DIR / "cleaned_data.csv"
ENRICHED_FILE = STAGING_DIR / "enriched_data.csv"

OUTPUT_FILES = {
    'detailed': FINAL_DIR / "accidents_detailed.csv",
    'risk_time': FINAL_DIR / "risk_by_time.csv",
    'risk_location': FINAL_DIR / "risk_by_location.csv",
    'highway_segments': FINAL_DIR / "highway_segments_risk.csv",
    'worst_scenarios': FINAL_DIR / "worst_scenarios.csv",
    'danger_rankings': FINAL_DIR / "danger_rankings.csv",
    'map_points': FINAL_DIR / "accidents_map_points.csv",
    'heatmap_clusters': FINAL_DIR / "accident_heatmap_clusters.csv",
    'daily_calendar': FINAL_DIR / "daily_risk_calendar.csv",
    'worst_answers': FINAL_DIR / "worst_answers.csv",
}

ENCODING = 'latin-1'
CSV_SEPARATOR = ';'
OUTPUT_ENCODING = 'utf-8'
OUTPUT_SEPARATOR = ','

CLUSTER_EPSILON_KM = 5
CLUSTER_MIN_SAMPLES = 10

SEGMENT_LENGTH_KM = 10

HIGH_RISK_PERCENTILE = 80
HOTSPOT_MIN_ACCIDENTS = 20

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'

BRAZILIAN_REGIONS = {
    'N': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
    'NE': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
    'SE': ['ES', 'MG', 'RJ', 'SP'],
    'S': ['PR', 'RS', 'SC'],
    'CO': ['DF', 'GO', 'MS', 'MT']
}

SEVERITY_COLORS = {
    'Com Vítimas Fatais': '#FF0000',
    'Com Vítimas Feridas': '#FFD700',
    'Sem Vítimas': '#D3D3D3'
}

TIME_PERIODS = {
    'madrugada': (0, 6),
    'manha': (6, 12),
    'tarde': (12, 18),
    'noite': (18, 24)
}

RUSH_HOURS = [(7, 9), (17, 19)]

HUMAN_CAUSES = [
    'Reação tardia', 'Ausência de reação', 'Condutor Dormindo',
    'Ingestão de álcool', 'Velocidade', 'Ultrapassagem',
    'Transitar na contramão', 'Acessar a via', 'Manobra',
    'Pedestre', 'distância', 'prefer'
]

MECHANICAL_CAUSES = [
    'falhas mecânicas', 'Falha', 'pneu', 'freio'
]

ENVIRONMENTAL_CAUSES = [
    'Chuva', 'Pista', 'Acumulo', 'água', 'neblina', 'nevoeiro',
    'vento', 'Curva acentuada', 'via'
]

LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
