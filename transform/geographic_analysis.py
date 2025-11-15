import pandas as pd
import numpy as np
import logging
from sklearn.cluster import DBSCAN
from utils import config
from utils.helpers import save_dataframe, calculate_distance_km

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def analyze_geography(df: pd.DataFrame) -> tuple:
    logger.info("="*80)
    logger.info("GEOGRAPHIC ANALYSIS PHASE - Creating clusters and segments")
    logger.info("="*80)
    
    df = df.copy()
    
    logger.info("\n1. Creating geographic clusters...")
    df, clusters_df = create_geographic_clusters(df)
    
    logger.info("\n2. Generating highway segments...")
    segments_df = create_highway_segments(df)
    
    logger.info("\n✓ Geographic analysis complete")
    
    return df, clusters_df, segments_df


def create_geographic_clusters(df: pd.DataFrame) -> tuple:
    valid_coords = df[
        (df['latitude'].notna()) &
        (df['longitude'].notna()) &
        (df['latitude'].between(-35, 5)) &
        (df['longitude'].between(-75, -30))
    ].copy()
    
    logger.info(f"   Processing {len(valid_coords):,} accidents with valid coordinates...")
    
    if len(valid_coords) < config.CLUSTER_MIN_SAMPLES:
        logger.warning("   Not enough valid coordinates for clustering")
        df['cluster_id'] = -1
        df['is_hotspot'] = 0
        return df, pd.DataFrame()
    
    coords = np.radians(valid_coords[['latitude', 'longitude']].values)
    
    epsilon_rad = config.CLUSTER_EPSILON_KM / 6371.0
    
    logger.info(f"   Running DBSCAN (eps={config.CLUSTER_EPSILON_KM}km, min_samples={config.CLUSTER_MIN_SAMPLES})...")
    
    clustering = DBSCAN(
        eps=epsilon_rad,
        min_samples=config.CLUSTER_MIN_SAMPLES,
        metric='haversine'
    ).fit(coords)
    
    valid_coords['cluster_id'] = clustering.labels_
    
    df['cluster_id'] = -1
    df.loc[valid_coords.index, 'cluster_id'] = valid_coords['cluster_id']
    
    df['is_hotspot'] = (df['cluster_id'] >= 0).astype(int)
    
    clusters = valid_coords[valid_coords['cluster_id'] >= 0].groupby('cluster_id').agg({
        'id': 'count',
        'latitude': 'mean',
        'longitude': 'mean',
        'mortos': 'sum',
        'feridos': 'sum',
        'severity_score': 'mean',
        'hour': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.mean(),
        'day_of_week': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.mean(),
        'causa_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários'
    }).reset_index()
    
    clusters.columns = [
        'cluster_id', 'accident_count', 'center_latitude', 'center_longitude',
        'deaths', 'injuries', 'avg_severity', 'predominant_hour',
        'predominant_day', 'predominant_cause'
    ]
    
    def calculate_cluster_radius(cluster_id):
        cluster_points = valid_coords[valid_coords['cluster_id'] == cluster_id]
        if len(cluster_points) == 0:
            return 0
        
        center = clusters[clusters['cluster_id'] == cluster_id][['center_latitude', 'center_longitude']].values[0]
        
        distances = []
        for idx, row in cluster_points.iterrows():
            dist = calculate_distance_km(
                center[0], center[1],
                row['latitude'], row['longitude']
            )
            distances.append(dist)
        
        return max(distances) if distances else 0
    
    if len(clusters) > 0:
        logger.info(f"   Calculating cluster radii...")
        clusters['radius_km'] = clusters['cluster_id'].apply(calculate_cluster_radius)
        
        clusters['density_score'] = clusters['accident_count'] / (clusters['radius_km']**2 + 1)
        
        clusters['risk_category'] = pd.cut(
            clusters['avg_severity'],
            bins=[0, 20, 40, 60, 100],
            labels=['baixo', 'medio', 'alto', 'extremo']
        )
        
        clusters['heat_intensity'] = clusters['density_score'] / clusters['density_score'].max()
        
        logger.info(f"   ✓ Created {len(clusters)} accident clusters")
        logger.info(f"   ✓ Identified {df['is_hotspot'].sum():,} accidents in hotspots")
    else:
        logger.info("   ✓ No clusters formed")
    
    return df, clusters


def create_highway_segments(df: pd.DataFrame) -> pd.DataFrame:
    if 'br' not in df.columns or 'km' not in df.columns:
        logger.warning("   Missing highway or km data")
        return pd.DataFrame()
    
    df['km_segment_start'] = (df['km'] // config.SEGMENT_LENGTH_KM) * config.SEGMENT_LENGTH_KM
    df['km_segment_end'] = df['km_segment_start'] + config.SEGMENT_LENGTH_KM
    df['segment_id'] = 'BR' + df['br'].astype(str) + '_km' + df['km_segment_start'].astype(int).astype(str)
    
    segments = df.groupby(['br', 'km_segment_start', 'km_segment_end', 'segment_id']).agg({
        'id': 'count',
        'uf': lambda x: x.mode()[0] if len(x.mode()) > 0 else x.iloc[0],
        'municipio': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
        'mortos': 'sum',
        'feridos': 'sum',
        'latitude': 'mean',
        'longitude': 'mean',
        'severity_score': 'mean',
        'causa_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
        'tipo_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
        'condicao_metereologica': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
        'hour': 'mean'
    }).reset_index()
    
    segments.columns = [
        'highway', 'km_start', 'km_end', 'segment_id', 'accident_count',
        'state', 'primary_city', 'deaths', 'injuries', 'center_latitude',
        'center_longitude', 'avg_severity', 'top_cause', 'top_accident_type',
        'top_weather', 'avg_hour'
    ]
    
    segments['accidents_per_km'] = segments['accident_count'] / config.SEGMENT_LENGTH_KM
    segments['deaths_per_km'] = segments['deaths'] / config.SEGMENT_LENGTH_KM
    
    avg_accidents_per_km = segments['accidents_per_km'].mean()
    avg_severity = segments['avg_severity'].mean()
    
    segments['risk_score'] = (
        (segments['accidents_per_km'] / avg_accidents_per_km) * 50 +
        (segments['avg_severity'] / avg_severity) * 50
    )
    
    segments['danger_rank'] = segments['risk_score'].rank(ascending=False)
    
    segments['risk_category'] = pd.cut(
        segments['risk_score'],
        bins=[0, 40, 60, 80, 150],
        labels=['baixo', 'medio', 'alto', 'extremo']
    )
    
    segments['segment_label'] = (
        'BR-' + segments['highway'].astype(str) +
        ' km ' + segments['km_start'].astype(int).astype(str) +
        '-' + segments['km_end'].astype(int).astype(str) +
        ' (' + segments['state'] + ')'
    )
    
    logger.info(f"   ✓ Created {len(segments):,} highway segments")
    
    if len(segments) > 0:
        top_segments = segments.nlargest(10, 'risk_score')
        logger.info(f"\n   Top 10 most dangerous segments:")
        for idx, row in top_segments.iterrows():
            logger.info(f"      {row['segment_label']}: {row['accident_count']} accidents, risk={row['risk_score']:.1f}")
    
    return segments


if __name__ == "__main__":
    from extract.extract_data import extract_data
    from transform.clean_data import clean_data
    from transform.enrich_data import enrich_data
    from transform.calculate_risks import calculate_risks
    
    df_raw = extract_data()
    df_clean = clean_data(df_raw)
    df_enriched = enrich_data(df_clean)
    df_risks = calculate_risks(df_enriched)
    df_geo, clusters, segments = analyze_geography(df_risks)
    
    print(f"\n✓ Geographic analysis complete")
    print(f"✓ Clusters: {len(clusters)}")
    print(f"✓ Segments: {len(segments)}")
    print(f"✓ Hotspot accidents: {df_geo['is_hotspot'].sum():,}")
