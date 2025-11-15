import pandas as pd
import numpy as np
import logging
from utils import config
from utils.helpers import save_dataframe

logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
logger = logging.getLogger(__name__)


def aggregate_data(df: pd.DataFrame, clusters: pd.DataFrame, segments: pd.DataFrame) -> dict:
    logger.info("="*80)
    logger.info("AGGREGATION PHASE - Creating summary views")
    logger.info("="*80)
    
    aggregated = {}
    
    logger.info("\n1. Aggregating risk by time...")
    aggregated['risk_time'] = aggregate_risk_by_time(df)
    
    logger.info("\n2. Aggregating risk by location...")
    aggregated['risk_location'] = aggregate_risk_by_location(df)
    
    logger.info("\n3. Creating danger rankings...")
    aggregated['rankings'] = create_danger_rankings(df)
    
    logger.info("\n4. Identifying worst scenarios...")
    aggregated['scenarios'] = create_worst_scenarios(df)
    
    logger.info("\n5. Creating daily risk calendar...")
    aggregated['daily'] = create_daily_calendar(df)
    
    logger.info("\n6. Generating worst answers...")
    aggregated['answers'] = generate_worst_answers(df)
    
    logger.info("\n7. Preparing map visualization data...")
    aggregated['map_points'] = prepare_map_points(df)
    aggregated['heatmap'] = clusters
    aggregated['segments'] = segments
    
    logger.info("\n✓ Aggregation complete - created 9 output files")
    
    return aggregated


def aggregate_risk_by_time(df: pd.DataFrame) -> pd.DataFrame:
    time_dims = []
    
    if 'hour' in df.columns:
        hour_agg = df.groupby('hour').agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean'
        }).reset_index()
        hour_agg['time_dimension'] = 'hour'
        hour_agg['time_value'] = hour_agg['hour'].astype(str) + 'h'
        time_dims.append(hour_agg.drop('hour', axis=1))
    
    if 'day_of_week' in df.columns and 'day_of_week_name_pt' in df.columns:
        dow_agg = df.groupby(['day_of_week', 'day_of_week_name_pt']).agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean'
        }).reset_index()
        dow_agg['time_dimension'] = 'day_of_week'
        dow_agg['time_value'] = dow_agg['day_of_week_name_pt']
        time_dims.append(dow_agg.drop(['day_of_week', 'day_of_week_name_pt'], axis=1))
    
    if 'day_of_month' in df.columns:
        dom_agg = df.groupby('day_of_month').agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean'
        }).reset_index()
        dom_agg['time_dimension'] = 'day_of_month'
        dom_agg['time_value'] = 'Dia ' + dom_agg['day_of_month'].astype(str)
        time_dims.append(dom_agg.drop('day_of_month', axis=1))
    
    if 'month' in df.columns and 'month_name' in df.columns:
        month_agg = df.groupby(['month', 'month_name']).agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean'
        }).reset_index()
        month_agg['time_dimension'] = 'month'
        month_agg['time_value'] = month_agg['month_name']
        time_dims.append(month_agg.drop(['month', 'month_name'], axis=1))
    
    if time_dims:
        result = pd.concat(time_dims, ignore_index=True)
        result.columns = [
            'accident_count', 'deaths', 'injuries', 'avg_severity',
            'avg_risk_score', 'time_dimension', 'time_value'
        ]
        
        result['fatality_rate'] = (result['deaths'] / result['accident_count']) * 100
        result['injury_rate'] = (result['injuries'] / result['accident_count']) * 100
        
        result['risk_rank'] = result.groupby('time_dimension')['avg_risk_score'].rank(ascending=False)
        
        result['is_worst'] = result.groupby('time_dimension')['avg_risk_score'].transform(
            lambda x: x >= x.quantile(0.9)
        ).astype(int)
        result['is_best'] = result.groupby('time_dimension')['avg_risk_score'].transform(
            lambda x: x <= x.quantile(0.1)
        ).astype(int)
        
        logger.info(f"   ✓ Created {len(result)} time-based risk aggregations")
        return result
    
    return pd.DataFrame()


def aggregate_risk_by_location(df: pd.DataFrame) -> pd.DataFrame:
    loc_dims = []
    
    if 'uf' in df.columns:
        state_agg = df.groupby('uf').agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean',
            'causa_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
            'tipo_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários'
        }).reset_index()
        state_agg['location_type'] = 'state'
        state_agg['location_name'] = state_agg['uf']
        loc_dims.append(state_agg.drop('uf', axis=1))
    
    if 'br' in df.columns:
        highway_agg = df.groupby('br').agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean',
            'causa_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
            'tipo_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
            'km': 'nunique'
        }).reset_index()
        highway_agg['location_type'] = 'highway'
        highway_agg['location_name'] = 'BR-' + highway_agg['br'].astype(str)
        highway_agg['accidents_per_100km'] = (highway_agg['id'] / highway_agg['km']) * 100
        loc_dims.append(highway_agg.drop(['br', 'km'], axis=1))
    
    if 'municipio' in df.columns:
        city_agg = df.groupby('municipio').agg({
            'id': 'count',
            'mortos': 'sum',
            'feridos': 'sum',
            'severity_score': 'mean',
            'composite_risk_score': 'mean',
            'causa_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários',
            'tipo_acidente': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Vários'
        }).reset_index().nlargest(50, 'id')
        city_agg['location_type'] = 'city'
        city_agg['location_name'] = city_agg['municipio']
        loc_dims.append(city_agg.drop('municipio', axis=1))
    
    if loc_dims:
        result = pd.concat(loc_dims, ignore_index=True)
        
        col_names = ['accident_count', 'deaths', 'injuries', 'avg_severity',
                     'avg_risk_score', 'top_cause', 'top_accident_type']
        
        if result.shape[1] > len(col_names) + 2:
            col_names.append('accidents_per_100km')
        
        col_names.extend(['location_type', 'location_name'])
        result.columns = col_names
        
        if 'accidents_per_100km' not in result.columns:
            result['accidents_per_100km'] = np.nan
        
        result['fatality_rate'] = (result['deaths'] / result['accident_count']) * 100
        
        result['risk_rank'] = result.groupby('location_type')['avg_risk_score'].rank(ascending=False)
        
        logger.info(f"   ✓ Created {len(result)} location-based risk aggregations")
        return result
    
    return pd.DataFrame()


def create_danger_rankings(df: pd.DataFrame) -> pd.DataFrame:
    rankings = []
    
    if 'hour' in df.columns:
        hour_rank = df.groupby('hour').agg({
            'id': 'count',
            'mortos': 'sum',
            'composite_risk_score': 'mean'
        }).reset_index().nlargest(10, 'id')
        hour_rank['category'] = 'worst_hours'
        hour_rank['item_name'] = hour_rank['hour'].astype(str) + 'h'
        rankings.append(hour_rank.drop('hour', axis=1))
    
    if 'day_of_week_name_pt' in df.columns:
        day_rank = df.groupby('day_of_week_name_pt').agg({
            'id': 'count',
            'mortos': 'sum',
            'composite_risk_score': 'mean'
        }).reset_index().nlargest(7, 'id')
        day_rank['category'] = 'worst_days'
        day_rank['item_name'] = day_rank['day_of_week_name_pt']
        rankings.append(day_rank.drop('day_of_week_name_pt', axis=1))
    
    if 'uf' in df.columns:
        state_rank = df.groupby('uf').agg({
            'id': 'count',
            'mortos': 'sum',
            'composite_risk_score': 'mean'
        }).reset_index().nlargest(10, 'id')
        state_rank['category'] = 'worst_states'
        state_rank['item_name'] = state_rank['uf']
        rankings.append(state_rank.drop('uf', axis=1))
    
    if 'br' in df.columns:
        highway_rank = df.groupby('br').agg({
            'id': 'count',
            'mortos': 'sum',
            'composite_risk_score': 'mean'
        }).reset_index().nlargest(10, 'id')
        highway_rank['category'] = 'worst_highways'
        highway_rank['item_name'] = 'BR-' + highway_rank['br'].astype(str)
        rankings.append(highway_rank.drop('br', axis=1))
    
    if rankings:
        result = pd.concat(rankings, ignore_index=True)
        result.columns = ['accident_count', 'deaths', 'risk_score', 'category', 'item_name']
        result['rank'] = result.groupby('category')['accident_count'].rank(ascending=False)
        
        avg_accidents = df['id'].count() / len(result['category'].unique())
        result['vs_average_pct'] = ((result['accident_count'] - avg_accidents) / avg_accidents) * 100
        
        logger.info(f"   ✓ Created {len(result)} danger rankings")
        return result
    
    return pd.DataFrame()


def create_worst_scenarios(df: pd.DataFrame) -> pd.DataFrame:
    scenarios = []
    
    if all(col in df.columns for col in ['is_weekend', 'is_night', 'br']):
        weekend_night_highway = df[
            (df['is_weekend'] == 1) & (df['is_night'] == 1)
        ].groupby('br').agg({
            'id': 'count',
            'mortos': 'sum',
            'composite_risk_score': 'mean'
        }).reset_index().nlargest(5, 'id')
        
        weekend_night_highway['scenario'] = 'Fim de semana à noite na BR-' + weekend_night_highway['br'].astype(str)
        scenarios.append(weekend_night_highway.drop('br', axis=1))
    
    if all(col in df.columns for col in ['alcohol_involved', 'is_night']):
        alcohol_night = df[
            (df['alcohol_involved'] == 1) & (df['is_night'] == 1)
        ].agg({
            'id': 'count',
            'mortos': 'sum',
            'composite_risk_score': 'mean'
        })
        
        scenarios.append(pd.DataFrame({
            'id': [alcohol_night['id']],
            'mortos': [alcohol_night['mortos']],
            'composite_risk_score': [alcohol_night['composite_risk_score']],
            'scenario': ['Álcool envolvido durante a noite']
        }))
    
    if scenarios:
        result = pd.concat(scenarios, ignore_index=True)
        result.columns = ['accident_count', 'deaths', 'risk_score', 'scenario_description']
        
        avg_risk = df['composite_risk_score'].mean()
        result['risk_multiplier'] = result['risk_score'] / avg_risk
        
        logger.info(f"   ✓ Created {len(result)} worst scenarios")
        return result
    
    return pd.DataFrame()


def create_daily_calendar(df: pd.DataFrame) -> pd.DataFrame:
    if 'date' not in df.columns:
        return pd.DataFrame()
    
    daily = df.groupby('date').agg({
        'id': 'count',
        'mortos': 'sum',
        'feridos': 'sum',
        'composite_risk_score': 'mean',
        'day_of_week_name_pt': 'first',
        'day_of_month': 'first',
        'month': 'first',
        'year': 'first'
    }).reset_index()
    
    daily.columns = [
        'date', 'accident_count', 'deaths', 'injuries', 'risk_score',
        'day_of_week', 'day_of_month', 'month', 'year'
    ]
    
    daily['risk_category'] = pd.cut(
        daily['risk_score'],
        bins=[0, 40, 60, 80, 150],
        labels=['baixo', 'medio', 'alto', 'extremo']
    )
    
    logger.info(f"   ✓ Created daily calendar with {len(daily)} days")
    
    return daily


def generate_worst_answers(df: pd.DataFrame) -> pd.DataFrame:
    answers = []
    
    if 'hour' in df.columns:
        worst_hour = df.groupby('hour').size().idxmax()
        count = df[df['hour'] == worst_hour].shape[0]
        answers.append({
            'question_id': 1,
            'question': 'Qual o pior horário para dirigir?',
            'answer': f'{worst_hour}h',
            'metric_value': f'{count:,} acidentes',
            'explanation': 'Horário de pico com maior volume de acidentes'
        })
    
    if 'day_of_week_name_pt' in df.columns:
        worst_day = df.groupby('day_of_week_name_pt').size().idxmax()
        count = df[df['day_of_week_name_pt'] == worst_day].shape[0]
        answers.append({
            'question_id': 2,
            'question': 'Qual o pior dia da semana?',
            'answer': worst_day,
            'metric_value': f'{count:,} acidentes',
            'explanation': 'Dia com maior volume de acidentes'
        })
    
    if 'uf' in df.columns:
        worst_state = df.groupby('uf').size().idxmax()
        count = df[df['uf'] == worst_state].shape[0]
        answers.append({
            'question_id': 3,
            'question': 'Qual o estado mais perigoso?',
            'answer': worst_state,
            'metric_value': f'{count:,} acidentes',
            'explanation': 'Estado com maior volume de acidentes'
        })
    
    if 'br' in df.columns:
        worst_highway = df.groupby('br').size().idxmax()
        count = df[df['br'] == worst_highway].shape[0]
        answers.append({
            'question_id': 4,
            'question': 'Qual a rodovia mais perigosa?',
            'answer': f'BR-{worst_highway}',
            'metric_value': f'{count:,} acidentes',
            'explanation': 'Rodovia com maior volume de acidentes'
        })
    
    result = pd.DataFrame(answers)
    logger.info(f"   ✓ Generated {len(result)} direct answers")
    
    return result


def prepare_map_points(df: pd.DataFrame) -> pd.DataFrame:
    map_cols = [
        'id', 'date', 'horario', 'hour', 'latitude', 'longitude',
        'uf', 'municipio', 'br', 'km', 'mortos', 'feridos',
        'classificacao_acidente', 'tipo_acidente', 'causa_acidente',
        'marker_color', 'marker_size', 'marker_opacity',
        'tooltip_text', 'popup_html'
    ]
    
    available_cols = [col for col in map_cols if col in df.columns]
    map_df = df[available_cols].copy()
    
    map_df = map_df[
        (map_df['latitude'].notna()) &
        (map_df['longitude'].notna()) &
        (map_df['latitude'].between(-35, 5)) &
        (map_df['longitude'].between(-75, -30))
    ]
    
    logger.info(f"   ✓ Prepared {len(map_df):,} map points with valid coordinates")
    
    return map_df


if __name__ == "__main__":
    from extract.extract_data import extract_data
    from transform.clean_data import clean_data
    from transform.enrich_data import enrich_data
    from transform.calculate_risks import calculate_risks
    from transform.geographic_analysis import analyze_geography
    
    df_raw = extract_data()
    df_clean = clean_data(df_raw)
    df_enriched = enrich_data(df_clean)
    df_risks = calculate_risks(df_enriched)
    df_geo, clusters, segments = analyze_geography(df_risks)
    
    aggregated = aggregate_data(df_geo, clusters, segments)
    
    print(f"\n✓ Aggregation complete")
    print(f"✓ Created {len(aggregated)} output files")
    for key in aggregated.keys():
        print(f"   - {key}: {len(aggregated[key])} rows")
