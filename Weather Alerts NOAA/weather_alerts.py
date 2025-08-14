import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def fetch_weather_alerts():
    """
    Fetch weather alerts from NOAA Weather API and return as DataFrame
    """
    url = "https://api.weather.gov/alerts"
    
    try:
        print("Making API request to NOAA Weather Service...")
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        print(f"API Response: {len(data.get('features', []))} alerts found")
        
        # Extract alerts from the features array
        alerts_data = []
        for feature in data.get('features', []):
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            alert = {
                'id': properties.get('id'),
                'area_desc': properties.get('areaDesc'),
                'event': properties.get('event'),
                'severity': properties.get('severity'),
                'certainty': properties.get('certainty'),
                'urgency': properties.get('urgency'),
                'headline': properties.get('headline'),
                'description': properties.get('description'),
                'instruction': properties.get('instruction'),
                'sent': properties.get('sent'),
                'effective': properties.get('effective'),
                'expires': properties.get('expires'),
                'status': properties.get('status'),
                'message_type': properties.get('messageType'),
                'sender_name': properties.get('senderName'),
                'web': properties.get('web'),
                'geometry_type': geometry.get('type') if geometry else None
            }
            alerts_data.append(alert)
        
        # Create DataFrame
        df = pd.DataFrame(alerts_data)
        
        # Convert datetime columns with UTC timezone handling
        datetime_columns = ['sent', 'effective', 'expires']
        for col in datetime_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
        
        print(f"✅ Created DataFrame with {len(df)} alerts")
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def save_dataframe(df, file_formats=['csv', 'parquet']):
    """
    Save DataFrame to multiple formats for analysis
    """
    if df.empty:
        print("❌ No data to save")
        return
    
    for fmt in file_formats:
        if fmt == 'csv':
            df.to_csv('weather_alerts.csv', index=False)
            print(f"✅ Saved to weather_alerts.csv")
        elif fmt == 'parquet':
            df.to_parquet('weather_alerts.parquet', index=False)
            print(f"✅ Saved to weather_alerts.parquet")
        elif fmt == 'json':
            df.to_json('weather_alerts.json', orient='records', indent=2)
            print(f"✅ Saved to weather_alerts.json")

def run_analytical_queries(df):
    """
    Run analytical queries using pandas DataFrame operations
    """
    if df.empty:
        print("❌ No data to analyze")
        return {}
    
    print("\n" + "="*60)
    print("ANALYTICAL QUERIES RESULTS")
    print("="*60)
    
    # Query 1: Alerts by Event Type
    print("\n1. COUNT OF ALERTS BY EVENT TYPE:")
    print("-" * 40)
    event_counts = df['event'].value_counts().head(10)
    print(event_counts.to_string())
    
    # Query 2: Alerts by Severity
    print("\n2. ALERTS BY SEVERITY LEVEL:")
    print("-" * 40)
    severity_counts = df['severity'].value_counts()
    severity_pct = (df['severity'].value_counts(normalize=True) * 100).round(2)
    severity_analysis = pd.DataFrame({
        'alert_count': severity_counts,
        'percentage': severity_pct
    }).fillna(0)
    print(severity_analysis.to_string())
    
    # Query 3: Texas Alerts
    print("\n3. ALERTS IN TEXAS:")
    print("-" * 40)
    texas_mask = df['area_desc'].str.contains('TX|Texas', case=False, na=False)
    texas_alerts = df[texas_mask]
    
    # This week filter (make timezone-aware)
    one_week_ago = pd.Timestamp.now(tz='UTC') - timedelta(days=7)
    texas_this_week = texas_alerts[texas_alerts['sent'] > one_week_ago]
    
    texas_summary = pd.DataFrame({
        'metric': ['total_texas_alerts', 'texas_alerts_this_week'],
        'count': [len(texas_alerts), len(texas_this_week)]
    })
    print(texas_summary.to_string(index=False))
    
    # Query 4: Time-based Analysis - Expiration Status
    print("\n4. TIME-BASED ANALYSIS - EXPIRATION STATUS:")
    print("-" * 40)
    now = pd.Timestamp.now(tz='UTC')
    
    def get_expiration_status(row):
        if pd.isna(row['expires']):
            return 'No Expiration Set'
        elif row['expires'] < now:
            return 'Expired'
        elif row['expires'] > now:
            return 'Active'
        else:
            return 'Unknown'
    
    df['expiration_status'] = df.apply(get_expiration_status, axis=1)
    expiration_counts = df['expiration_status'].value_counts()
    print(expiration_counts.to_string())
    
    # Query 5: Top Areas by Active Alerts
    print("\n5. TOP AREAS BY NUMBER OF ACTIVE ALERTS:")
    print("-" * 40)
    active_mask = (
        (df['expiration_status'] == 'Active') | 
        (df['expiration_status'] == 'No Expiration Set')
    ) & (df['status'] == 'Actual')
    
    active_alerts = df[active_mask]
    top_areas = active_alerts['area_desc'].value_counts().head(10)
    print(top_areas.to_string())
    
    # Query 6: Operational KPI - Urgent Alerts
    print("\n6. OPERATIONAL KPI - URGENT ALERTS NEEDING IMMEDIATE ACTION:")
    print("-" * 40)
    urgent_mask = (
        (df['urgency'] == 'Immediate') & 
        (df['severity'].isin(['Severe', 'Extreme'])) &
        (df['expiration_status'].isin(['Active', 'No Expiration Set']))
    )
    
    urgent_alerts = df[urgent_mask][['event', 'area_desc', 'severity', 'urgency', 'expires']].copy()
    
    if not urgent_alerts.empty:
        # Calculate minutes until expiration
        urgent_alerts['minutes_until_expiration'] = (
            (urgent_alerts['expires'] - pd.Timestamp.now(tz='UTC')).dt.total_seconds() / 60
        ).round(0)
        urgent_alerts = urgent_alerts.sort_values('minutes_until_expiration')
        print(urgent_alerts.head(10).to_string(index=False))
    else:
        print("No immediate urgent alerts found")
    
    # Query 7: Summary Statistics
    print("\n7. SUMMARY STATISTICS:")
    print("-" * 40)
    summary_stats = pd.DataFrame({
        'metric': [
            'total_alerts',
            'actual_alerts', 
            'test_alerts',
            'immediate_alerts',
            'severe_alerts',
            'extreme_alerts'
        ],
        'count': [
            len(df),
            len(df[df['status'] == 'Actual']),
            len(df[df['status'] == 'Test']),
            len(df[df['urgency'] == 'Immediate']),
            len(df[df['severity'] == 'Severe']),
            len(df[df['severity'] == 'Extreme'])
        ]
    })
    print(summary_stats.to_string(index=False))
    
    # Additional DataFrame-specific analysis
    print("\n8. DATA QUALITY ANALYSIS:")
    print("-" * 40)
    data_quality = pd.DataFrame({
        'column': df.columns,
        'non_null_count': df.count(),
        'null_count': df.isnull().sum(),
        'null_percentage': (df.isnull().sum() / len(df) * 100).round(2)
    })
    print(data_quality.to_string(index=False))
    
    return {
        'dataframe': df,
        'event_counts': event_counts,
        'severity_analysis': severity_analysis,
        'texas_summary': texas_summary,
        'expiration_counts': expiration_counts,
        'top_areas': top_areas,
        'urgent_alerts': urgent_alerts,
        'summary_stats': summary_stats,
        'data_quality': data_quality
    }

def export_analysis_results(results):
    """
    Export analysis results to both CSV and Parquet files
    """
    if not results:
        return
    
    # Define the analysis results to export
    exports = {
        'analysis_event_counts': results['event_counts'],
        'analysis_severity_breakdown': results['severity_analysis'],
        'analysis_summary_stats': results['summary_stats'],
        'analysis_data_quality': results['data_quality']
    }
    
    # Export each result to both CSV and Parquet
    for filename, data in exports.items():
        if isinstance(data, pd.Series):
            # Convert Series to DataFrame for better export
            df = data.to_frame().reset_index()
        else:
            df = data.reset_index() if hasattr(data, 'reset_index') else data
        
        # Export to CSV
        df.to_csv(f'{filename}.csv', index=False)
        
        # Export to Parquet
        df.to_parquet(f'{filename}.parquet', index=False)
    
    # Also export the top areas and urgent alerts if they exist
    if 'top_areas' in results and not results['top_areas'].empty:
        top_areas_df = results['top_areas'].to_frame().reset_index()
        top_areas_df.to_csv('analysis_top_areas.csv', index=False)
        top_areas_df.to_parquet('analysis_top_areas.parquet', index=False)
    
    if 'urgent_alerts' in results and not results['urgent_alerts'].empty:
        results['urgent_alerts'].to_csv('analysis_urgent_alerts.csv', index=False)
        results['urgent_alerts'].to_parquet('analysis_urgent_alerts.parquet', index=False)
    
    print(f"\n✅ Analysis results exported to both CSV and Parquet files")

def main():
    """
    Main function - Complete weather alerts analysis pipeline using DataFrames
    """
    print("🌦️  NOAA Weather Alerts Analysis (DataFrame Version)")
    print("="*55)
    
    # Step 1: Fetch data from API and create DataFrame
    df = fetch_weather_alerts()
    
    if df.empty:
        print("❌ No alerts fetched. Exiting.")
        return
    
    # Step 2: Save DataFrame to multiple formats
    save_dataframe(df, ['csv', 'parquet'])
    
    # Step 3: Run analytical queries using pandas operations
    results = run_analytical_queries(df)
    
    # Step 4: Export analysis results
    export_analysis_results(results)
    
    print(f"\n✅ Analysis complete!")
    print(f"📊 Total alerts processed: {len(df)}")
    print(f"💾 Data saved in dual formats:")
    print(f"   - weather_alerts.csv/.parquet (main dataset)")
    print(f"   - analysis_*.csv/.parquet (individual analysis results)")
    print(f"🔍 You can explore the data using:")
    print(f"   - CSV files: Excel, Google Sheets, any spreadsheet app")
    print(f"   - Parquet files: pandas (faster loading)")
    print(f"   - Python: pd.read_csv() or pd.read_parquet()")
    print(f"   - Jupyter notebooks for interactive analysis")

if __name__ == "__main__":
    main()
