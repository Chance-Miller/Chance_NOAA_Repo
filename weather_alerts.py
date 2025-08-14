import requests
import json
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import re

def fetch_weather_alerts():
    """
    Fetch weather alerts from NOAA Weather API
    """
    url = "https://api.weather.gov/alerts"
    
    try:
        print("Making API request to NOAA Weather Service...")
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        print(f"API Response: {len(data.get('features', []))} alerts found")
        
        # Extract alerts from the features array
        alerts = []
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
            alerts.append(alert)
        
        return alerts
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def save_to_sqlite(alerts, db_path='weather_alerts.db'):
    """
    Save alerts to SQLite database
    """
    conn = sqlite3.connect(db_path)
    
    # Drop existing table and recreate with correct schema
    conn.execute("DROP TABLE IF EXISTS weather_alerts")
    
    # Create table with enhanced schema
    create_table_sql = """
    CREATE TABLE weather_alerts (
        id TEXT PRIMARY KEY,
        area_desc TEXT,
        event TEXT,
        severity TEXT,
        certainty TEXT,
        urgency TEXT,
        headline TEXT,
        description TEXT,
        instruction TEXT,
        sent TEXT,
        effective TEXT,
        expires TEXT,
        status TEXT,
        message_type TEXT,
        sender_name TEXT,
        web TEXT,
        geometry_type TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    conn.execute(create_table_sql)
    
    # Insert alerts
    for alert in alerts:
        insert_sql = """
        INSERT INTO weather_alerts 
        (id, area_desc, event, severity, certainty, urgency, headline, 
         description, instruction, sent, effective, expires, status, 
         message_type, sender_name, web, geometry_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        conn.execute(insert_sql, (
            alert['id'], alert['area_desc'], alert['event'], 
            alert['severity'], alert['certainty'], alert['urgency'],
            alert['headline'], alert['description'], alert['instruction'],
            alert['sent'], alert['effective'], alert['expires'],
            alert['status'], alert['message_type'], alert['sender_name'],
            alert['web'], alert['geometry_type']
        ))
    
    conn.commit()
    conn.close()
    print(f"✅ Saved {len(alerts)} alerts to SQLite database")

def run_analytical_queries(db_path='weather_alerts.db'):
    """
    Run analytical queries as required by the exercise
    """
    conn = sqlite3.connect(db_path)
    
    print("\n" + "="*60)
    print("ANALYTICAL QUERIES RESULTS")
    print("="*60)
    
    # Query 1: Alerts by Event Type
    print("\n1. COUNT OF ALERTS BY EVENT TYPE:")
    print("-" * 40)
    query1 = """
    SELECT 
        event,
        COUNT(*) as alert_count
    FROM weather_alerts 
    WHERE event IS NOT NULL
    GROUP BY event
    ORDER BY alert_count DESC
    LIMIT 10
    """
    df1 = pd.read_sql_query(query1, conn)
    print(df1.to_string(index=False))
    
    # Query 2: Alerts by Severity
    print("\n2. ALERTS BY SEVERITY LEVEL:")
    print("-" * 40)
    query2 = """
    SELECT 
        severity,
        COUNT(*) as alert_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM weather_alerts), 2) as percentage
    FROM weather_alerts 
    WHERE severity IS NOT NULL
    GROUP BY severity
    ORDER BY alert_count DESC
    """
    df2 = pd.read_sql_query(query2, conn)
    print(df2.to_string(index=False))
    
    # Query 3: Texas Alerts (this week)
    print("\n3. ALERTS IN TEXAS:")
    print("-" * 40)
    query3 = """
    SELECT 
        COUNT(*) as texas_alerts,
        COUNT(CASE WHEN datetime(sent) > datetime('now', '-7 days') THEN 1 END) as texas_alerts_this_week
    FROM weather_alerts 
    WHERE area_desc LIKE '%TX%' OR area_desc LIKE '%Texas%'
    """
    df3 = pd.read_sql_query(query3, conn)
    print(df3.to_string(index=False))
    
    # Query 4: Time-based Analysis - Minutes until expiration
    print("\n4. TIME-BASED ANALYSIS - EXPIRATION STATUS:")
    print("-" * 40)
    query4 = """
    SELECT 
        CASE 
            WHEN expires IS NULL THEN 'No Expiration Set'
            WHEN datetime(expires) < datetime('now') THEN 'Expired'
            WHEN datetime(expires) > datetime('now') THEN 'Active'
            ELSE 'Unknown'
        END as expiration_status,
        COUNT(*) as count
    FROM weather_alerts
    GROUP BY expiration_status
    ORDER BY count DESC
    """
    df4 = pd.read_sql_query(query4, conn)
    print(df4.to_string(index=False))
    
    # Query 5: Top States/Areas by Active Alerts
    print("\n5. TOP AREAS BY NUMBER OF ACTIVE ALERTS:")
    print("-" * 40)
    query5 = """
    SELECT 
        area_desc,
        COUNT(*) as active_alerts
    FROM weather_alerts 
    WHERE (expires IS NULL OR datetime(expires) > datetime('now'))
        AND status = 'Actual'
        AND area_desc IS NOT NULL
    GROUP BY area_desc
    ORDER BY active_alerts DESC
    LIMIT 10
    """
    df5 = pd.read_sql_query(query5, conn)
    print(df5.to_string(index=False))
    
    # Query 6: Operational KPI - Urgent Alerts Requiring Immediate Action
    print("\n6. OPERATIONAL KPI - URGENT ALERTS NEEDING IMMEDIATE ACTION:")
    print("-" * 40)
    query6 = """
    SELECT 
        event,
        area_desc,
        severity,
        urgency,
        CASE 
            WHEN expires IS NOT NULL THEN 
                ROUND((julianday(expires) - julianday('now')) * 24 * 60, 0)
            ELSE NULL
        END as minutes_until_expiration
    FROM weather_alerts 
    WHERE urgency = 'Immediate' 
        AND severity IN ('Severe', 'Extreme')
        AND (expires IS NULL OR datetime(expires) > datetime('now'))
    ORDER BY minutes_until_expiration ASC
    LIMIT 10
    """
    df6 = pd.read_sql_query(query6, conn)
    if not df6.empty:
        print(df6.to_string(index=False))
    else:
        print("No immediate urgent alerts found")
    
    # Summary Statistics
    print("\n7. SUMMARY STATISTICS:")
    print("-" * 40)
    query7 = """
    SELECT 
        COUNT(*) as total_alerts,
        COUNT(CASE WHEN status = 'Actual' THEN 1 END) as actual_alerts,
        COUNT(CASE WHEN status = 'Test' THEN 1 END) as test_alerts,
        COUNT(CASE WHEN urgency = 'Immediate' THEN 1 END) as immediate_alerts,
        COUNT(CASE WHEN severity = 'Severe' THEN 1 END) as severe_alerts,
        COUNT(CASE WHEN severity = 'Extreme' THEN 1 END) as extreme_alerts
    FROM weather_alerts
    """
    df7 = pd.read_sql_query(query7, conn)
    print(df7.to_string(index=False))
    
    conn.close()
    
    return {
        'event_counts': df1,
        'severity_breakdown': df2,
        'texas_alerts': df3,
        'expiration_status': df4,
        'top_areas': df5,
        'urgent_alerts': df6,
        'summary_stats': df7
    }

def main():
    """
    Main function - Complete weather alerts analysis pipeline
    """
    print("🌦️  NOAA Weather Alerts Analysis")
    print("="*50)
    
    # Step 1: Fetch data from API
    alerts = fetch_weather_alerts()
    
    if not alerts:
        print("❌ No alerts fetched. Exiting.")
        return
    
    # Step 2: Store data locally in SQLite
    save_to_sqlite(alerts)
    
    # Step 3: Run analytical queries
    results = run_analytical_queries()
    
    print(f"\n✅ Analysis complete! Database saved as 'weather_alerts.db'")
    print(f"📊 Total alerts processed: {len(alerts)}")
    print(f"🔍 You can explore the data further using:")
    print(f"   - DB Browser for SQLite (GUI)")
    print(f"   - sqlite3 weather_alerts.db (command line)")
    print(f"   - The SQL queries in NOAA.sql file")

if __name__ == "__main__":
    main()
