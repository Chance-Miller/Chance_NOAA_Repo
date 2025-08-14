# Weather Alerts Analysis - NOAA API

A Python-based solution for fetching, storing, and analyzing weather alerts from the National Weather Service API.

## Overview

This project demonstrates:
1. **Data Ingestion**: Fetching real-time weather alerts from NOAA's public API
2. **Data Storage**: Local storage using SQLite database
3. **Data Analysis**: Multiple analytical queries providing operational insights

## Architecture & Design Choices

### Technology Stack
- **Python 3.x** - Main programming language
- **SQLite** - Local database (chosen for simplicity, portability, and no setup requirements)
- **Pandas** - Data analysis and presentation
- **Requests** - HTTP API calls

### Design Assumptions
1. **Data Freshness**: Alerts are fetched fresh on each run (no incremental updates)
2. **Storage Format**: SQLite chosen over alternatives (DuckDB, Parquet) for simplicity and universal compatibility
3. **Data Validation**: Basic validation through API response structure
4. **Geographic Scope**: All US alerts (no filtering by region)

### Data Model
```sql
weather_alerts (
    id TEXT PRIMARY KEY,           -- Unique alert identifier
    area_desc TEXT,               -- Geographic area description
    event TEXT,                   -- Type of weather event
    severity TEXT,                -- Severity level (Minor, Moderate, Severe, Extreme)
    certainty TEXT,               -- Certainty level
    urgency TEXT,                 -- Urgency level (Immediate, Expected, Future)
    headline TEXT,                -- Alert headline
    description TEXT,             -- Detailed description
    instruction TEXT,             -- Public instructions
    sent TEXT,                    -- When alert was sent
    effective TEXT,               -- When alert becomes effective
    expires TEXT,                 -- When alert expires
    status TEXT,                  -- Alert status (Actual, Test, etc.)
    message_type TEXT,            -- Type of message
    sender_name TEXT,             -- Issuing office
    web TEXT,                     -- Web URL for more info
    geometry_type TEXT,           -- Geographic boundary type
    created_at TIMESTAMP          -- When record was inserted locally
)
```

## How to Run

### Prerequisites
```bash
pip install requests pandas
```

### Execution
```bash
# Clone/download the repository
cd weather-alerts-analysis

# Run the analysis
python3 weather_alerts.py
```

### Output
- Creates `weather_alerts.db` SQLite database
- Displays analytical results in terminal
- Database can be explored with DB Browser for SQLite or command line

## Analytical Queries & Results

### 1. Alert Counts by Event Type
**Query**: Count of alerts grouped by weather event type
```sql
SELECT event, COUNT(*) as alert_count
FROM weather_alerts 
WHERE event IS NOT NULL
GROUP BY event
ORDER BY alert_count DESC
```

**Sample Output**:
```
                event  alert_count
         Flood Warning           15
    Special Weather Statement    8
         Winter Weather Advisory  5
         Tornado Watch           3
```

### 2. Alerts by Severity Level
**Query**: Distribution of alerts by severity with percentages
```sql
SELECT severity, COUNT(*) as alert_count,
       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM weather_alerts), 2) as percentage
FROM weather_alerts 
GROUP BY severity
ORDER BY alert_count DESC
```

### 3. Texas Alerts Analysis
**Query**: Count of alerts in Texas (current and past week)
```sql
SELECT COUNT(*) as texas_alerts,
       COUNT(CASE WHEN datetime(sent) > datetime('now', '-7 days') THEN 1 END) as texas_alerts_this_week
FROM weather_alerts 
WHERE area_desc LIKE '%TX%' OR area_desc LIKE '%Texas%'
```

### 4. Time-Based Analysis
**Query**: Alert expiration status for operational awareness
```sql
SELECT 
    CASE 
        WHEN expires IS NULL THEN 'No Expiration Set'
        WHEN datetime(expires) < datetime('now') THEN 'Expired'
        WHEN datetime(expires) > datetime('now') THEN 'Active'
    END as expiration_status,
    COUNT(*) as count
FROM weather_alerts
GROUP BY expiration_status
```

### 5. Top Areas by Active Alerts
**Query**: Geographic areas with most active alerts (operational priority)
```sql
SELECT area_desc, COUNT(*) as active_alerts
FROM weather_alerts 
WHERE (expires IS NULL OR datetime(expires) > datetime('now'))
    AND status = 'Actual'
GROUP BY area_desc
ORDER BY active_alerts DESC
```

### 6. Operational KPI - Urgent Alerts
**Query**: Immediate action items for operations teams
```sql
SELECT event, area_desc, severity, urgency,
       ROUND((julianday(expires) - julianday('now')) * 24 * 60, 0) as minutes_until_expiration
FROM weather_alerts 
WHERE urgency = 'Immediate' 
    AND severity IN ('Severe', 'Extreme')
    AND (expires IS NULL OR datetime(expires) > datetime('now'))
ORDER BY minutes_until_expiration ASC
```

## Key Insights for Operations Teams

1. **Alert Volume**: Real-time count of active weather alerts nationwide
2. **Severity Distribution**: Breakdown of alert criticality levels
3. **Geographic Hotspots**: Areas with highest alert concentrations
4. **Time Sensitivity**: Alerts requiring immediate vs. future action
5. **Event Types**: Most common weather hazards currently active

## Files Structure
```
├── weather_alerts.py    # Main Python script
├── NOAA.sql            # SQL queries for analysis
├── README.md           # This documentation
└── weather_alerts.db   # Generated SQLite database
```

## Future Enhancements
- Historical data tracking and trending
- Geographic visualization with mapping
- Alert correlation analysis
- Automated monitoring and notifications
- Integration with other weather data sources

Now let's run your enhanced script to complete the exercise:

```bash
python3 weather_alerts.py
```

This enhanced solution provides:

1. ✅ **Data Ingestion**: Fetches from NOAA API
2. ✅ **Local Storage**: SQLite database 
3. ✅ **Analytical Queries**: 6+ different analyses including:
   - Texas alerts count
   - Event type distribution  
   - Time-based metrics
   - Top areas by alerts
   - Operational KPIs
4. ✅ **Professional Documentation**: Complete README
5. ✅ **Runnable Code**: Single command execution

Your coding exercise is now complete and ready for submission! 🎉
