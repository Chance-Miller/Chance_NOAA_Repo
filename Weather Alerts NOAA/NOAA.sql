-- NOAA Weather Alerts Analysis Queries

-- 1. Create table structure for weather alerts
CREATE TABLE IF NOT EXISTS weather_alerts (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Count alerts by severity
SELECT 
    severity,
    COUNT(*) as alert_count
FROM weather_alerts 
WHERE severity IS NOT NULL
GROUP BY severity
ORDER BY alert_count DESC;

-- 3. Count alerts by event type
SELECT 
    event,
    COUNT(*) as alert_count
FROM weather_alerts 
WHERE event IS NOT NULL
GROUP BY event
ORDER BY alert_count DESC;

-- 4. Active alerts (not expired)
SELECT 
    event,
    area_desc,
    severity,
    headline,
    effective,
    expires
FROM weather_alerts 
WHERE datetime(expires) > datetime('now')
   OR expires IS NULL
ORDER BY severity DESC, effective DESC;

-- 5. Alerts by state/area
SELECT 
    CASE 
        WHEN area_desc LIKE '%,%' THEN TRIM(SUBSTR(area_desc, INSTR(area_desc, ',') + 1))
        ELSE area_desc
    END as state,
    COUNT(*) as alert_count
FROM weather_alerts 
WHERE area_desc IS NOT NULL
GROUP BY state
ORDER BY alert_count DESC;

-- 6. Recent alerts (last 24 hours)
SELECT 
    event,
    area_desc,
    severity,
    headline,
    sent
FROM weather_alerts 
WHERE datetime(sent) > datetime('now', '-1 day')
ORDER BY sent DESC;

-- 7. Flood warnings specifically
SELECT 
    area_desc,
    headline,
    description,
    effective,
    expires
FROM weather_alerts 
WHERE event LIKE '%Flood%'
ORDER BY effective DESC;
