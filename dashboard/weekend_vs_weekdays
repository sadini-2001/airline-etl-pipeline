SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    d.day_name,
    COUNT(*) AS total_flights,
    ROUND(AVG(f.ArrDelay), 1) AS avg_delay_mins,
    ROUND(SUM(CASE WHEN f.delay_bucket = 'on_time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS on_time_pct
FROM airline.gold.fact_flight_delays f
JOIN airline.gold.dim_date d
    ON f.FlightDate = d.flight_date
GROUP BY d.is_weekend, d.day_name
ORDER BY d.day_name;
