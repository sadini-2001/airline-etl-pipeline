SELECT
    d.year_month,
    d.month_name,
    COUNT(*) AS total_flights,
    ROUND(AVG(f.ArrDelay), 1) AS avg_delay_mins,
    SUM(CASE WHEN f.delay_bucket = 'severe' THEN 1 ELSE 0 END) AS severe_delays,
    SUM(f.Cancelled) AS cancellations
FROM airline.gold.fact_flight_delays f
JOIN airline.gold.dim_date d
    ON f.FlightDate = d.flight_date
GROUP BY d.year_month, d.month_name
ORDER BY d.year_month;
