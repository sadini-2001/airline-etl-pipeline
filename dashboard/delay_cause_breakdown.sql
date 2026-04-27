SELECT
    primary_delay_cause,
    COUNT(*) AS total_flights,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM airline.gold.fact_flight_delays
WHERE primary_delay_cause != 'none'
AND ArrDelay > 0
GROUP BY primary_delay_cause
ORDER BY total_flights DESC;
