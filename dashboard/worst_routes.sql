SELECT
    CONCAT(Origin, ' → ', Dest) AS route,
    Reporting_Airline AS airline,
    COUNT(*) AS total_flights,
    ROUND(AVG(ArrDelay), 1) AS avg_delay_mins,
    MAX(ArrDelay) AS worst_delay_mins,
    SUM(Cancelled) AS cancellations
FROM airline.gold.fact_flight_delays
WHERE ArrDelay > 0
GROUP BY Origin, Dest, Reporting_Airline
HAVING COUNT(*) >= 10
ORDER BY avg_delay_mins DESC
LIMIT 15;
