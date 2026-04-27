    Reporting_Airline                           AS airline,
    COUNT(*)                                    AS total_flights,
    SUM(CASE WHEN delay_bucket = 'on_time' 
        THEN 1 ELSE 0 END)                      AS on_time_flights,
    ROUND(SUM(CASE WHEN delay_bucket = 'on_time' 
        THEN 1 ELSE 0 END) * 100.0 
        / COUNT(*), 1)                          AS on_time_pct,
    ROUND(AVG(ArrDelay), 1)                     AS avg_arr_delay_mins
FROM airline.gold.fact_flight_delays
GROUP BY Reporting_Airline
ORDER BY on_time_pct DESC;
