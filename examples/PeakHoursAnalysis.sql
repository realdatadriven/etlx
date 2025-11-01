-- Peak Hours Analysis
SELECT EXTRACT(hour FROM tpep_pickup_datetime) AS hour_of_day,
    COUNT(*) AS total_trips,
    ROUND(AVG(total_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0), 2) AS avg_duration_minutes
FROM yellow_tripdata
WHERE tpep_dropoff_datetime > tpep_pickup_datetime
GROUP BY hour_of_day
ORDER BY hour_of_day