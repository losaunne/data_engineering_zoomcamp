SELECT count(1) 
  FROM yellow_taxi_trips
 WHERE DATE(tpep_pickup_datetime) = '2021-01-15';