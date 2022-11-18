SELECT drp.zone AS dropoff_zone
     , COUNT(y.index) AS trip_count
  FROM yellow_taxi_trips y
       LEFT JOIN zones drp
	     ON y."DOLocationID" = drp.locationid
	   LEFT JOIN zones pu
	     ON y."PULocationID" = pu.locationid
 WHERE pu.zone = 'Central Park'
   AND y.tpep_pickup_datetime::date = '2021-01-14'
 GROUP BY drp.zone
 ORDER BY COUNT(y.index) DESC;