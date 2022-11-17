SELECT CONCAT(pu.zone, ' / ', drp.zone) AS from_to
     , AVG(y.total_amount) AS avg_fare
  FROM yellow_taxi_trips y
       LEFT JOIN zones drp
	     ON y."DOLocationID" = drp.locationid
	   LEFT JOIN zones pu
	     ON y."PULocationID" = pu.locationid
 GROUP BY CONCAT(pu.zone, ' / ', drp.zone)		
 ORDER BY AVG(y.total_amount) DESC;