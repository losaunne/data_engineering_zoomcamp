SELECT *
  FROM yellow_taxi_trips y
 WHERE y.tip_amount IN (SELECT MAX(tip_amount) 
                          FROM yellow_taxi_trips)