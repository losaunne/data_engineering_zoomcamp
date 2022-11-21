# Notes
run build command whenever changing dockerfile (and docker-compose?) (`docker-compose build`, then `docker-compose up -d`)
would need to change airflow credentials in a production environment

crontab.guru is a helpful cron website

docker exec -it worker-container-id bash - helpful to open the container in your bash prompt

airflow uses jinja, a templating engine

https://towardsdatascience.com/using-apache-airflow-dockeroperator-with-docker-compose-57d0217c8219

When using airflow workers, you can't install needed packages in Dockerflie, you would instead list them in requirements.txt

IDEMPOTENCY - the same result regardless of how many times you run it. Should design airflow pipelines in this way, i.e. upload table with unique name

pgcli -h localhost -p 5432 -U root -d ny_taxi
\dt - shows list of tables

SELECT 
  EXTRACT(YEAR FROM tpep_pickup_datetime) AS year_num,
  EXTRACT(MONTH FROM tpep_pickup_datetime) AS year_month,
  COUNT(1) AS record_count
  FROM `trips_data_all.yellow_taxi_trips`
GROUP BY 
  year_num,
  year_month;