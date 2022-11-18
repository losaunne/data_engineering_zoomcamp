## Week 1 

### Terraform
Followed [instructions](https://github.com/losaunne/data_engineering_zoomcamp/blob/main/week_1/terraform_gcp/README.md) to create a Google Cloud project and install terraform, generated credentials and saved .json file. Copied file `terraform_gcp` over from original repo, set default value for `project_id`, then ran the below commands:

- `terraform init`
- `terraform plan`
- `terraform apply`

### Docker / SQL
First, you must create the network and name the database using commands below.
```
docker network create pg-network
```

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /Users/losaunne/Desktop/projects/my_de_zoomcamp/week_1/docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

After creating all the data pipeline files, run
```
docker build -t taxi_ingest:001 .
```
to first build the Dockerfile. Next, run
```
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```
Data should now be loaded in the Postgres DB. Now if you want to query and launch, run `docker-compose up -d`. When you want to disconnect run `docker-compose down`.

* When connecting to postgres DB, use "pg-database" as host name