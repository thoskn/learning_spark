#TODO
* Setup docker for DB
    - check networking. e.g --network=host etc., is it accessible to internet...?
* Spark job to do some aggregations etc. on data in DB
* Write results out to a redis cache
* Docker compose to setup DB, spark cluster, cache
* ECS?
* Combine another data source with db data in spark. e.g load from database and combine with data loaded from parquet -  do something with results
* Script for submitting job to cluster
* Spark streaming for loading to DB
    - also load to file (data lake like)
* File watcher submitting to spark stream
    - Java?
* Infinite loop: file watcher submit to stream, which does some stuff and writes back to location watched by file watcher
* build web app
    - vue.js or react...
    - submit new people, transactions, items
        - ingest automatically (spark stream?)
    - build the aggregates into cache on command (submit spark job)
    - display aggregate results
* deploy in AWS (cloudfront, S3, ECS/EKS/EMR, Elasticache)
    
# Setup
## Requirements
* Docker

## Steps
1. Start the postgres database by running the following command (yes I know the password here is bad)
    ```commandline
    docker run --rm --name spark_postgres -e POSTGRES_PASSWORD=pword -d -p 5432:5432 postgres
    ``` 
    You can confirm it is running by using the following command (if psql is installed)
    ```commandline
    psql -h localhost -p 5432 -U postgres -d postgres
    ```
    Once connected to postgres you can run `\list` to view the databases. `\q` to quit.

1. Create database. In psql run:
```psql
create database spark_stuff;
```

1. Build the pyspark image
```commandline
 docker build -f docker/pyspark/Dockerfile .
```
docker run --rm -e POSTGRES_HOST=172.17.0.2 pyspark:gen_load

docker-compose -f docker/docker-compose.yaml up --build