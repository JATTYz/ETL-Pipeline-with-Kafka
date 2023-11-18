
# Building an ETL pipeline using KAKFA

## Pipeline Architecture

![alt text](https://github.com/JATTYz/ETL-Pipeline-with-Kafka/blob/main/ETL_Architecture.jpg)

## Build containers 
docker-compose -f docker-compose.yml up --build --no-start

## Start containers
docker-compose -f docker-compose.yml start

## Access to ksqldb
docker-compose exec ksqldb-cli  ksql http://primary-ksqldb-server:8088

### Implementations
Follow steps in the mysql.sql file
