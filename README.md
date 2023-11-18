
# Building an ETL pipeline using KAKFA

## Architechture

![alt text]((https://github.com/JATTYz/ETL-Pipeline-with-Kafka/blob/main/ETL_Architechture.png))

## Build containers 
docker-compose -f docker-compose.yml up --build --no-start

## Start containers
docker-compose -f docker-compose.yml start

## Access to ksqldb
docker-compose exec ksqldb-cli  ksql http://primary-ksqldb-server:8088
