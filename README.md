
# Building an ETL pipeline using KAKFA

## Architechture

![alt text](https://github.com/[username]/[reponame]/blob/[branch]/image.jpg?raw=true)

## Build containers 
docker-compose -f docker-compose.yml up --build --no-start

## Start containers
docker-compose -f docker-compose.yml start

## Access to ksqldb
docker-compose exec ksqldb-cli  ksql http://primary-ksqldb-server:8088
