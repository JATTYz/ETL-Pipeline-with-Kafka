```
docker-compose -f docker-compose.yml up --build --no-start
```
```
docker-compose -f docker-compose.yml start
```

```
docker-compose exec ksqldb-cli  ksql http://primary-ksqldb-server:8088
```




docker exec -it etl_mysql_1 bash