
DROP TABLE products_on_hand;
DROP TABLE orders;
DROP TABLE products;
DROP TABLE geom;


CREATE TABLE `products` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `description` varchar(512) DEFAULT NULL,
  `weight` float DEFAULT NULL,
  `price` int DEFAULT NULL,
  PRIMARY KEY (`id`)
);

INSERT INTO `products` (`id`,`name`, `description`, `weight`, `price`)
VALUES (101,'Lenovo Legion 9i', 
        'Intel Core i9-13980HX, 64GB DDR5-5600, and 2TB SSD', 
        2.56, 8879);

INSERT INTO `products` (`id`,`name`, `description`, `weight`, `price`)
VALUES (102,'Asus ROG Strix Scar 18', 
        'Intel Core i9-13980HX Processor (24-core), 64GB DDR5-4800 RAM, and 2TB SSD (1TB + 1TB)', 
        3.16, 6799);

INSERT INTO `products` (`id`,`name`, `description`, `weight`, `price`)
VALUES (103,'AMD Ryzen 9 7950X3D', 
        'AMD Ryzen 9 7950X3D AM5 Processor with 16 cores, 32 threads, and 120W TDP', 
        0.21, 1089);

INSERT INTO `products` (`id`,`name`, `description`, `weight`, `price`)
VALUES (104,'MSI Stealth 16 Mercedes', 
        'Intel® Core™ i9-13900H Processor, 32GB DDR5-5200 RAM, and a 2TB SSD', 
        3.21, 5499);

INSERT INTO `products` (`id`,`name`, `description`, `weight`, `price`)
VALUES (105,'Karuza ROG EVANGELION', 
        'EVANGELION-02 Core i9 13900KS RTX 4090 Gaming PC', 
        3.65, 10999);

CREATE TABLE `orders` (
  `order_number` int NOT NULL AUTO_INCREMENT,
  `order_date` date NOT NULL,
  `purchaser` int NOT NULL,
  `quantity` int NOT NULL,
  `product_id` int NOT NULL,
  `total_price` int NOT NULL,
  PRIMARY KEY (`order_number`),
  KEY `order_customer` (`purchaser`),
  KEY `ordered_product` (`product_id`),
  CONSTRAINT `orders_ibfk_1` FOREIGN KEY (`purchaser`) REFERENCES `customers` (`id`),
  CONSTRAINT `orders_ibfk_2` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
);

CREATE TABLE `orders` (
  `order_number` int NOT NULL AUTO_INCREMENT,
  `order_date` date NOT NULL,
  `purchaser` int NOT NULL,
  `quantity` int NOT NULL,
  `product_id` int NOT NULL,
  `total_price` int NOT NULL,
  PRIMARY KEY (`order_number`),
  KEY `order_customer` (`purchaser`),
  KEY `ordered_product` (`product_id`),
  FOREIGN KEY (`purchaser`) REFERENCES `customers` (`id`),
  FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
);

INSERT INTO `orders` (`order_number`,`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES (1001, '2023-10-28', 1001, 5, 101, 44395);
INSERT INTO `orders` (`order_number`,`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES (1002, '2023-10-28', 1002, 1, 102, 6799);
INSERT INTO `orders` (`order_number`,`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES (1003, '2023-10-28', 1003, 4, 103, 4356);
INSERT INTO `orders` (`order_number`,`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES (1004, '2023-10-28', 1004, 2, 104, 10998);

INSERT INTO `orders` (`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES ('2023-10-28', 1001, 2, 104, 10998);
INSERT INTO `orders` (`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES ('2023-10-28', 1001, 2, 104, 10998);
INSERT INTO `orders` (`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES ('2023-10-28', 1001, 2, 104, 10998);


INSERT INTO `orders` (`order_date`, `purchaser`, `quantity`, `product_id`, `total_price`)
VALUES ('2023-10-28', 1005, 100, 103, 108900);



CREATE SOURCE CONNECTOR `mysql-connector` WITH(
    "connector.class"= 'io.debezium.connector.mysql.MySqlConnector',
    "tasks.max"= '1',
    "database.hostname"= 'mysql',
    "database.port"= '3306',
    "database.user"= 'root',
    "database.password"= '1234',
    "database.server.id"= '184054',
    "database.server.name"= 'dbserver1',
    "database.whitelist"= 'inventory',
    "table.whitelist"= 'inventory.customers,inventory.products,inventory.orders',
    "database.history.kafka.bootstrap.servers"= 'kafka:9092',
    "database.history.kafka.topic"= 'schema-changes.inventory',
    "transforms"= 'unwrap',
    "transforms.unwrap.type"= 'io.debezium.transforms.ExtractNewRecordState',
    "key.converter"= 'org.apache.kafka.connect.json.JsonConverter',
    "key.converter.schemas.enable"= 'false',
    "value.converter"= 'org.apache.kafka.connect.json.JsonConverter',
    "value.converter.schemas.enable"= 'false');

SET 'auto.offset.reset' = 'earliest';

PRINT "dbserver1.inventory.customers" FROM BEGINNING;
PRINT "dbserver1.inventory.orders" FROM BEGINNING;
PRINT "dbserver1.inventory.products" FROM BEGINNING;

CREATE STREAM S_CUSTOMER (ID INT,
                       FIRST_NAME string,
                       LAST_NAME string,
                       EMAIL string)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.customers',
                       VALUE_FORMAT='json');

CREATE TABLE T_CUSTOMER
AS
    SELECT id,
           latest_by_offset(first_name) as first_name,
           latest_by_offset(last_name) as last_name,
           latest_by_offset(email) as email
    FROM s_customer
    GROUP BY id
    EMIT CHANGES;


CREATE STREAM S_PRODUCT (ID INT,
                       NAME string,
                       description string,
                       weight DOUBLE,
                       price INT)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.products',
                       VALUE_FORMAT='json');

CREATE TABLE T_PRODUCT
AS
    SELECT id,
           latest_by_offset(name) as name,
           latest_by_offset(description) as description,
           latest_by_offset(weight) as weight,
           latest_by_offset(price) as price
    FROM s_product
    GROUP BY id
    EMIT CHANGES;

CREATE STREAM S_ORDER (
    order_number integer,
    order_date date,
    purchaser integer,
    quantity integer,
    product_id integer,
    total_price integer
    ) 
    WITH (KAFKA_TOPIC='dbserver1.inventory.orders',VALUE_FORMAT='json');



CREATE TABLE T_ORDER 
AS
    SELECT order_number,
           latest_by_offset(order_date) as order_date,
           latest_by_offset(purchaser) as purchaser,
           latest_by_offset(quantity) as quantity,
           latest_by_offset(product_id) as product_id,
           latest_by_offset(total_price) as total_price
    FROM s_order
    GROUP BY order_number
    EMIT CHANGES;



CREATE TABLE T_PRODUCT_SALES AS
SELECT  p.id, SUM(o.quantity) as quantity, SUM(o.total_price) as total_sales
FROM s_order as o 
LEFT JOIN t_product AS p ON p.id = o.product_id
GROUP BY p.id
emit changes;

CREATE TABLE T_ENRICHED_PRODUCT_SALES
AS SELECT s.ID AS ID , p.name, s.QUANTITY, s.TOTAL_SALES from T_PRODUCT_SALES s
LEFT JOIN t_product AS p ON p.id = s.ID
emit changes;

CREATE STREAM S_PRODCUT_SALES (
    id INT KEY,
    name string,
    quantity int,
    total_sales int
)WITH (
    KAFKA_TOPIC = 'T_ENRICHED_PRODUCT_SALES',
    VALUE_FORMAT= 'JSON'
);

CREATE STREAM S_ENRICHED_PRODUCT_SALES WITH (VALUE_FORMAT='AVRO') AS
select ID, name, quantity, total_sales from S_PRODCUT_SALES
partition by ID
emit changes;

CREATE SINK CONNECTOR `postgres-sink` WITH(
    "connector.class"= 'io.confluent.connect.jdbc.JdbcSinkConnector',
    "tasks.max"= '1',
    "dialect.name"= 'PostgreSqlDatabaseDialect',
    "table.name.format"= 'enriched_sales',
    "topics"= 'S_ENRICHED_PRODUCT_SALES',
    "connection.url"= 'jdbc:postgresql://postgres:5432/inventory?user=postgresuser&password=1234',
    "auto.create"= 'true',
    "insert.mode"= 'upsert',
    "pk.fields"= 'PRODUCT_ID',
    "pk.mode"= 'record_key',
    "key.converter"= 'org.apache.kafka.connect.converters.IntegerConverter',
    "key.converter.schemas.enable" = 'false',
    "value.converter"= 'io.confluent.connect.avro.AvroConverter',
    "value.converter.schemas.enable" = 'true',
    "value.converter.schema.registry.url"= 'http://schema-registry:8081'
);


CREATE STREAM S_ENRICHED_ORDER WITH (VALUE_FORMAT='AVRO') AS
select o.order_number AS ORDER_NUMBER, p.id as PRODUCT_ID, 
p.name as PRODUCT_NAME, 
o.quantity AS Quantity, 
c.id as customer_id, 
c.email as customer_email,
o.total_price AS total_price
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
partition by o.order_number
emit changes;


CREATE SINK CONNECTOR `postgres-sink2` WITH(
    "connector.class"= 'io.confluent.connect.jdbc.JdbcSinkConnector',
    "tasks.max"= '1',
    "dialect.name"= 'PostgreSqlDatabaseDialect',
    "table.name.format"= 'enriched_orders',
    "topics"= 'S_ENRICHED_ORDER',
    "connection.url"= 'jdbc:postgresql://postgres:5432/inventory?user=postgresuser&password=1234',
    "auto.create"= 'true',
    "insert.mode"= 'upsert',
    "pk.fields"= 'ORDER_NUMBER',
    "pk.mode"= 'record_key',
    "key.converter"= 'org.apache.kafka.connect.converters.IntegerConverter',
    "key.converter.schemas.enable" = 'false',
    "value.converter"= 'io.confluent.connect.avro.AvroConverter',
    "value.converter.schemas.enable" = 'true',
    "value.converter.schema.registry.url"= 'http://schema-registry:8081'
);


SELECT "PRODUCT_ID","NAME", "QUANTITY", "TOTAL_SALES" FROM enriched_sales ORDER BY "TOTAL_SALES" DESC;

\d+ enriched_orders;


curl -X "POST" "http://primary-ksqldb-server:8088" -H "Accept: application/vnd.ksql.v1+json" -d $'{"ksql": "LIST STREAMS;","streamsProperties": {}}'
