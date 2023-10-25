CREATE SOURCE CONNECTOR `mysql-connector` WITH(
    "connector.class"= 'io.debezium.connector.mysql.MySqlConnector',
    "tasks.max"= '1',
    "database.hostname"= 'mysql',
    "database.port"= '3306',
    "database.user"= 'root',
    "database.password"= 'debezium',
    "database.server.id"= '184054',
    "database.server.name"= 'dbserver1',
    "database.whitelist"= 'inventory',
    "table.whitelist"= 'inventory.clients,inventory.products,inventory.orders',
    "database.history.kafka.bootstrap.servers"= 'kafka:9092',
    "database.history.kafka.topic"= 'schema-changes.inventory',
    "transforms"= 'unwrap',
    "transforms.unwrap.type"= 'io.debezium.transforms.ExtractNewRecordState',
    "key.converter"= 'org.apache.kafka.connect.json.JsonConverter',
    "key.converter.schemas.enable"= 'false',
    "value.converter"= 'org.apache.kafka.connect.json.JsonConverter',
    "value.converter.schemas.enable"= 'false');

show topics;

SET 'auto.offset.reset' = 'earliest';

PRINT "dbserver1.inventory.clients" FROM BEGINNING;
PRINT "dbserver1.inventory.orders" FROM BEGINNING;
PRINT "dbserver1.inventory.products" FROM BEGINNING;

CREATE STREAM S_CLIENTS (client_id INT,
                       NAME string,
                       EMAIL string)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.clients',
                       VALUE_FORMAT='json');

CREATE TABLE T_CLIENTS
AS
    SELECT client_id,
           latest_by_offset(name) as name,
           latest_by_offset(email) as email
    FROM s_clients
    GROUP BY client_id
    EMIT CHANGES;

CREATE STREAM S_PRODUCT (product_id INT,
                       NAME string,
                       description string,
                       price DOUBLE)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.products',
                       VALUE_FORMAT='json');

CREATE TABLE T_PRODUCT
AS
    SELECT product_id,
           latest_by_offset(name) as name,
           latest_by_offset(description) as description,
           latest_by_offset(price) as price
    FROM s_product
    GROUP BY product_id
    EMIT CHANGES;

CREATE STREAM s_order (
    order_id integer,
    order_date timestamp,
    client_id integer,
    product_id integer,
    quantity integer) 
    WITH (KAFKA_TOPIC='dbserver1.inventory.orders',VALUE_FORMAT='json');


SELECT
    o.product_id AS product_id,
    p.name AS product_name,
    IFNULL(SUM(o.quantity), 0) AS total_sales_volume
FROM s_order o
LEFT JOIN S_PRODUCT p ON o.product_id = p.product_id
GROUP BY o.product_id, p.name
EMIT CHANGES;


SELECT TITLE,
       SUM(TICKET_TOTAL_VALUE) AS TOTAL_VALUE
FROM MOVIE_TICKET_SALES
GROUP BY TITLE
EMIT CHANGES
LIMIT 3;


SELECT SUM(quantity) as quantity FROM s_order emit changes;


SELECT o.order_id, p.name AS product
FROM s_order AS o
LEFT JOIN s_product AS p ON p.product_id = o.product_id
EMIT CHANGES;

select o.order_id, o.quantity, p.name as product, SUM(o.quantity) AS total_sales_volume from s_order as o left join s_product as p on p.product_id = o.product_id emit changes;




select p.product_id AS PRODUCT_ID, p.name AS product, SUM(o.quantity) as quantity 
from s_order as o 
LEFT JOIN t_product AS p ON p.product_id = o.product_id
group by quantity, p.name
emit changes;





select o.order_number, o.quantity, p.name as product, c.email as customer, p.id as product_id, c.id as customer_id
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
emit changes;

CREATE STREAM SA_ENRICHED_ORDER WITH (VALUE_FORMAT='AVRO') AS
   select o.order_number, o.quantity, p.name as product, c.email as customer, p.id as product_id, c.id as customer_id
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
partition by o.order_number
emit changes;

CREATE SINK CONNECTOR `postgres-sink` WITH(
    "connector.class"= 'io.confluent.connect.jdbc.JdbcSinkConnector',
    "tasks.max"= '1',
    "dialect.name"= 'PostgreSqlDatabaseDialect',
    "table.name.format"= 'en_order',
    "topics"= 'SA_ENRICHED_ORDER',
    "connection.url"= 'jdbc:postgresql://postgres:5432/inventory?user=postgresuser&password=postgrespw',
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



CREATE TABLE employees (
    first_name VARCHAR(50)
);

CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

INSERT INTO orders (order_date, purchaser, quantity, product_id)
VALUES ('2023-10-24', 1001, 5, 2002);


UPDATE products SET price = 10.99 WHERE id = 101;

UPDATE products SET price = 20.99 WHERE id = 102;
UPDATE products SET price = 30.99 WHERE id = 103;

UPDATE products SET price = 40.99 WHERE id = 104;

UPDATE products SET price = 60.99 WHERE id = 105;
UPDATE products SET price = 340.99 WHERE id = 106;

UPDATE products SET price = 1140.99 WHERE id = 107;

UPDATE products SET price = 240.99 WHERE id = 108;

UPDATE products SET price = 340.99 WHERE id = 109;

 products | CREATE TABLE `products2` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `description` varchar(512) DEFAULT NULL,
  `weight` float DEFAULT NULL,
  `price` int DEFAULT NULL,
  PRIMARY KEY (`id`)
)


INSERT INTO orders (order_id, order_date, client_id, product_id, quantity)
VALUES
    (1001, '2023-10-24', 1, 1, 5), 
    (1002, '2023-10-25', 2, 2, 3),  
    (1003, '2023-10-26', 1, 3, 2);


SELECT
    p.product_id AS product_id,
    p.name AS product_name,
    SUM(o.quantity) AS total_sales_volume
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name
ORDER BY p.product_id;


docker run --name mysql -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=change-me mysql:8



CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(255),
    description VARCHAR(512),
    price FLOAT
);


CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    order_date DATE,
    client_id INT,
    product_id INT,
    quantity INT,
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);


CREATE TABLE clients (
    client_id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255)
);



-- Insert data for clients
INSERT INTO clients (client_id, name, email)
VALUES
    (1, 'John Smith', 'john.smith@example.com'),
    (2, 'Jane Doe', 'jane.doe@example.com'),
    (3, 'Bob Johnson', 'bob.johnson@example.com');


-- Insert data for products
INSERT INTO products (product_id, name, description, price)
VALUES
    (1, 'Product A', 'Description for Product A', 29.99),
    (2, 'Product B', 'Description for Product B', 39.99),
    (3, 'Product C', 'Description for Product C', 49.99);


-- Insert data for orders
INSERT INTO orders (order_id, order_date, client_id, product_id, quantity)
VALUES
    (1001, '2023-10-24', 1, 1, 5),  
    (1002, '2023-10-25', 2, 2, 3),  
    (1003, '2023-10-26', 3, 3, 2);  


INSERT INTO orders (order_id, order_date, client_id, product_id, quantity)
VALUES (1004, '2023-10-24', 3, 3, 5);

INSERT INTO orders (order_id, order_date, client_id, product_id, quantity, price)
VALUES (2, '2023-10-24', 1001, 2002, 1, 129.99);

INSERT INTO orders (order_id, order_date, client_id, product_id, quantity, price)
VALUES (3, '2023-10-24', 1001, 2003, 3, 339.99);



SELECT
    c.client_id,
    c.name AS client_name,
    SUM(p.price) AS total_price
FROM clients c
LEFT JOIN orders o ON c.client_id = o.client_id
LEFT JOIN products p ON o.product_id = p.product_id
GROUP BY c.client_id, c.name
ORDER BY total_price DESC;


