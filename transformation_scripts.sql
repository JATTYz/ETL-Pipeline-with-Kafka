CREATE STREAM S_CUSTOMER (ID INT,
                       FIRST_NAME string,
                       LAST_NAME string,
                       EMAIL string)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.customers',
                       VALUE_FORMAT='json');

CREATE TABLE T_CUSTOMER
AS
    SELECT id,
           latest_by_offset(first_name) as fist_name,
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

CREATE STREAM s_order (
    order_number integer,
    order_date timestamp,
    purchaser integer,
    quantity integer,
    product_id integer,
    total_price integer
    ) 
    WITH (KAFKA_TOPIC='dbserver1.inventory.orders',VALUE_FORMAT='json');

CREATE STREAM SA_ENRICHED_ORDER WITH (VALUE_FORMAT='AVRO') AS
   select o.order_number AS ORDER_NUMBER, p.id as PRODUCT_ID, p.name as PRODUCT_NAME, o.quantity AS Quantity, c.id as customer_id, c.email as customer
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
partition by o.order_number
emit changes;



select o.order_number, o.quantity, c.email as customer, c.id as customer_id
from s_order as o 
left join t_customer as c on o.purchaser = c.id
emit changes;














CREATE STREAM ENRICHED_PRODUCT_SALES (
    name STRING KEY,
    quantity INT,
    total_sales INT
) WITH (
    KAFKA_TOPIC = 'T_PRODUCT_SALES',
    VALUE_FORMAT= 'JSON'
)


CREATE STREAM ENRICHED_PRODUCT_SALES (
    name STRING KEY,
    quantity INT,
    total_sales INT
) WITH (
    KAFKA_TOPIC = 'T_PRODUCT_SALES',
    VALUE_FORMAT= 'JSON'
)

CREATE STREAM ENRICHED_PRODUCT_SALES_T WITH (VALUE_FORMAT='AVRO') AS
select name, quantity, total_sales from ENRICHED_PRODUCT_SALES
partition by name
emit changes;