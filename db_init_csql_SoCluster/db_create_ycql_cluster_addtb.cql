
DROP TABLE IF EXISTS dbycql.warehouse;
DROP TABLE IF EXISTS dbycql.district;
DROP TABLE IF EXISTS dbycql.customer;
DROP TABLE IF EXISTS dbycql.orders;
DROP TABLE IF EXISTS dbycql.item;
DROP TABLE IF EXISTS dbycql.orderline;
DROP TABLE IF EXISTS dbycql.stock;
DROP TABLE IF EXISTS dbycql.customer_item;
DROP KEYSPACE IF EXISTS dbycql;

-- data path: /home/stuproj/cs4224j/project_data/data_files_old/

-- show all key spaces(DBs)
CREATE KEYSPACE dbycql 
WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': '3'} 
  ;
  -- AND DURABLE_WRITES = true;

-- DESCRIBE dbycql;
USE dbycql;


-- cql支持，键空间是一个定义节点上数据复制的命名空间。集群每个节点包含一个键空间，是否可以优化

--  5 entity tables --
CREATE TABLE dbycql.warehouse (
  -- 1-10, so ok in tinyint for [-128	127]
  W_id int, 
  -- partition key and primary key
  PRIMARY KEY(W_id), 
  W_name varchar,
  W_street_1 varchar,
  W_street_2 varchar,
  W_city varchar,
  W_state varchar,
  W_zip varchar,
  W_tax decimal,
  W_ytd decimal
);

-- WITH NULL='null': set NULL='null' or whatever character you want for null value ...
copy dbycql.warehouse from '/home/stuproj/cs4224j/project_data/data_files_old/warehouse.csv'
WITH NULL='null';


CREATE TABLE dbycql.district (
  -- 1-10,
  D_W_id int,
  -- 1-10,
  D_id int,
  -- Note: D_id is clustering key
  PRIMARY KEY(D_W_id, D_id),

  D_name varchar,
  D_street_1 varchar,
  D_street_2 varchar,
  D_city varchar,
  D_state varchar,
  D_zip varchar,
  D_tax decimal,
  D_ytd decimal,
  D_next_O_id int
);

copy dbycql.district from '/home/stuproj/cs4224j/project_data/data_files_old/district.csv' 
WITH NULL='null';


-- CREATE TABLE dbycql.customer (
--   C_W_id int,
--   C_D_id int,
--   C_id int,
--   -- Note: as compound foreign key
--   PRIMARY KEY((C_W_id, C_D_id), C_id),
  
--   C_first varchar,
--   C_middle varchar,
--   C_last varchar,
--   C_street_1 varchar,
--   C_street_2 varchar,
--   C_city varchar,
--   C_state varchar,
--   C_zip varchar,
--   C_phone varchar,
--   C_since timestamp,
--   C_credit varchar,
--   C_credit_lim decimal,
--   C_discount decimal,
--   C_balance decimal,
--   C_ytd_payment int,
--   C_payment_cnt int,
--   C_delivery_cnt int,
--   C_data varchar);
-- -- insert from csv
-- copy dbycql.customer from '/home/stuproj/cs4224j/project_data/data_files_old/customer.csv'
-- WITH NULL='null' AND INGESTRATE=7000;
-- select count(*) as no_imported_rows from dbycql.customer;

DROP TABLE IF EXISTS dbycql.customer;
CREATE TABLE dbycql.customer (
  C_W_id int,
  C_D_id int,
  C_id int,
  C_first varchar,
  C_middle varchar,
  C_last varchar,
  C_street_1 varchar,
  C_street_2 varchar,
  C_city varchar,
  C_state varchar,
  C_zip varchar,
  C_phone varchar,
  C_since timestamp,
  C_credit varchar,
  C_credit_lim decimal,
  C_discount decimal,
  C_balance decimal,
  C_ytd_payment float,
  C_payment_cnt int,
  C_delivery_cnt int,
  C_data varchar,
  PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID))
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC);

copy dbycql.customer (C_W_id,C_D_id,C_id,C_first,C_middle,C_last,C_street_1,C_street_2,C_city,C_state,C_zip,C_phone,C_since,C_credit,C_credit_lim,C_discount,C_balance,C_ytd_payment,C_payment_cnt,C_delivery_cnt,C_data)
from '/home/stuproj/cs4224j/project_data/data_files_old/customer.csv' 
WITH NULL='null' AND INGESTRATE=7000;
select count(*) as no_imported_customer from dbycql.customer;



-- orders
CREATE TABLE dbycql.orders (
  -- (O W ID, O D ID, O C ID) is a foreign key that refers to customer table.
  O_W_id int,
  O_D_id int,
  O_id int,
  O_C_id int,
  PRIMARY KEY ((O_W_id, O_D_id), O_id),
  -- The range of O CARRIER ID is [1,10]: use smallint is 16 bit in CQL, tinyint is 8
  O_carrier_id int, -- data has lots of null
  O_OL_cnt decimal,
  O_all_local decimal,
  O_entry_d timestamp
);
-- insert from csv
copy dbycql.orders from '/home/stuproj/cs4224j/project_data/data_files_old/order.csv'
WITH NULL='null' AND INGESTRATE=7000;
select count(*) as row_imported_orders from dbycql.orders;
-- select count(*) from orders;


-- item
CREATE TABLE dbycql.item (
  I_id int PRIMARY KEY,
  I_name varchar,
  i_price decimal,
  I_im_id int,
  I_data varchar
);
-- insert from csv
copy dbycql.item from '/home/stuproj/cs4224j/project_data/data_files_old/item.csv'
WITH NULL='null' AND INGESTRATE=7000;
select count(*) as no_imported_Item from dbycql.item;


-- 2 relationship tables -- 
CREATE TABLE dbycql.stock (
  -- S I ID is a primary key that refers to item table. 
  -- S W ID is a cluster key that refers to warehouse table.
  S_W_id int,
  S_I_id int,
  PRIMARY KEY(S_W_id, S_I_id),
  
  S_quantity decimal,
  -- S_ytd decimal,
  S_ytd decimal,
  S_order_cnt int,
  S_remote_cnt int,
  S_dist_01 varchar,
  S_dist_02 varchar,
  S_dist_03 varchar,
  S_dist_04 varchar,
  S_dist_05 varchar,
  S_dist_06 varchar,
  S_dist_07 varchar,
  S_dist_08 varchar,
  S_dist_09 varchar,
  S_dist_10 varchar,
  S_data varchar
);


copy dbycql.stock from '/home/stuproj/cs4224j/project_data/data_files_old/stock.csv'
WITH NULL='null' AND INGESTRATE=10000;
select count(*) as no_imported_stocks from dbycql.stock;


-- CREATE TABLE dbycql.orderline (
--   -- OL I ID is a foreign key that refers to item table.
--   OL_W_id int, 
--   OL_D_id int, 
--   OL_O_id int,
--   OL_number int,
--   PRIMARY KEY((OL_W_id, OL_D_id), OL_O_id, OL_number),
--   OL_I_id int,
--   OL_delivery_D timestamp, -- data has lots of null
--   OL_amount decimal,
--   OL_supply_W_id int,
--   OL_quantity decimal,
--   OL_dist_info varchar
-- );
-- copy dbycql.orderline from '/home/stuproj/cs4224j/project_data/data_files_old/order-line.csv'
-- WITH NULL='null' AND INGESTRATE=7000;
-- -- select * from "orderline" where OL_delivery_D is null;
-- select count(*) as no_imported_rows from dbycql.orderline;

-- 6. 新的 orderline
DROP TABLE IF EXISTS dbycql.orderline;
CREATE TABLE dbycql.orderline (
    ol_w_id int,
    ol_d_id int,
    ol_o_id int,
    ol_number int,
    ol_i_id int,
    ol_delivery_d timestamp,
    ol_amount decimal,
    ol_supply_w_id int,
    ol_quantity decimal,
    ol_dist_info text,
    PRIMARY KEY ((ol_w_id, ol_d_id), ol_quantity, ol_o_id, ol_number)
) WITH CLUSTERING ORDER BY (ol_quantity DESC, ol_o_id ASC, ol_number ASC);

copy dbycql.orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
from '/home/stuproj/cs4224j/project_data/data_files_old/order-line.csv' 
WITH NULL='null' AND INGESTRATE=10000;
-- select count(*) as no_imported_OL from dbycql.orderline;


-- 新加表
-- customer_item: warehouse，district，customer，order，item
-- cql: (CI_W_ID),CI_D_ID,CI_C_ID,CI_O_ID,CI_I_ID,
DROP TABLE IF EXISTS dbycql.customer_item;
create table dbycql.customer_item(
		CI_W_ID int, 
		CI_D_ID int, 
		CI_C_ID int, 
		CI_O_ID int, 
		CI_I_ID int,
		-- CI_I_NUMBER int,
primary key((CI_W_ID),CI_D_ID,CI_C_ID,CI_O_ID,CI_I_ID));

-- cql
copy customer_item from '/home/stuproj/cs4224j/project_data/data_files_old/customer_item.csv' 
WITH NULL='null' AND INGESTRATE=10000;
-- select count(*) as no_imported_customer_item from dbycql.orderline;


-- show all tables
DESCRIBE tables;
