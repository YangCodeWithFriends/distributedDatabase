
DROP TABLE IF EXISTS ycql_idx.warehouse;
DROP TABLE IF EXISTS ycql_idx.district;
DROP TABLE IF EXISTS ycql_idx.customer;
DROP TABLE IF EXISTS ycql_idx.orders;
DROP TABLE IF EXISTS ycql_idx.item;
DROP TABLE IF EXISTS ycql_idx.orderline;
DROP TABLE IF EXISTS ycql_idx.stock;
DROP TABLE IF EXISTS ycql_idx.customer_item;
DROP KEYSPACE IF EXISTS ycql_idx;

-- data path: /home/stuproj/cs4224j/project_data/data_files/

-- show all key spaces(DBs)
CREATE KEYSPACE ycql_idx 
WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': '3'} 
  ;
  -- AND DURABLE_WRITES = true;

-- DESCRIBE ycql_idx;
USE ycql_idx;


--  5 entity tables --
-- 10
CREATE TABLE ycql_idx.warehouse (
  -- 1-10, so ok in tinyint for [-128	127]
  W_id int, 
  W_name varchar,
  W_street_1 varchar,
  W_street_2 varchar,
  W_city varchar,
  W_state varchar,
  W_zip varchar,
  W_tax decimal,
  W_ytd decimal,
  PRIMARY KEY(W_id)
)
WITH transactions = { 'enabled' : true };

-- WITH NULL='null': set NULL='null' or whatever character you want for null value ...
copy ycql_idx.warehouse from '/home/stuproj/cs4224j/project_data/data_files/warehouse.csv'
WITH NULL='null';


-- 100
CREATE TABLE ycql_idx.district (
  -- 1-10,
  D_W_id int,
  -- 1-10,
  D_id int,
  -- Note: D_id is clustering key
  D_name varchar,
  D_street_1 varchar,
  D_street_2 varchar,
  D_city varchar,
  D_state varchar,
  D_zip varchar,
  D_tax decimal,
  D_ytd decimal,
  D_next_O_id int,
  PRIMARY KEY(D_W_id, D_id)
)
WITH transactions = { 'enabled' : true };

copy ycql_idx.district from '/home/stuproj/cs4224j/project_data/data_files/district.csv' 
WITH NULL='null';



DROP TABLE IF EXISTS ycql_idx.customer;
CREATE TABLE ycql_idx.customer (
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
  -- C_data varchar,
  PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID))
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC)
and transactions = { 'enabled' : true };

copy ycql_idx.customer (C_W_id,C_D_id,C_id,C_first,C_middle,C_last,C_street_1,C_street_2,C_city,C_state,C_zip,C_phone,C_since,C_credit,C_credit_lim,C_discount,C_balance,C_ytd_payment,C_payment_cnt,C_delivery_cnt)
from '/home/stuproj/cs4224j/project_data/data_files/customer_new.csv' 
WITH NULL='null' AND INGESTRATE=7000;
select count(*) as no_imported_customer from ycql_idx.customer;


-- orders
CREATE TABLE ycql_idx.orders (
  -- (O W ID, O D ID, O C ID) is a foreign key that refers to customer table.
  O_W_id int,
  O_D_id int,
  O_id int,
  O_C_id int,
  -- The range of O CARRIER ID is [1,10]: use smallint is 16 bit in CQL, tinyint is 8
  O_carrier_id int, -- data has lots of null
  O_OL_cnt decimal,
  O_all_local decimal,
  O_entry_d timestamp,
  PRIMARY KEY ((O_W_id, O_D_id), O_id)
)
WITH transactions = { 'enabled' : true };
-- insert from csv
copy ycql_idx.orders from '/home/stuproj/cs4224j/project_data/data_files/order.csv'
WITH NULL='null' AND INGESTRATE=7000;
select count(*) as row_imported_orders from ycql_idx.orders;
-- select count(*) from orders;


-- item
CREATE TABLE ycql_idx.item (
  I_id int,
  I_name varchar,
  i_price decimal,
  -- I_im_id int,
  -- I_data varchar
  PRIMARY KEY(I_id)
)
WITH transactions = { 'enabled' : true };
-- insert from csv
copy ycql_idx.item from '/home/stuproj/cs4224j/project_data/data_files/item_new.csv'
WITH NULL='null' AND INGESTRATE=7000;
select count(*) as no_imported_Item from ycql_idx.item;


-- 2 relationship tables -- 
CREATE TABLE ycql_idx.stock (
  -- S I ID is a primary key that refers to item table. 
  -- S W ID is a cluster key that refers to warehouse table.
  S_W_id int,
  S_I_id int,
  S_quantity decimal,
  -- S_ytd decimal,
  S_ytd decimal,
  S_order_cnt int,
  S_remote_cnt int,
  -- S_dist_01 varchar,
  -- S_dist_02 varchar,
  -- S_dist_03 varchar,
  -- S_dist_04 varchar,
  -- S_dist_05 varchar,
  -- S_dist_06 varchar,
  -- S_dist_07 varchar,
  -- S_dist_08 varchar,
  -- S_dist_09 varchar,
  -- S_dist_10 varchar,
  -- S_data varchar
  PRIMARY KEY(S_W_id, S_I_id)
)
WITH transactions = { 'enabled' : true };


copy ycql_idx.stock from '/home/stuproj/cs4224j/project_data/data_files/stock_new.csv'
WITH NULL='null' AND INGESTRATE=10000;
select count(*) as no_imported_stocks from ycql_idx.stock;


-- 6. 新的 orderline
DROP TABLE IF EXISTS ycql_idx.orderline;
CREATE TABLE ycql_idx.orderline (
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
) WITH CLUSTERING ORDER BY (ol_quantity DESC, ol_o_id ASC, ol_number ASC)
and transactions = { 'enabled' : true };

copy ycql_idx.orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
from '/home/stuproj/cs4224j/project_data/data_files/order-line.csv' 
WITH NULL='null' AND INGESTRATE=10000;
-- select count(*) as no_imported_OL from ycql_idx.orderline;


-- 新加表
-- customer_item: warehouse，district，customer，order，item
-- cql: (CI_W_ID),CI_D_ID,CI_C_ID,CI_O_ID,CI_I_ID,
DROP TABLE IF EXISTS ycql_idx.customer_item;
create table ycql_idx.customer_item(
		CI_W_ID int, 
		CI_D_ID int, 
		CI_C_ID int, 
		CI_O_ID int, 
		CI_I_ID int,
		-- CI_I_NUMBER int,
    primary key((CI_W_ID),CI_D_ID,CI_C_ID,CI_O_ID,CI_I_ID)
)
WITH transactions = { 'enabled' : true };

-- cql
copy customer_item from '/home/stuproj/cs4224j/project_data/data_files/customer_item.csv' 
WITH NULL='null' AND INGESTRATE=10000;
-- select count(*) as no_imported_customer_item from ycql_idx.orderline;


-- show all tables
DESCRIBE tables;

-- 加索引
-- create index if not exists w_id_idx on ycql_idx.warehouse (W_id);
-- create index if not exists district_idx on ycql_idx.district (D_W_ID, D_ID);
create index if not exists customer_idx on ycql_idx.customer (C_W_ID, C_D_ID, C_ID);
create index if not exists orders_idx on ycql_idx.orders (O_W_ID, O_D_ID, O_C_ID, O_ID, O_CARRIER_ID);
-- create index if not exists item_idx on ycql_idx.item (I_ID);
create index if not exists stock_idx on ycql_idx.stock (S_W_ID, S_I_ID);
create index if not exists orderline_idx on ycql_idx.orderline (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_QUANTITY);
create index if not exists customer_item_idx on ycql_idx.customer_item (CI_W_ID, CI_I_ID);