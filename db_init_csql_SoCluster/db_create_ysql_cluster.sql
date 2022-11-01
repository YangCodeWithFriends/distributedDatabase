
DROP DATABASE IF EXISTS dbysql;
CREATE DATABASE dbysql;
-- show all tables
-- \dt;

-- USE dbysql5424J; MySQL 
\c dbysql;

-- data path: /home/stuproj/cs4224j/project_data/data_files/

--  5 entity tables --
DROP TABLE if EXISTS warehouse CASCADE;
CREATE TABLE warehouse (
  W_id int NOT NULL,
  W_name varchar(10) NOT NULL,
  W_street_1 varchar(20) NOT NULL,
  W_street_2 varchar(20) NOT NULL,
  W_city varchar(20) NOT NULL,
  W_state char(2) NOT NULL,
  W_zip char(9) NOT NULL,
  W_tax decimal(4,4) NOT NULL,
  W_ytd decimal(12,2) NOT NULL,
  
  PRIMARY KEY(W_id HASH) -- yugabyte distrbuted table sharding
);

-- insert from csv
\copy warehouse from '/home/stuproj/cs4224j/project_data/data_files/warehouse.csv' WITH (FORMAT CSV, NULL 'null');


DROP TABLE if EXISTS district CASCADE;
CREATE TABLE district (
  -- D W ID is a foreign key that refers to warehouse table.
  D_W_id int NOT NULL REFERENCES warehouse(W_id),
  D_id int NOT NULL,
  -- Note: as compound foreign key
  PRIMARY KEY((D_W_id, D_id) HASH), -- yugabyte distrbuted table sharding

  D_name varchar(10) NOT NULL,
  D_street_1 varchar(20) NOT NULL,
  D_street_2 varchar(20) NOT NULL,
  D_city varchar(20) NOT NULL,
  D_state char(2) NOT NULL,
  D_zip char(9) NOT NULL,
  D_tax decimal(4,4) NOT NULL,
  D_ytd decimal(12,2) NOT NULL,
  D_next_O_id int NOT NULL
);

\copy district from '/home/stuproj/cs4224j/project_data/data_files/district.csv' WITH (FORMAT CSV, NULL 'null');


DROP TABLE if EXISTS customer CASCADE;
CREATE TABLE customer (
  -- combined (C W ID, C D ID) is a foreign key that refers to district table.
  C_W_id int NOT NULL,
  C_D_id int NOT NULL,
  FOREIGN KEY (C_W_id, C_D_id) REFERENCES district(D_W_id, D_id),
  C_id int NOT NULL,
  -- Note: as compound foreign key
  PRIMARY KEY((C_W_id, C_D_id, C_id) HASH), -- yugabyte distrbuted table sharding
  
  C_first varchar(16) NOT NULL,
  C_middle char(2) NOT NULL,
  C_last varchar(16) NOT NULL,
  C_street_1 varchar(20) NOT NULL,
  C_street_2 varchar(20) NOT NULL,
  C_city varchar(20) NOT NULL,
  C_state char(2) NOT NULL,
  C_zip char(9) NOT NULL,
  C_phone char(16) NOT NULL,
  C_since timestamp NOT NULL,
  C_credit char(2) NOT NULL,
  C_credit_lim decimal(12,2) NOT NULL,
  C_discount decimal(5,4) NOT NULL,
  C_balance decimal(12,2) NOT NULL,
  C_ytd_payment float NOT NULL,
  C_payment_cnt int NOT NULL,
  C_delivery_cnt int NOT NULL,
  C_data varchar(500) NOT NULL);
-- insert from csv
\copy customer from '/home/stuproj/cs4224j/project_data/data_files/customer.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_customers from customer;



-- Note: order is a keyword in SQL due to "order by"
-- 3e5
DROP TABLE if EXISTS orders CASCADE;
CREATE TABLE orders (
  -- (O W ID, O D ID, O C ID) is a foreign key that refers to customer table.
  O_W_id int NOT NULL,
  O_D_id int NOT NULL,
  O_id int NOT NULL,
  PRIMARY KEY((O_W_id, O_D_id, O_id) HASH),
  O_C_id int NOT NULL,
  FOREIGN KEY (O_W_id, O_D_id, O_C_id) REFERENCES customer(C_W_id, C_D_id, C_id),
  -- Note: as compound foreign key
  

  -- The range of O CARRIER ID is [1,10]: use smallint in pgsql(but small int is 16 bit in CQL, tinyint is 8)
  O_carrier_id smallint, -- data has lots of null
  O_OL_cnt decimal(2,0) NOT NULL,
  O_all_local decimal(1,0) NOT NULL,
  O_entry_d timestamp NOT NULL
);
-- insert from csv
\copy orders from '/home/stuproj/cs4224j/project_data/data_files/order.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_orders from orders;


-- 1e5
DROP TABLE if EXISTS item CASCADE;
CREATE TABLE item (
  I_id int NOT NULL,
  PRIMARY KEY(I_id HASH),
  I_name varchar(24) NOT NULL,
  i_price decimal(5,2) NOT NULL,
  I_im_id int NOT NULL,
  I_data varchar(50) NOT NULL
);
-- insert from csv
\copy item from '/home/stuproj/cs4224j/project_data/data_files/item.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_Item from item;


-- 1e6
DROP TABLE if EXISTS stock CASCADE;
CREATE TABLE stock (
  -- S I ID is a foreign key that refers to item table. 
  -- S W ID is a foreign key that refers to warehouse table.
  S_W_id int NOT NULL REFERENCES warehouse(W_id),
  S_I_id int NOT NULL REFERENCES item(I_id),
  PRIMARY KEY((S_W_id, S_I_id) HASH),
  
  S_quantity decimal(4,0) NOT NULL,
  S_ytd decimal(8,2) NOT NULL,
  S_order_cnt int NOT NULL,
  S_remote_cnt int NOT NULL,
  S_dist_01 char(24) NOT NULL,
  S_dist_02 char(24) NOT NULL,
  S_dist_03 char(24) NOT NULL,
  S_dist_04 char(24) NOT NULL,
  S_dist_05 char(24) NOT NULL,
  S_dist_06 char(24) NOT NULL,
  S_dist_07 char(24) NOT NULL,
  S_dist_08 char(24) NOT NULL,
  S_dist_09 char(24) NOT NULL,
  S_dist_10 char(24) NOT NULL,
  S_data varchar(50) NOT NULL
);
\copy stock from '/home/stuproj/cs4224j/project_data/data_files/stock.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_stock from stock;




-- 3749590
DROP TABLE if EXISTS orderline CASCADE;
CREATE TABLE orderline (
  -- (OL W ID, OL D ID, OL O ID) is a foreign key that refers to Order table. 
  -- OL I ID is a foreign key that refers to item table.
  OL_W_id int NOT NULL, 
  OL_D_id int NOT NULL, 
  OL_O_id int NOT NULL,
  FOREIGN KEY (OL_W_id, OL_D_id, OL_O_id) REFERENCES orders(O_W_id, O_D_id, O_id),
  OL_number int NOT NULL,
  -- PRIMARY KEY(OL_W_id, OL_D_id, OL_O_id, OL_number),
  PRIMARY KEY((OL_W_id, OL_D_id, OL_O_id, OL_number) HASH),
  OL_I_id int NOT NULL REFERENCES item(I_id),
  
  
  OL_delivery_D timestamp, -- data has lots of null
  OL_amount decimal(7,2) NOT NULL,
  OL_supply_W_id int NOT NULL,
  OL_quantity decimal(2,0) NOT NULL,
  OL_dist_info char(24) NOT NULL
);
\copy orderline from '/home/stuproj/cs4224j/project_data/data_files/order-line.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_OLine from "orderline";


-- 新表
-- 朱姐的 customer_item, 3749590
DROP TABLE if EXISTS customer_item CASCADE;
create table customer_item(
  CI_W_ID int, 
  CI_D_ID int, 
  CI_C_ID int, 
  CI_O_ID int, 
  CI_I_ID int,
  primary key(CI_W_ID,CI_D_ID,CI_C_ID,CI_O_ID,CI_I_ID)
);
\copy customer_item from '/home/stuproj/cs4224j/project_data/data_files/customer_item.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_customer_item from customer_item; 


-- 瑶姐要的临时表
create table new_order_info (
    NO_O_ID int NOT NULL, 
    NO_N int NOT NULL, 
    NO_W_ID int NOT NULL,  
    NO_D_ID int NOT NULL, 
    NO_C_ID int NOT NULL, 
    NO_I_CNT int NOT NULL, 
    NO_I_ID int NOT NULL, 
    NO_SUPPLY_W_ID int NOT NULL, 
    NO_QUANTITY decimal(2,0) NOT NULL, 
    primary key (NO_O_ID, NO_N, NO_W_ID, NO_D_ID, NO_C_ID)
);


-- 输出看导入结果
select count(*) as no_imported_district from district;
select count(*) as no_imported_orders from orders;
select count(*) as no_imported_customers from customer;
select count(*) as no_imported_Item from item;
select count(*) as no_imported_stock from stock;
select count(*) as no_imported_OLine from orderline;
select count(*) as no_imported_customer_item from customer_item; 
select count(*) as no_new_order_info from new_order_info;


-- show all tables
\dt;

