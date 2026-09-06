CREATE SCHEMA iceberg.bronze WITH (location = 's3://lakehouse/bronze/');
SHOW SCHEMAS FROM iceberg;

CREATE TABLE iceberg.bronze.test_table (id INT, name VARCHAR);
INSERT INTO iceberg.bronze.test_table VALUES (1, 'hello');
SELECT * FROM iceberg.bronze.test_table;

CREATE SCHEMA iceberg.bronze WITH (location = 's3://lakehouse/bronze/');
CREATE TABLE iceberg.bronze.test_table (id INT, name VARCHAR);
INSERT INTO iceberg.bronze.test_table VALUES (1, 'hello');
SELECT * FROM iceberg.bronze.test_table;