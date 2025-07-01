DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res NUMERIC(10,2)
);

INSERT INTO test_table_schema (res) VALUES (123.45);
INSERT INTO test_table_schema (res) VALUES (0.00);
INSERT INTO test_table_schema (res) VALUES (-999.99);
INSERT INTO test_table_schema (res) VALUES (9999999.99);
INSERT INTO test_table_schema (res) VALUES (NULL);
