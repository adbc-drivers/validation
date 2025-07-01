DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res INTEGER
);

INSERT INTO test_table_schema (res) VALUES (131072);
INSERT INTO test_table_schema (res) VALUES (2147483647);
INSERT INTO test_table_schema (res) VALUES (-2147483648);
INSERT INTO test_table_schema (res) VALUES (0);
INSERT INTO test_table_schema (res) VALUES (NULL);
