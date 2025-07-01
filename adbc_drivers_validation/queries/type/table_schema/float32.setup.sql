DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res REAL
);

INSERT INTO test_table_schema (res) VALUES (3.14);
INSERT INTO test_table_schema (res) VALUES (0.0);
INSERT INTO test_table_schema (res) VALUES (-3.4e38);
INSERT INTO test_table_schema (res) VALUES (3.4e38);
INSERT INTO test_table_schema (res) VALUES (1.175494351e-38);
INSERT INTO test_table_schema (res) VALUES (NULL);
