DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res TIMESTAMP
);

INSERT INTO test_table_schema (res) VALUES (TIMESTAMP '2023-05-15 13:45:30');
INSERT INTO test_table_schema (res) VALUES (TIMESTAMP '2000-01-01 00:00:00');
INSERT INTO test_table_schema (res) VALUES (TIMESTAMP '1969-07-20 20:17:40');
INSERT INTO test_table_schema (res) VALUES (TIMESTAMP '9999-12-31 23:59:59');
INSERT INTO test_table_schema (res) VALUES (NULL);
