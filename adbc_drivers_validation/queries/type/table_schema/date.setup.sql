DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res DATE
);

INSERT INTO test_table_schema (res) VALUES (DATE '2023-05-15');
INSERT INTO test_table_schema (res) VALUES (DATE '0001-01-01');
INSERT INTO test_table_schema (res) VALUES (DATE '1969-07-20');
INSERT INTO test_table_schema (res) VALUES (DATE '9999-12-31');
INSERT INTO test_table_schema (res) VALUES (NULL);
