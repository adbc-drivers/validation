DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res SMALLINT
);

INSERT INTO test_table_schema (res) VALUES (16384);
INSERT INTO test_table_schema (res) VALUES (32767);
INSERT INTO test_table_schema (res) VALUES (-32768);
INSERT INTO test_table_schema (res) VALUES (0);
INSERT INTO test_table_schema (res) VALUES (NULL);
