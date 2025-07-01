DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res BIGINT
);

INSERT INTO test_table_schema (res) VALUES (4294967296);
INSERT INTO test_table_schema (res) VALUES (9223372036854775807);
INSERT INTO test_table_schema (res) VALUES (-9223372036854775808);
INSERT INTO test_table_schema (res) VALUES (0);
INSERT INTO test_table_schema (res) VALUES (NULL);
