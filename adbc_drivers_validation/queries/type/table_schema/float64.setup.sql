DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res DOUBLE PRECISION
);

INSERT INTO test_table_schema (res) VALUES (3.14159265358979);
INSERT INTO test_table_schema (res) VALUES (0.0);
INSERT INTO test_table_schema (res) VALUES (-1.7976931348623157e308);
INSERT INTO test_table_schema (res) VALUES (1.7976931348623157e308);
INSERT INTO test_table_schema (res) VALUES (2.2250738585072014e-308);
INSERT INTO test_table_schema (res) VALUES (NULL);
