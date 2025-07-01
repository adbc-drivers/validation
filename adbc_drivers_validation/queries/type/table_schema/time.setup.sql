DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res TIME
);

INSERT INTO test_table_schema (res) VALUES (TIME '13:45:31.123456');
INSERT INTO test_table_schema (res) VALUES (TIME '00:00:00');
INSERT INTO test_table_schema (res) VALUES (TIME '23:59:59.999999');
INSERT INTO test_table_schema (res) VALUES (TIME '12:30:45.500');
INSERT INTO test_table_schema (res) VALUES (NULL);
