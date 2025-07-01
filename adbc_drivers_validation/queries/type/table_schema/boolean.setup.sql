DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res BOOLEAN
);

INSERT INTO test_table_schema (res) VALUES (TRUE);
INSERT INTO test_table_schema (res) VALUES (FALSE);
INSERT INTO test_table_schema (res) VALUES (NULL);
