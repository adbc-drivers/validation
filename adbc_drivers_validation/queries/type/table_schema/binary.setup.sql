DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res BLOB
);

INSERT INTO test_table_schema (res) VALUES (X'e38193e38293e381abe381a1e381afe38081e4b896e7958cefbc81');
INSERT INTO test_table_schema (res) VALUES (X'00');
INSERT INTO test_table_schema (res) VALUES (X'deadbeef');
INSERT INTO test_table_schema (res) VALUES (X'');
INSERT INTO test_table_schema (res) VALUES (NULL);
