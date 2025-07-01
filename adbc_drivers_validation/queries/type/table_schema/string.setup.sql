DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res VARCHAR(1000)
);

INSERT INTO test_table_schema (res) VALUES ('hello');
INSERT INTO test_table_schema (res) VALUES ('');
INSERT INTO test_table_schema (res) VALUES ('Special chars: !@#$%^&*()_+{}|:"<>?~`-=[]\;'',./');
INSERT INTO test_table_schema (res) VALUES ('Unicode: 你好, Привет, こんにちは, สวัสดี');
INSERT INTO test_table_schema (res) VALUES (NULL);
