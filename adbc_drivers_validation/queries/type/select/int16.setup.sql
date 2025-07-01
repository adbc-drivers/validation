DROP TABLE IF EXISTS test_int16;

CREATE TABLE test_int16 (
    idx INTEGER,
    res SMALLINT
);

INSERT INTO test_int16 (idx, res) VALUES (1, 16384);
INSERT INTO test_int16 (idx, res) VALUES (2, 32767);
INSERT INTO test_int16 (idx, res) VALUES (3, -32768);
INSERT INTO test_int16 (idx, res) VALUES (4, 0);
INSERT INTO test_int16 (idx, res) VALUES (5, NULL);
