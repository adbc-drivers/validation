CREATE TABLE test_int64 (
    idx INTEGER,
    res BIGINT
);

INSERT INTO test_int64 (idx, res) VALUES (1, 4294967296);
INSERT INTO test_int64 (idx, res) VALUES (2, 9223372036854775807);
INSERT INTO test_int64 (idx, res) VALUES (3, -9223372036854775808);
INSERT INTO test_int64 (idx, res) VALUES (4, 0);
INSERT INTO test_int64 (idx, res) VALUES (5, NULL);
