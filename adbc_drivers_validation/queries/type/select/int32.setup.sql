CREATE TABLE test_int32 (
    idx INTEGER,
    res INTEGER
);

INSERT INTO test_int32 (idx, res) VALUES (1, 131072);
INSERT INTO test_int32 (idx, res) VALUES (2, 2147483647);
INSERT INTO test_int32 (idx, res) VALUES (3, -2147483648);
INSERT INTO test_int32 (idx, res) VALUES (4, 0);
INSERT INTO test_int32 (idx, res) VALUES (5, NULL);
