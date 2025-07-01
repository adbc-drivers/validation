DROP TABLE IF EXISTS test_float32;

CREATE TABLE test_float32 (
    idx INTEGER,
    res REAL
);

INSERT INTO test_float32 (idx, res) VALUES (1, 3.14);
INSERT INTO test_float32 (idx, res) VALUES (2, 0.0);
INSERT INTO test_float32 (idx, res) VALUES (3, -3.4e38);
INSERT INTO test_float32 (idx, res) VALUES (4, 3.4e38);
INSERT INTO test_float32 (idx, res) VALUES (5, 1.175494351e-38);
INSERT INTO test_float32 (idx, res) VALUES (6, NULL);
