DROP TABLE IF EXISTS test_boolean;

CREATE TABLE test_boolean (
    idx INTEGER,
    res BOOLEAN
);

INSERT INTO test_boolean (idx, res) VALUES (1, TRUE);
INSERT INTO test_boolean (idx, res) VALUES (2, FALSE);
INSERT INTO test_boolean (idx, res) VALUES (3, NULL);
