DROP TABLE IF EXISTS test_timestamptz;

CREATE TABLE test_timestamptz (
    idx INTEGER,
    res TIMESTAMP WITH TIME ZONE
);

INSERT INTO test_timestamptz (idx, res) VALUES (1, TIMESTAMP WITH TIME ZONE '2023-05-15 13:45:30+00');
INSERT INTO test_timestamptz (idx, res) VALUES (2, TIMESTAMP WITH TIME ZONE '2000-01-01 00:00:00+00');
INSERT INTO test_timestamptz (idx, res) VALUES (3, TIMESTAMP WITH TIME ZONE '1969-07-20 20:17:40+00');
INSERT INTO test_timestamptz (idx, res) VALUES (4, TIMESTAMP WITH TIME ZONE '9999-12-31 23:59:59+00');
INSERT INTO test_timestamptz (idx, res) VALUES (5, NULL);
