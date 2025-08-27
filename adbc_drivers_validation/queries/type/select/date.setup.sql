CREATE TABLE test_date (
    idx INTEGER,
    res DATE
);

INSERT INTO test_date (idx, res) VALUES (1, DATE '2023-05-15');
INSERT INTO test_date (idx, res) VALUES (2, DATE '0001-01-01');
INSERT INTO test_date (idx, res) VALUES (3, DATE '1969-07-20');
INSERT INTO test_date (idx, res) VALUES (4, DATE '9999-12-31');
INSERT INTO test_date (idx, res) VALUES (5, NULL);
