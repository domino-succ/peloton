-- create the test tables
--DROP TABLE IF EXISTS A;
--DROP TABLE IF EXISTS B;

SET ENABLE_MERGEJOIN TO FALSE;
SET ENABLE_HASHJOIN TO FALSE;
SET ENABLE_NESTLOOP TO TRUE;

CREATE TABLE A(id INT, value INT);
CREATE TABLE B(id INT, value INT);

-- join with empty tables
--SELECT * FROM A INNER JOIN B ON A.value = B.value;
--SELECT * FROM A LEFT OUTER JOIN B ON A.value = B.value;
--SELECT * FROM A RIGHT OUTER JOIN B ON A.value = B.value;
--SELECT * FROM A FULL OUTER JOIN B ON A.value = B.value;

-- load in the data

INSERT INTO A VALUES(0, 10);
INSERT INTO A VALUES(1, 11);
INSERT INTO A VALUES(2, 12);
INSERT INTO A VALUES(3, 13);
INSERT INTO A VALUES(4, 14);
INSERT INTO A VALUES(5, 15);
INSERT INTO A VALUES(6, 16);
INSERT INTO A VALUES(7, 17);
INSERT INTO A VALUES(8, 18);
INSERT INTO A VALUES(9, 19);

INSERT INTO B VALUES(20, 20);
INSERT INTO B VALUES(21, 21);
INSERT INTO B VALUES(22, 22);
INSERT INTO B VALUES(23, 23);
INSERT INTO B VALUES(24, 24);

-- join with non empty tables
SELECT * FROM A INNER JOIN B ON A.value = B.value;
SELECT * FROM A LEFT OUTER JOIN B ON A.value = B.value;
SELECT * FROM A RIGHT OUTER JOIN B ON A.value = B.value;
SELECT * FROM A FULL OUTER JOIN B ON A.value = B.value;

-- load in some more data

INSERT INTO B VALUES(25, 25);
INSERT INTO B VALUES(26, 26);
INSERT INTO A VALUES(4, 14);

-- join with non empty tables
SELECT * FROM A INNER JOIN B ON A.value = B.value;
SELECT * FROM A LEFT OUTER JOIN B ON A.value = B.value;
SELECT * FROM A RIGHT OUTER JOIN B ON A.value = B.value;
SELECT * FROM A FULL OUTER JOIN B ON A.value = B.value;
