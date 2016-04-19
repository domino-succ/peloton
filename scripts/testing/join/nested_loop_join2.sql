-- create the test tables
SET ENABLE_MERGEJOIN TO FALSE;
SET ENABLE_HASHJOIN TO FALSE;
SET ENABLE_NESTLOOP TO TRUE;

CREATE TABLE C(id INT, value INT);
CREATE TABLE D(id INT, value INT);

-- join with empty tables
SELECT * FROM C INNER JOIN D ON C.value = D.value;
SELECT * FROM C LEFT OUTER JOIN D ON C.value = D.value;
SELECT * FROM C RIGHT OUTER JOIN D ON C.value = D.value;
SELECT * FROM C FULL OUTER JOIN D ON C.value = D.value;

-- load in the data

INSERT INTO C VALUES(100, 100);
INSERT INTO C VALUES(101, 101);
INSERT INTO C VALUES(102, 102);
INSERT INTO C VALUES(103, 103);
INSERT INTO C VALUES(104, 104);
INSERT INTO C VALUES(105, 105);
INSERT INTO C VALUES(106, 106);
INSERT INTO C VALUES(107, 107);
INSERT INTO C VALUES(108, 108);
INSERT INTO C VALUES(109, 109);

INSERT INTO D VALUES(120, 120);
INSERT INTO D VALUES(121, 121);
INSERT INTO D VALUES(122, 122);
INSERT INTO D VALUES(123, 123);
INSERT INTO D VALUES(124, 124);

-- join with non empty tables
SELECT * FROM C INNER JOIN D ON C.value = D.value;
SELECT * FROM C LEFT OUTER JOIN D ON C.value = D.value;
SELECT * FROM C RIGHT OUTER JOIN D ON C.value = D.value;
SELECT * FROM C FULL OUTER JOIN D ON C.value = D.value;

-- load in some more data

INSERT INTO D VALUES(125, 125);
INSERT INTO D VALUES(126, 126);
INSERT INTO C VALUES(104, 104);

-- join with non empty tables
SELECT * FROM C INNER JOIN D ON C.value = D.value;
SELECT * FROM C LEFT OUTER JOIN D ON C.value = D.value;
SELECT * FROM C RIGHT OUTER JOIN D ON C.value = D.value;
SELECT * FROM C FULL OUTER JOIN D ON C.value = D.value;
