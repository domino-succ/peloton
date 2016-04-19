-- create the test tables
SET ENABLE_MERGEJOIN TO FALSE;
SET ENABLE_HASHJOIN TO FALSE;
SET ENABLE_NESTLOOP TO TRUE;

CREATE TABLE E(id INT, value INT);
CREATE TABLE F(id INT, value INT);

-- join with empty tables
SELECT * FROM E INNER JOIN F ON E.value = F.value;
SELECT * FROM E LEFT OUTER JOIN F ON E.value = F.value;
SELECT * FROM E RIGHT OUTER JOIN F ON E.value = F.value;
SELECT * FROM E FULL OUTER JOIN F ON E.value = F.value;

-- load in the data

INSERT INTO E VALUES(200, 200);
INSERT INTO E VALUES(201, 201);
INSERT INTO E VALUES(202, 202);
INSERT INTO E VALUES(203, 203);
INSERT INTO E VALUES(204, 204);
INSERT INTO E VALUES(205, 205);
INSERT INTO E VALUES(206, 206);
INSERT INTO E VALUES(207, 207);
INSERT INTO E VALUES(208, 208);
INSERT INTO E VALUES(209, 209);

INSERT INTO F VALUES(220, 220);
INSERT INTO F VALUES(221, 221);
INSERT INTO F VALUES(222, 222);
INSERT INTO F VALUES(223, 223);
INSERT INTO F VALUES(224, 224);

-- join with non empty tables
SELECT * FROM E INNER JOIN F ON E.value = F.value;
SELECT * FROM E LEFT OUTER JOIN F ON E.value = F.value;
SELECT * FROM E RIGHT OUTER JOIN F ON E.value = F.value;
SELECT * FROM E FULL OUTER JOIN F ON E.value = F.value;

-- load in some more data

INSERT INTO F VALUES(225, 225);
INSERT INTO F VALUES(226, 226);
INSERT INTO E VALUES(204, 204);

-- join with non empty tables
SELECT * FROM E INNER JOIN F ON E.value = F.value;
SELECT * FROM E LEFT OUTER JOIN F ON E.value = F.value;
SELECT * FROM E RIGHT OUTER JOIN F ON E.value = F.value;
SELECT * FROM E FULL OUTER JOIN F ON E.value = F.value;
