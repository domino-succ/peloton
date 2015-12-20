-- create the test tables
DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

create table A(id INT PRIMARY KEY, data TEXT);
create table B(id INT PRIMARY KEY, data TEXT);

-- load in the data
INSERT INTO A VALUES(0, 'Ming says Hello World 0');
INSERT INTO B VALUES(0, 'Joy says World Hello 0');


-- nested loop join left outer join
SELECT A.* FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT B.* FROM B LEFT OUTER JOIN A ON A.id != B.id;
SELECT A.id, B.id FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT A.id, B.data FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT A.id, A.id, B.data, B.data FROM A LEFT OUTER JOIN B ON A.id != B.id;
SELECT A.*, B.* FROM A LEFT OUTER JOIN B ON A.id != B.id;

-- nested loop join with right outer join
SELECT A.* FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT B.* FROM B RIGHT OUTER JOIN A ON A.id != B.id;
SELECT A.id, B.id FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT A.id, B.data FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT A.id, A.id, B.data, B.data FROM A RIGHT OUTER JOIN B ON A.id != B.id;
SELECT A.*, B.* FROM A RIGHT OUTER JOIN B ON A.id != B.id;


-- load more data
INSERT INTO A VALUES(1, 'Ming says Hello World 1');
INSERT INTO A VALUES(2, 'Ming says Hello World 2');
INSERT INTO A VALUES(3, 'Ming says Hello World 3');
INSERT INTO B VALUES(1, 'Joy says World Hello 1');
INSERT INTO B VALUES(2, 'Joy says World Hello 2');
INSERT INTO B VALUES(3, 'Joy says World Hello 3');
INSERT INTO B VALUES(4, 'Joy says World Hello 4');
INSERT INTO B VALUES(5, 'Joy says World Hello 5');
INSERT INTO B VALUES(6, 'Joy says World Hello 6');
INSERT INTO B VALUES(7, 'Joy says World Hello 7');


-- nested loop join
SELECT * FROM A,B;
SELECT * FROM B,A;

-- nested loop join with predicates
SELECT * FROM A,B WHERE A.id > 2;
SELECT * FROM A,B WHERE A.id > 2 AND B.id = 6;
SELECT * FROM A,B WHERE A.id > 2 AND B.id >= 6;
SELECT * FROM B,A WHERE A.id > 2 AND B.id = 6;


-- nested loop join with projection
SELECT A.* FROM A,B;
SELECT B.* FROM B,A;
SELECT A.id, B.id FROM A,B;
SELECT A.id, B.data FROM A,B;
SELECT A.id, A.id, B.data, B.data FROM A,B;
SELECT A.*, B.* FROM A,B;

-- nested loop join with projection and predicates
SELECT A.* FROM A,B WHERE A.id > 2;
SELECT B.* FROM B,A WHERE B.id = 3;
SELECT A.id, B.id FROM A,B WHERE A.id <= 2;
SELECT A.id, B.data FROM A,B WHERE A.id = 1 AND B.id > 4;
SELECT A.id, A.id, B.data, B.data FROM A,B WHERE A.id > 2;
SELECT A.*, B.* FROM A,B WHERE A.id > 2;



