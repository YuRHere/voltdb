CREATE TABLE HELLOWORLD (
   HELLO VARCHAR(15),
   WORLD VARCHAR(15),
   DIALECT VARCHAR(15) NOT NULL,
   PRIMARY KEY (DIALECT)
);
PARTITION TABLE HELLOWORLD ON COLUMN DIALECT;

CREATE PROCEDURE Insert PARTITION ON TABLE Helloworld COLUMN Dialect 
   AS INSERT INTO HELLOWORLD (Dialect, Hello, World) VALUES (?, ?, ?);
CREATE PROCEDURE Select PARTITION ON TABLE Helloworld COLUMN Dialect 
   AS SELECT HELLO, WORLD FROM HELLOWORLD WHERE DIALECT = ?;
