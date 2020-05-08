use movie;

-- DIRTY READS

-- It is necessary to have two DB clients in order to run async transactions.
-- How to run:
--		1.open one connection to the DB and run "EXEC usp_lab4_sgbd_dirty_reads_T1"
--		2.open a second connection to the DB and run "EXEC usp_lab4_sgbd_dirty_reads_T2" 
--			after more than 5 seconds after the first one started but no more than 10 (because it will have already rolled back)
--		3.notice that "STEFAN CEL MARE" is the title of every movie only in the result of the first query.

--TIMELINE:
--T1:t=0----------UPDATE------------t=5------STEFAN CEL MARE is the title of every film now---------t=10---ROLLBACK-------END
--T2:----------------------------------------t=0------------------------SELECT * FROM Filme------------------------------------------t=10--SELECT * FROM Filme---------COMMIT

-- THE PROBLEM CONSISTS OF THE FACT THAT THE SECOND TRANSACTION, BETWEEN t=0 AND t=5 READS UNCOMMITED DATA
-- SOLUTION: SET TRANSACTION ISOLATION LEVEL READ COMMITTED - in order to prevent the query from returning uncommited data.

go

ALTER PROCEDURE usp_lab4_sgbd_dirty_reads_T1
AS
BEGIN

INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'UPDATE');
BEGIN TRANSACTION
UPDATE Filme SET titlu='STEFAN CEL MARE'; -- this is a non-commited update that will be read by T2, which is wrong.
WAITFOR DELAY '00:00:05' -- We execute the second transaction 5 seconds after the first one has started.
WAITFOR DELAY '00:00:05' -- I created another timer to make sure the T2 will read the data changed by T1.
ROLLBACK TRANSACTION
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'ROLLBACK');
END;

go

ALTER PROCEDURE usp_lab4_sgbd_dirty_reads_T2
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'SELECT');
-- SET TRANSACTION ISOLATION LEVEL READ COMMITTED -- problem
SET TRANSACTION ISOLATION LEVEL READ COMMITTED -- solution
BEGIN TRAN
SELECT * FROM Filme
WAITFOR DELAY '00:00:10'
SELECT * FROM Filme
COMMIT TRAN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'COMMIT');
END;


EXEC usp_lab4_sgbd_dirty_reads_T1
EXEC usp_lab4_sgbd_dirty_reads_T2




--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
go
-- NON-REPEATABLE READS

-- It is necessary to have two DB clients in order to run async transactions.
-- How to run:
--		1.open one connection to the DB and run "EXEC usp_lab4_sgbd_non_repeatable_reads_T1"
--		2.open a second connection to the DB and run "EXEC usp_lab4_sgbd_non_repeatable_reads_T2" 
--			after at least 5 seconds after the first one started but no more than 10 (because it will have already commited the changes)
--		3.notice that "1991" is the year of the new Film returned by the first query and "9119" when returned by the second query, after the commit.

--TIMELINE:
--T1:t=0----------INSERT------------t=5----an_aparitie=1991---------------------t=10---UPDATE----an_aparitie=9119-------COMMIT
--T2:------------------------------------------t=0----------SELECT * FROM Filme---------------------------------------------t=10----------SELECT * FROM Filme----------COMMIT

-- THE PROBLEM CONSISTS OF THE FACT THAT THE SECOND TRANSACTION, BETWEEN t=0 AND t=10 READS TWO DIFFERENT VALUES FOR THE SAME RECORD (different from the initial one)
-- SOLUTION: SET TRANSACTION ISOLATION LEVEL READ COMMITTED - in order to prevent the query from returning different data values.

ALTER PROCEDURE usp_lab4_sgbd_non_repeatable_reads_T1
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'INSERT');
INSERT INTO Filme(an_aparitie, titlu, cod_director) VALUES (1991, 'THE BIRDS', 1) -- this is a non-commited insert that will be read by T2, which is wrong.
BEGIN TRAN
WAITFOR DELAY '00:00:05' -- create a 5 second gap to start the second transaction
WAITFOR DELAY '00:00:05'
UPDATE Filme SET an_aparitie=9119 WHERE titlu like 'THE BIRDS';
COMMIT TRAN -- the change will persist in the database; however, the second transaction should be able to read the commited data only from now on
END;
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'COMMIT');
go

ALTER PROCEDURE usp_lab4_sgbd_non_repeatable_reads_T2
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'SELECT');
--SET TRANSACTION ISOLATION LEVEL READ COMMITTED -- problem
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ -- solution
BEGIN TRAN
SELECT * FROM Filme
WAITFOR DELAY '00:00:10'
SELECT * FROM Filme
COMMIT TRAN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'COMMIT');
END;


EXEC usp_lab4_sgbd_non_repeatable_reads_T1
EXEC usp_lab4_sgbd_non_repeatable_reads_T2 -- this should return only the first read value of the Film
SELECT * FROM Filme -- this should return the current value of the inserted and updated Film


go

--select * from Filme
--delete from Filme where titlu like 'THE BIRDS'
--select * from Filme





--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
go
-- PHANTOM READS

-- It is necessary to have two DB clients in order to run async transactions.
-- How to run:
--		1.open one connection to the DB and run "EXEC usp_lab4_sgbd_phantom_reads_T1"
--		2.open a second connection to the DB and run "EXEC usp_lab4_sgbd_phantom_reads_T2" immediately. (NO MORE THAN 5 seconds after the T1 started)
--		3.notice that in the second transaction, the second query returns one more piece of data than is should have, because the T1 has commited changes on the DB.

--TIMELINE:
--T1:t=0------------------------------------------t=5--INSERT NEW RECORD IN THE DB--------COMMIT
--T2:--------------t=0---SELECT * FROM Filme---------------------------------------------------------------------------------t=10--------SELECT * FROM Filme--------COMMIT

-- THE PROBLEM CONSISTS OF THE FACT THAT THE SECOND TRANSACTION READS THE DATA COMMITED FROM AN UNKNOWN TRANSACTION, WHEN IT SHOULD ONLY CONSIDER THE ITS INITIAL DATA.
-- SOLUTION: SET TRANSACTION ISOLATION LEVEL TO SERIALIZABLE - in order to prevent the query from reading additional data that did not exist in the beginning.

ALTER PROCEDURE usp_lab4_sgbd_phantom_reads_T1
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'INSERT');
BEGIN TRAN
WAITFOR DELAY '00:00:05' -- start T2 before the 5 seconds pass
INSERT INTO Filme(an_aparitie,titlu,cod_director) VALUES (2003, 'At the end of the world', 1)
COMMIT TRAN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'COMMIT');
END;

go

ALTER PROCEDURE usp_lab4_sgbd_phantom_reads_T2
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'SELECT');
-- SET TRANSACTION ISOLATION LEVEL REPEATABLE READ -- problem
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE -- solution
BEGIN TRAN
SELECT * FROM Filme
WAITFOR DELAY '00:00:10'
SELECT * FROM Filme
COMMIT TRAN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme', 'COMMIT');
END;


EXEC usp_lab4_sgbd_phantom_reads_T1
EXEC usp_lab4_sgbd_phantom_reads_T2
SELECT * FROM Filme

go
--select * from Filme
--delete from Filme where titlu like 'At the end of the world'
--select * from Filme


--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
go
-- DEADLOCK

-- How to run:
--		1.open one connection to the DB and run "EXEC usp_lab4_sgbd_deadlock_T1"
--		2.open a second connection to the DB and run "usp_lab4_sgbd_deadlock_T2" immediately. (NO MORE THAN 10 seconds after the T1 started)
--		3.notice that in the second transaction, the second query returns one more piece of data than is should have, because the T1 has commited changes on the DB.

--TIMELINE:
--T1:t=0------------------------------------------t=10--TRY ACCESS T2's locked table------------------DEADLOCK----X; ABORT
--T2:----t=0---SELECT * FROM Filme---------------------t=10--TRY ACCESS T2's locked table-------------DEADLOCK--- T2 will win the "contest" because it has higher priority in solution




-- inserted some toy data
--INSERT INTO Director (data_nastere,nume) VALUES (GETDATE(), 'George'); -- id == 23
--SELECT * FROM Director
--INSERT INTO Filme (an_aparitie,titlu,cod_director) VALUES (2020, 'AZI E AZI', 23)
--SELECT * FROM Filme -- cod == 70086
go


ALTER PROCEDURE usp_lab4_sgbd_deadlock_T1
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme Director', 'UPDATE');
BEGIN TRAN
UPDATE Filme SET titlu='deadlock Books Transaction 1' WHERE cod_film=70086;
-- this transaction has exclusively lock on table Books
WAITFOR DELAY '00:00:10'
UPDATE Director SET nume='deadlock Authors Transaction 1' WHERE cod_director=23;
COMMIT TRAN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Filme Director', 'COMMIT');
END;

go

ALTER PROCEDURE usp_lab4_sgbd_deadlock_T2
AS
BEGIN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Director Filme', 'UPDATE');
SET DEADLOCK_PRIORITY HIGH -- solution (ensures that the T2 will always be commited when a deadlock appears between T1 and T2)
-- SET DEADLOCK_PRIORITY LOW
BEGIN TRAN
UPDATE Director SET nume='deadlock Authors Transaction 2' WHERE cod_director=23
-- this transaction has exclusively lock on table Authors
WAITFOR DELAY '00:00:10'
update Filme SET titlu='deadlock Books Transaction 2' WHERE cod_film=70086
COMMIT TRAN
INSERT INTO Logger (ExecutionDate,TableOperation,TypeOperation) VALUES (GETDATE(), 'Director Filme', 'COMMIT');
END;


BEGIN TRY
EXEC usp_lab4_sgbd_deadlock_T1
END TRY
BEGIN CATCH
PRINT 'deadlocked!'
END CATCH
EXEC usp_lab4_sgbd_deadlock_T2


-----------------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM Logger;
