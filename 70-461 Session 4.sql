USE 70-46
GO  

 select * FROM Employees

--2. UNION and UNION all select * from inserted
--union
--select * from deleted
--select convert(char(5),'hi') as Greeting
--union all
--select convert(char(11),'hello there') as GreetingNow
--union all
--select convert(char(11),'bonjour')
--union all
--select convert(char(11),'hi')
--select convert(tinyint, 45) as Mycolumn
--union
--select convert(bigint, 456)
--select 4
--union
--select 'hi there'
----3. Except and Intersect 
--select *, Row_Number() over(order by (select null)) % 3 as ShouldIDelete
----into tblTransactionNew
--from tblTransaction
--delete from tblTransactionNew
--where ShouldIDelete = 1
--update tblTransactionNew
--set DateOfTransaction = dateadd(day,1,DateOfTransaction)
--Where ShouldIDelete = 2
--alter table tblTransactionNew
--drop column ShouldIDelete
--select * from tblTransaction -- 2486 rows
--intersect--except--union--union all
--select * from tblTransactionNew -- 1657 rows, 829 changed rows, 828 unchanged
--order by EmployeeNumber
----4. CASE declare @myOption as varchar(10) = 'Option C'
--select case when @myOption = 'Option A' then 'First option'
-- when @myOption = 'Option B' then 'Second option'
----else 'No Option' 
--END as MyOptions
--go

--declare @myOption as varchar(10) = 'Option A'
--select case @myOption when 'Option A' then 'First option'
-- when 'Option B' then 'Second option'
-- else 'No Option' END as MyOptions
--go
-- case when left(EmployeeGovernmentID,1)='A' then 'Letter A'
-- when EmployeeNumber<200 then 'Less than 200'
-- else 'Neither letter' END + '.' as myCol
-- FROM tblEmployee
----5. Isnull and Coalesce 
--select * from tblEmployee where EmployeeMiddleName is null
--declare @myOption as varchar(10) = 'Option B'
--select isnull(@myOption, 'No Option') as MyOptions
--go
--declare @myFirstOption as varchar(10) --= 'Option A'
--declare @mySecondOption as varchar(10) --= 'Option B'
--select coalesce(@myFirstOption, @mySecondOption, 'No option') as MyOptions
--go
--select isnull('ABC',1) as MyAnswer
--select coalesce('ABC',1) as MyOtherAnswer
--go
--select isnull(null,null) as MyAnswer
--select coalesce(null,null) as MyOtherAnswer
--go
--create table tblExample
--(myOption nvarchar(10) null)
--go
--insert into tblExample (myOption)
--values ('Option A')
--select coalesce(myOption, 'No option') as MyOptions
--into tblIsCoalesce
--from tblExample 
--select case when myOption is not null then myOption else 'No option' end as myOptions 
--from tblExample
--go
--select isnull(myOption, 'No option') as MyOptions
--into tblIsNull
--from tblExample 
--go

--drop table tblExample
--drop table tblIsCoalesce
--drop table tblIsNull
--# 7. MERGE statement 

BEGIN TRAN
ALTER TABLE tblTransaction 
ADD Comments varchar(50) NULL
GO
--SELECT * FROM tblTransaction
	--MERGE INTO tblTransaction as T
	MERGE TOP(5) PERCENT INTO tblTransaction as T
	USING (SELECT DateOfTransaction, EmployeeNumber , SUM(Amount) AS TotalAmount
			FROM tblTransactionNew 
			GROUP BY  DateOfTransaction, EmployeeNumber ) AS S
	ON T.EmployeeNumber = S.EmployeeNumber AND T.DateOfTransaction = S.DateOfTransaction
	--WHEN MATCHED AND T.EmployeeNumber > 200 THEN
	WHEN MATCHED AND  T.Amount + S.TotalAmount  > 0  THEN
		UPDATE SET Amount = T.Amount + S.TotalAmount, Comments = 'Updated Row'
	WHEN MATCHED THEN 
		DELETE 
	WHEN NOT MATCHED BY TARGET THEN
		 INSERT ([Amount], [DateOfTransaction], [EmployeeNumber] , Comments)
		 VALUES (S.TotalAmount, S.DateOfTransaction, S.EmployeeNumber, 'Inserted Row')
	WHEN NOT MATCHED BY SOURCE THEN
		 UPDATE SET Comments = 'Unchanged'
	 OUTPUT inserted.*, deleted.*, $action;
	--Select * from tblTransactionNew
	--SELECT * FROM tblTransactionNew ORDER BY EmployeeNumber, DateOfTransaction
ROLLBACK TRAN

-- tblTransaction (no) - tblTransactionNew (yes)
-- 1 tblTransaction - 1 tblTransactionNew
-- 1 tblTransaction - multiple rows TblTransactionNew+
-- if there are problems with triggers we should disable them using 
-- DISABLE TRIGGER [TR_tblTransaction] ON [dbo].[tblTransaction]
select   EmployeeNumber, DateOfTransaction from tblTransactionNew

SELECT DateOfTransaction, EmployeeNumber , SUM(Amount) AS TotalAmount
FROM tblTransactionNew 
GROUP BY  DateOfTransaction, EmployeeNumber 
HAVING COUNT(*)>1

BEGIN TRAN
go
DISABLE TRIGGER TR_tblTransaction ON dbo.tblTransaction
GO
MERGE INTO tblTransaction as T
USING (SELECT DateOfTransaction, EmployeeNumber, MIN(Amount) as Amount
 FROM tblTransactionNew
 GROUP BY DateOfTransaction, EmployeeNumber) as S
ON T.EmployeeNumber = S.EmployeeNumber AND
T.DateOfTransaction = S.DateOfTransaction
WHEN MATCHED THEN
 UPDATE SET Amount = T.Amount + S.Amount
WHEN NOT MATCHED THEN
INSERT (Amount, DateOfTransaction, EmployeeNumber)
VALUES (S.Amount, S.DateOfTransaction, S.EmployeeNumber)
OUTPUT deleted.*, inserted.*;
ROLLBACK TRAN
--9. Merge with additional column 
BEGIN TRAN
ALTER TABLE tblTransaction
ADD Comments varchar(50) NULL
GO -- DDL
MERGE TOP (5) PERCENT INTO tblTransaction as T --DML
    USING (select EmployeeNumber, DateOfTransaction, sum(Amount) as Amount
from tblTransactionNew
group by EmployeeNumber, DateOfTransaction) as S
ON T.EmployeeNumber = S.EmployeeNumber AND T.DateOfTransaction = S.DateOfTransaction
WHEN MATCHED AND T.Amount + S.Amount >0 THEN
 UPDATE SET Amount = T.Amount + S.Amount, Comments = 'Updated Row'
 WHEN MATCHED THEN
DELETE
WHEN NOT MATCHED BY TARGET THEN
 INSERT ([Amount], [DateOfTransaction], [EmployeeNumber], Comments)
VALUES (S.Amount, S.DateOfTransaction, S.EmployeeNumber, 'Inserted Row')
WHEN NOT MATCHED BY SOURCE THEN
UPDATE SET Comments = 'Unchanged'
OUTPUT inserted.*, deleted.* , $action;
--Select * from tblTransaction ORDER BY EmployeeNumber, DateOfTransaction
ROLLBACK TRAN


--11: creating procedure 
CREATE PROCEDURE NameEmployees (
	BEGIN 
			SELECT EmployeeNumber, EmployeeFirstName, EmployeeLastName
			FROM tblEmployee
	END
)

CREATE PROC NameEmployees as
	begin
		select EmployeeNumber, EmployeeFirstName, EmployeeLastName
		from tblEmployee
	end
go

NameEmployees

execute NameEmployees
exec NameEmployees
EXEC NameEmployees

go

--12. Ask for a specific employee 
exec NameEmployees

--select * from sys.views where name='ViewByDepartment'
if exists (select * from sys.procedures where name = 'NameEmployees')
if exists (select * from sys.procedures)
select * from sys.procedures
go

SELECT	OBJECT_ID('NameEmployees','P')
if OBJECT_ID('NameEmployees','P') IS NOT NULL
DROP PROC NameEmployees
go
--EXEC NameEmployees

CREATE PROC NameEmployees(@EmployeeNumber int) as
	begin
		if exists (SELECT * from tblEmployee where EmployeeNumber = @EmployeeNumber)
		begin
			select EmployeeNumber, EmployeeFirstName, EmployeeLastName
			from tblEmployee
			where EmployeeNumber = @EmployeeNumber
		end
	end
go

NameEmployees 4
EXEC NameEmployees 4

execute NameEmployees 223
exec NameEmployees 323
select EmployeeNumber from NameEmployees

DECLARE @EmployeeName int = 123
select @EmployeeName

exec NameEmployees 4

select * from tblEmployee where NameEmployees = 223

-- 13. Different outcomes 
--if exists (select * from sys.procedures where name='NameEmployees')
if object_ID('NameEmployees','U') IS NOT NULL
drop proc NameEmployees
go

--exec NameEmployees Could not find stored procedure 'NameEmployees'.
create proc NameEmployees(@EmployeeNumber int) as
begin
	if exists (Select * from tblEmployee where EmployeeNumber = @EmployeeNumber)
	begin
		if @EmployeeNumber < 300
		begin
			select EmployeeNumber, EmployeeFirstName, EmployeeLastName
			from tblEmployee
			where EmployeeNumber = @EmployeeNumber
		end
		else
		begin
			select EmployeeNumber, EmployeeFirstName, EmployeeLastName, Department
			from tblEmployee
			where EmployeeNumber = @EmployeeNumber
			select * from tblTransaction where EmployeeNumber = @EmployeeNumber
		end
	end
end
go
NameEmployees 4
execute NameEmployees 223
exec NameEmployees 324


SELECT [Amount],[DateOfTransaction], [EmployeeNumber] FROM [dbo].[tblTransaction] sum ([Amount] ) 
	UNION 
SELECT [Amount],[DateOfTransaction],[EmployeeNumber]  FROM [dbo].[tblTransactionNew]


--14. Ask for a range of employees 
--if exists (select * from sys.procedures where name='NameEmployees')
if object_ID('NameEmployees','P') IS NOT NULL
drop proc NameEmployees
go
create proc NameEmployees(@EmployeeNumberFrom int, @EmployeeNumberTo int) as
begin
	if exists (Select * from tblEmployee where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo)
	begin
		select EmployeeNumber, EmployeeFirstName, EmployeeLastName
		from tblEmployee
		where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
	end
end
go
NameEmployees 4, 5
execute NameEmployees 223, 227
exec NameEmployees @EmployeeNumberFrom = 323, @EmployeeNumberTo = 327


--15. A different SELECT statement per employee 
--if exists (select * from sys.procedures where name='NameEmployees')

IF object_ID('NameEmployees','P') IS NOT NULL
drop proc NameEmployees
GO
CREATE PROC NameEmployees
					@EmployeeNumberFrom int,
					@EmployeeNumberTo int
							AS
BEGIN
	IF exists (Select * from tblEmployee where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo)
	BEGIN
		declare @EmployeeNumber int = @EmployeeNumberFrom
		while @EmployeeNumber <= @EmployeeNumberTo
		BEGIN
			if not exists (Select * from tblEmployee where EmployeeNumber = @EmployeeNumber)
			BEGIN
				SET @EmployeeNumber =+1
				CONTINUE
			END
			select EmployeeNumber, EmployeeFirstName, EmployeeLastName
			from tblEmployee
			where EmployeeNumber = @EmployeeNumber
			set @EmployeeNumber = @EmployeeNumber + 1
		END
	end
end
go

	DECLARE @Total int ,
	DECLARE @result VARCHAR(30) = 227 output
	EXEC @Total = NameEmployees 223, @result OUTPUT  
	SELECT @result AS resultNr 



	

	--NameEmployees 4, 5
	--execute NameEmployees 223, 227
	--exec NameEmployees @EmployeeNumberFrom = 323, @EmployeeNumberTo = 327


--16. Returning values --if exists (select * from sys.procedures where name='NameEmployees')
if object_ID('AverageBalance','P') IS NOT NULL
drop proc AverageBalance
go
	create proc AverageBalance(@EmployeeNumberFrom int, @EmployeeNumberTo int, @AverageBalance int OUTPUT) as
begin
	--if exists (Select * from tblEmployee where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo)
		begin
				declare @TotalAmount money
				declare @NumberOfEmployee int
				select @TotalAmount = SUM(Amount) from tblTransaction
				where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
				--select EmployeeNumber, EmployeeFirstName, EmployeeLastName
				--from tblEmployee
				select @NumberOfEmployee = COUNT(distinct EmployeeNumber) from tblEmployee
				where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
				if @NumberOfEmployee = 0
					set @AverageBalance = 0
				else
					set @AverageBalance = @TotalAmount / @NumberOfEmployee

				SET @AverageBalance = @@ROWCOUNT
				SET @AverageBalance = @TotalAmount / @NumberOfEmployee
			--RETURN 0
		end
end
GO
	DECLARE @AvrgBalance int, @ReturnStatus int
	EXEC @ReturnStatus = AverageBalance 4, 5, @AvrgBalance OUTPUT
	select @AvrgBalance as Average_Balance, @ReturnStatus as Return_Status
GO
	DECLARE @AvrgBalance int, @ReturnStatus int
	execute @ReturnStatus = AverageBalance 223, 227, @AvrgBalance OUTPUT
	select @AvrgBalance as MyRowCount, @ReturnStatus as Return_Status
GO
	DECLARE @AvrgBalance int, @ReturnStatus int
	EXEC  @ReturnStatus = AverageBalance @EmployeeNumberFrom = 323, @EmployeeNumberTo = 327, @AverageBalance = @AvrgBalance OUTPUT
	select @AvrgBalance as MyRowCount, @ReturnStatus as Return_Status
GO
	DECLARE @AvrgBalance int
	EXEC AverageBalance 223 , 327, @AvrgBalance OUTPUT
	select  @AvrgBalance as MyRowCount

	select * from tblTransaction
	where EmployeeNumber between 100 and 300
	order by EmployeeNumber

	select SUM(EmployeeNumber) from tblTransaction
	where EmployeeNumber between 100 and 300
	order by EmployeeNumber

	select  COUNT(EmployeeNumber) from tblTransaction
	where EmployeeNumber between 100 and 300

	select  COUNT(distinct EmployeeNumber) from tblTransaction
	where EmployeeNumber between 100 and 300

	select COUNT(distinct EmployeeNumber) from tblEmployee
	where EmployeeNumber between 3 and 11

	--drop table tblTransaction

--19. Try � Catch 
--if exists (select * from sys.procedures where name='AverageBalance')
if object_ID('AverageBalance','P') IS NOT NULL
drop proc AverageBalance
go
create proc AverageBalance(@EmployeeNumberFrom int, @EmployeeNumberTo int, @AverageBalance int OUTPUT) as
begin
	SET NOCOUNT ON
	declare @TotalAmount money
	declare @NumOfEmployee int
	begin try
		select @TotalAmount = sum(Amount) from tblTransaction
		where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
		select @NumOfEmployee = count(distinct EmployeeNumber) from tblEmployee
		where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
		set @AverageBalance = @TotalAmount / @NumOfEmployee
		RETURN 0
	end try
	begin catch
		set @AverageBalance = 0
		SELECT ERROR_MESSAGE() AS ErrorMessage, ERROR_LINE() as ErrorLine,
		 ERROR_NUMBER() as ErrorNumber, ERROR_PROCEDURE() as ErrorProcedure,
		 ERROR_SEVERITY() as ErrorSeverity, -- 0-10 for information
		 -- 16 default SQL SERVER log / Windows Application log
		 -- 20-25 very bad must close the connection and DB
		 ERROR_STATE() as ErrorState
		RETURN 1
	end catch
end
GO
DECLARE @AvgBalance int, @ReturnStatus int
EXEC @ReturnStatus = AverageBalance 4, 5, @AvgBalance OUTPUT
select @AvgBalance as Average_Balance, @ReturnStatus as Return_Status
select @ReturnStatus as result
GO
DECLARE @AvgBalance int, @ReturnStatus int
execute @ReturnStatus = AverageBalance 223, 227, @AvgBalance OUTPUT
select @AvgBalance as Average_Balance, @ReturnStatus as Return_Status
GO
DECLARE @AvgBalance int, @ReturnStatus int
exec @ReturnStatus = AverageBalance @EmployeeNumberFrom = 323, @EmployeeNumberTo = 327, @AverageBalance = @AvgBalance OUTPUT
select @AvgBalance as Average_Balance, @ReturnStatus as Return_Status

SELECT TRY_CONVERT(int, 'two')
 --21. Print 
--if exists (select * from sys.procedures where name='AverageBalance')
if object_ID('AverageBalance','P') IS NOT NULL
drop proc AverageBalance
go
create proc AverageBalance(@EmployeeNumberFrom int, @EmployeeNumberTo int, @AverageBalance int OUTPUT) as
begin
		SET NOCOUNT ON
		declare @TotalAmount decimal(5,2)
		declare @NumOfEmployee int
		begin try
			print 'The employee numbers are from ' + convert(varchar(10),@EmployeeNumberFrom) + ' to ' + convert(varchar(10),@EmployeeNumberTo)
			select @TotalAmount = sum(Amount) from tblTransaction
			where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
			select @NumOfEmployee = count(distinct EmployeeNumber) from tblEmployee
			where EmployeeNumber between @EmployeeNumberFrom and @EmployeeNumberTo
			set @AverageBalance = @TotalAmount / @NumOfEmployee
			RETURN 0
		end try
		begin catch
			set @AverageBalance = 0
			if ERROR_NUMBER() = 8134 -- @@ERROR
			begin
				set @AverageBalance = 0
				print 'There are no valid employees in this range.'
				Return 8134
			end
			else
				declare @ErrorMessage as varchar(255)
				select @ErrorMessage = error_Message()
				raiserror (@ErrorMessage, 16, 1)
				--throw 56789, 'Too many flanges', 1
				-- PRINT ERROR_MESSAGE() AS ErrorMessage, ERROR_LINE() as ErrorLine, 
				-- ERROR_NUMBER() as ErrorNumber, ERROR_PROCEDURE() as ErrorProcedure, 
				 --ERROR_SEVERITY() as ErrorSeverity, -- 0-10 for information
				 -- 16 default SQL SERVER log / Windows Application log
 				 -- 20-25 
				-- ERROR_STATE() as ErrorState
				--RETURN 1
				select 'Hi There'
		end catch
end
--testing
go
DECLARE @AvgBalance int, @ReturnStatus int
EXEC @ReturnStatus = AverageBalance 4, 5, @AvgBalance OUTPUT
select @AvgBalance as Average_Balance, @ReturnStatus as Return_Status 
GO
DECLARE @AvgBalance int, @ReturnStatus int
execute @ReturnStatus = AverageBalance 223, 227, @AvgBalance OUTPUT
select @AvgBalance as Average_Balance, @ReturnStatus as Return_Status, 'Error did not stop us' as myMessage
GO
DECLARE @AvgBalance int, @ReturnStatus int
exec @ReturnStatus = AverageBalance @EmployeeNumberFrom = 323, @EmployeeNumberTo = 327, @AverageBalance = @AvgBalance OUTPUT
select @AvgBalance as Average_Balance, @ReturnStatus as Return_Status


CREATE PROCEDURE GetEmployeeSalary
    @EmployeeID INT,               -- Input parameter
    @Salary DECIMAL(10, 2) OUTPUT  -- Output parameter
AS
BEGIN
    -- Get the employee's salary and assign it to the output parameter
    SELECT @Salary = Salary
    FROM Employees
    WHERE EmployeeID = @EmployeeID;
END;
DECLARE @EmployeeSalary DECIMAL(10, 2);  -- Declare a variable to hold the output

-- Execute the stored procedure
EXEC GetEmployeeSalary @EmployeeID = 123, @Salary = @EmployeeSalary OUTPUT;

-- Now @EmployeeSalary contains the employee's salary
PRINT @EmployeeSalary;


CREATE TABLE EmployeesScoup(
    EmployeeID INT IDENTITY(1,1),
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50));

INSERT INTO EmployeesScoup (FirstName, LastName)
VALUES ('Toya', 'Smith'),
 ('www', 'lkmd'),
 ('dsk v', 'dslv'),
 ('p', 'pp'),
 ('Toya', 'Smith') SELECT SCOPE_IDENTITY() AS scopeIdentity;

select * from EmployeesScoup
SELECT EmployeeID, FirstName, LastName ,
       IF(EmployeeID = 6 ,'Toya', 'NOname') AS userName
FROM EmployeesScoup;

-- Retrieve the last generated identity value within this scope
SELECT SCOPE_IDENTITY();
SELECT SCOPE_IDENTITY() AS ScopeIdentityValue,
       @@IDENTITY AS AtAtIdentityValue,        
       IDENT_CURRENT('EmployeesScoup') AS IdentCurrentValue; 
GO  
CREATE TABLE TZ (  
   Z_id  INT IDENTITY(1,1)PRIMARY KEY,  
   Z_name VARCHAR(20) NOT NULL);  
  
INSERT TZ  
   VALUES ('Lisa'),('Mike'),('Carla');  
  
SELECT * FROM TZ;

CREATE TABLE TY (  
   Y_id  INT IDENTITY(100,5)PRIMARY KEY,  
   Y_name VARCHAR(20) NULL);  
  
INSERT TY (Y_name)  
   VALUES ('boathouse'), ('rocks'), ('elevator')select SCOPE_IDENTITY() as result;
  
SELECT * FROM TY;
go 


DECLARE @sql NVARCHAR(1000);
DECLARE @EmpID INT = 101;
DECLARE @Y_id INT = 110;
SET @sql = 'SELECT * FROM TY WHERE Y_id = @Y_id';
EXEC sp_executesql @sql, N'@Y_id INT', @EmpID;
go


begin
DECLARE @sql NVARCHAR(1000);
SET @sql = 'SELECT * FROM Employees WHERE EmployeeID = 101';
EXEC(@sql);
end
go

	begin
		DECLARE @sql2 NVARCHAR(1000);
		DECLARE @EmpID INT = 101;
		SET @sql2 = 'SELECT * FROM Employees WHERE EmployeeID = @EmployeeID';
		EXEC sp_executesql @sql, N'@EmployeeID INT', @EmpID;
	end
go 

-- using the executesql to execute the proc ipmlementation 

CREATE TABLE Employees(
    EmployeeID INT PRIMARY KEY,          -- Unique identifier for each employee
    DepartmentID INT,                    -- ID of the department the employee belongs to
    Name NVARCHAR(50),                   -- Name of the employee
    Salary DECIMAL(10, 2)                -- Salary of the employee (decimal format for currency)
);
go

-- Inserting sample data into Employees table
INSERT INTO Employees (EmployeeID, DepartmentID, Name, Salary)
VALUES 
	(101, 1, 'John Doe', 5500.00),
	(102, 1, 'Jane Smith', 6200.00),
	(103, 2, 'Mark Johnson', 4800.00),
	(104, 2, 'Emily Davis', 7000.00);
go

CREATE PROCEDURE GetEmployeeDetails2
    @EmployeeID INT,
    @DepartmentID INT,
    @EmployeeName NVARCHAR(50) OUTPUT,
    @Salary DECIMAL(10, 2) OUTPUT
AS
BEGIN
    SELECT @EmployeeName = Name, @Salary = Salary
    FROM Employees
    WHERE EmployeeID = @EmployeeID AND DepartmentID = @DepartmentID;
    RETURN 0;
END;
GO

-- select * from Employees
-- execution of the proc GetEmployeeDetails2
BEGIN 

	DECLARE @EmployeeID INT = 101;
	DECLARE @DepartmentID INT = 1;
	DECLARE @EmployeeName NVARCHAR(50);
	DECLARE @Salary DECIMAL(10, 2);
	-- SQL string to execute the stored procedure
	DECLARE @sql NVARCHAR(1000);
	SET @sql = 'EXEC GetEmployeeDetails2 @EmployeeID, @DepartmentID 
	, @EmployeeName OUTPUT , @Salary OUTPUT';
	-- Execute the SQL with sp_executesql and pass parameters
	EXEC sys.sp_executesql @sql, N'@EmployeeID INT, @DepartmentID INT, @EmployeeName NVARCHAR(50) OUTPUT
		, @Salary DECIMAL(10,2) OUTPUT',
		@EmployeeID, @DepartmentID, @EmployeeName OUTPUT, @Salary OUTPUT;

	-- Display the output values
	SELECT @EmployeeName AS EmployeeName, @Salary AS Salary;
END
GO

BEGIN TRANSACTION;      -- @@TRANCOUNT = 1
BEGIN TRANSACTION;      -- @@TRANCOUNT = 2
SELECT @@TRANCOUNT;     -- Returns 2
COMMIT TRANSACTION;     -- @@TRANCOUNT = 1 (only commits the last transaction)
SELECT @@TRANCOUNT;     -- Returns 1
ROLLBACK TRANSACTION;   -- @@TRANCOUNT = 0 (rolls back the entire transaction chain)
SELECT @@TRANCOUNT;     -- Returns 0
GO

DECLARE @MyTableVariable TABLE (
    nCount INT
);

-- Insert data into the table variable
INSERT INTO @MyTableVariable (nCount)
VALUES (1), (2), (3);

-- Select from the table variable
SELECT * 
FROM  tblTransactionNew
WHERE [DateOfTransaction] = '2014-04-07'


SELECT * 
FROM tblTransactionNew 
WHERE CAST([DateOfTransaction] AS DATE) = '2014-04-07';

SELECT * 
FROM tblTransactionNew 
WHERE CONVERT(VARCHAR, [DateOfTransaction], 120) LIKE '2014-04-07%';

