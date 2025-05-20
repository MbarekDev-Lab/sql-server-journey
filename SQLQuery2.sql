BEGIN TRAN;
UPDATE [dbo].[tblEmployee]  SET [EmployeeNumber] = 122 WHERE [EmployeeNumber] = 123;
WAITFOR DELAY '00:00:10';

rollback tran
