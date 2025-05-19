--Scope of Locks

| Lock Scope     | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| Row-level      | Locks a single row. Very fine-grained; used for high concurrency.           |
| Page-level     | Locks an 8KB page (may include multiple rows).                              |
| Table-level    | Locks the entire table. Least concurrent; used when many rows are affected. |
| Key-level      | Applied in indexes to lock a specific key range.                            |
| Extent-level   | Locks 8 contiguous pages (rare).                                            |
| Database-level | Rare, only during operations like backups or maintenance.                   |

--Types of Locks
| Lock Type           | Description                                                               |
| ------------------- | ------------------------------------------------------------------------- |
| Shared (S)          | For **read** operations (SELECT). Multiple shared locks can coexist.      |
| Exclusive (X)       | For **write** operations (INSERT, UPDATE, DELETE). Prevents other access. |
| Update (U)          | Used during updates, to prevent deadlocks (intermediate between S and X). |
| Intent (IS, IX, IU) | Signifies intent to acquire a lower-level lock. Helps lock hierarchy.     |
| Schema              | Acquired when schema changes occur (DDL operations).                      |
| Bulk Update (BU)    | For bulk insert operations.                                               |

--AITFOR Statement (The WAITFOR statement delays execution. Useful for simulating locks or testing.)
BEGIN TRAN
	UPDATE [dbo].[tblEmployee] SET [EmployeeNumber] = 122 WHERE [EmployeeNumber] = 123
	WAITFOR DELAY '00:00:10'
ROLLBACK TRAN


BEGIN TRAN;
UPDATE dbo.tblEmployee
SET EmployeeFirstName = 'BlockingUser'
WHERE EmployeeNumber = 123;

-- Hold the lock for 1 minute
WAITFOR DELAY '00:01:00';

-- COMMIT later (manual or after delay)
UPDATE dbo.tblEmployee
SET EmployeeFirstName = 'BlockedUser'
WHERE EmployeeNumber = 123;

-- Who is blocking whom?
SELECT 
    blocking_session_id AS Blocker,
    session_id AS Blocked,
    wait_type, wait_time, wait_resource
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;


