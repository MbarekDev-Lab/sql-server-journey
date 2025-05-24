USE [70-461];

SELECT * FROM tblEmployee;
SELECT * FROM tblAttendance;

select t1.Department , t2.EmployeeNumber, t2.AttendanceMonth, t2.NumberAttendance
From tblEmployee AS t1 
JOIN tblAttendance AS t2 
ON t1.EmployeeNumber =  t2.EmployeeNumber;

--Only employees that have attendance records will appear.
SELECT *
FROM tblEmployee AS e
INNER JOIN tblAttendance AS a
ON e.EmployeeNumber = a.EmployeeNumber;

--All employees, even if they have no attendance.
--Attendance columns will be NULL if missing
SELECT distinct * 
FROM tblEmployee AS e
LEFT JOIN tblAttendance AS a
ON e.EmployeeNumber = a.EmployeeNumber
WHERE e.EmployeeNumber = 165;

--Pick the latest AttendanceMonth
SELECT e.EmployeeNumber,e.EmployeeFirstName,e.EmployeeLastName, 
MAX(a.AttendanceMonth) AS LatestAttendanceMonth
FROM tblEmployee AS e LEFT JOIN tblAttendance AS a ON e.EmployeeNumber = a.EmployeeNumber
GROUP BY e.EmployeeNumber, e.EmployeeFirstName, e.EmployeeLastName;

--Row per employee to collapse duplicates(no duplication)
SELECT e.EmployeeNumber,e.EmployeeFirstName,e.EmployeeLastName,
    COUNT(a.AttendanceMonth) AS TotalAttendanceRecords
FROM tblEmployee AS e
LEFT JOIN tblAttendance AS a ON e.EmployeeNumber = a.EmployeeNumber
GROUP BY e.EmployeeNumber, e.EmployeeFirstName, e.EmployeeLastName;

-- 36 rows attendances records per employee (LEFT JOIN does not "collapse" duplicates)
SELECT  EmployeeNumber,  COUNT(*) AS AttendanceRecords
FROM tblAttendance
GROUP BY EmployeeNumber
ORDER BY AttendanceRecords desc;

-- EXISTS could be the perfect replacement instead of using GROUP BY 
-- when we just want to check the existence of a match
SELECT e.EmployeeNumber, e.EmployeeFirstName, e.EmployeeLastName
FROM tblEmployee e
WHERE EXISTS (
    SELECT 1
    FROM tblAttendance a
    WHERE a.EmployeeNumber = e.EmployeeNumber
);

-- OVER 
SELECT e.EmployeeNumber,
  YEAR(AttendanceMonth) AS AttendanceYear, 
  SUM(a.NumberAttendance) OVER() AS GrandTotalAttendance
FROM tblEmployee AS e JOIN tblAttendance AS a ON e.EmployeeNumber = a.EmployeeNumber
--GROUP BY e.EmployeeNumber,  YEAR(AttendanceMonth);

SELECT e.EmployeeNumber,
  YEAR(AttendanceMonth) AS AttendanceYear, 
  SUM(a.NumberAttendance)  AS GrandTotalAttendance
FROM tblEmployee AS e JOIN tblAttendance AS a ON e.EmployeeNumber = a.EmployeeNumber
GROUP BY e.EmployeeNumber,  YEAR(AttendanceMonth);


 -- GROUP BY vs OVER()
SELECT EmployeeNumber, SUM(NumberAttendance) AS  TotalCompanyAttendance
FROM  tblAttendance
GROUP BY  EmployeeNumber;

SELECT EmployeeNumber, AttendanceMonth,NumberAttendance,
  SUM(NumberAttendance) OVER(PARTITION BY EmployeeNumber) AS TotalCompanyAttendance
FROM tblAttendance;

SELECT EmployeeNumber, AttendanceMonth,NumberAttendance,
  SUM(NumberAttendance) OVER() AS TotalCompanyAttendance
FROM tblAttendance;

select A.EmployeeNumber, A.AttendanceMonth, A.NumberAttendance,sum(A.NumberAttendance) over() as TotalAttendance,
convert(decimal(18,7),A.NumberAttendance) / sum(A.NumberAttendance) over() * 100.0000 as PercentageAttendance
from tblEmployee as E join tblAttendance as A
on E.EmployeeNumber = A.EmployeeNumber

-- 	SUM over each group separately vs SUM over all rows
SELECT sum(NumberAttendance) FROM tblAttendance;
SELECT SUM(NumberAttendance) OVER() AS TotalAttendances
FROM tblAttendance;

SELECT EmployeeNumber, AttendanceMonth, NumberAttendance,
ROW_NUMBER() OVER(ORDER BY AttendanceMonth) AS RowNum
FROM tblAttendance;

--mixing window functions after GROUP BY,
--like in a CTE (Common Table Expression)
WITH AttendancePerEmployee AS (
    SELECT EmployeeNumber, YEAR(AttendanceMonth) AS Year,
        SUM(NumberAttendance) AS TotalAttendance
    FROM tblAttendance
    GROUP BY EmployeeNumber, YEAR(AttendanceMonth)
)
SELECT EmployeeNumber, Year, TotalAttendance,
    SUM(TotalAttendance) OVER() AS GrandTotal 
FROM AttendancePerEmployee;

SELECT A.EmployeeNumber, A.AttendanceMonth, A.NumberAttendance,
SUM(A.NumberAttendance) OVER(PARTITION BY A.EmployeeNumber, YEAR(AttendanceMonth) ORDER BY A.AttendanceMonth DESC) AS RuningTotal
--,CONVERT(decimal(18,7),A.NumberAttendance) / SUM(A.NumberAttendance) OVER(PARTITION BY A.EmployeeNumber) * 100.0000 AS PercentageAttendance
FROM tblEmployee AS E JOIN tblAttendance AS A 
ON E.EmployeeNumber = A.EmployeeNumber
--WHERE A.AttendanceMonth < '20150101';
--WHERE year(A.AttendanceMonth) = '2014';

--Partition by and Order by
select A.EmployeeNumber, A.AttendanceMonth, A.NumberAttendance, 
sum(A.NumberAttendance) over(PARTITION BY E.EmployeeNumber) as SumAttendance,
convert(money,A.NumberAttendance) / sum(A.NumberAttendance) over(PARTITION BY E.EmployeeNumber) * 100 as PercentageAttendance
from tblEmployee as E join tblAttendance as A
on E.EmployeeNumber = A.EmployeeNumber
WHERE A.AttendanceMonth < '20150101';

-- Range
select A.EmployeeNumber, A.AttendanceMonth, A.NumberAttendance, 
SUM(A.NumberAttendance) over(PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth 
ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as RollingTotal
from tblEmployee as E join tblAttendance as A
on  E.EmployeeNumber =  A.EmployeeNumber

--Current Row and Unbounded
SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth, 
  A.NumberAttendance, 
  SUM(A.NumberAttendance) OVER(
    PARTITION BY E.EmployeeNumber 
    ORDER BY A.AttendanceMonth 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS RollingTotal
FROM tblEmployee AS E 
JOIN tblAttendance AS A
ON E.EmployeeNumber = A.EmployeeNumber;

CREATE TABLE tblSales (
    SaleID INT PRIMARY KEY IDENTITY(1,1),
    EmployeeNumber INT NOT NULL,          
    SaleDate DATE NOT NULL,               
    SaleAmount DECIMAL(10, 2) NOT NULL     
);
INSERT INTO tblSales (EmployeeNumber, SaleDate, SaleAmount)
VALUES
    (101, '2024-01-10', 500.00),
    (101, '2024-02-05', 600.00),
    (101, '2024-03-15', 300.00),
    (102, '2024-01-12', 400.00),
    (102, '2024-02-20', 700.00),
    (103, '2024-01-20', 200.00);

--Query for Cumulative Sales Per Employee, per Year
SELECT 
  s.EmployeeNumber,
  YEAR(s.SaleDate) AS SaleYear,
  s.SaleDate,
  s.SaleAmount,
  SUM(s.SaleAmount) OVER (
    PARTITION BY s.EmployeeNumber, YEAR(s.SaleDate) 
    ORDER BY s.SaleDate 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS CumulativeSales
FROM tblSales AS s
ORDER BY s.EmployeeNumber, s.SaleDate;

-- Range versus Rows
SELECT 
    A.EmployeeNumber, 
    A.AttendanceMonth, 
    A.NumberAttendance,
    SUM(A.NumberAttendance) 
    OVER (
        PARTITION BY A.EmployeeNumber, YEAR(A.AttendanceMonth) 
        ORDER BY A.AttendanceMonth 
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS RowsTotal,

    SUM(A.NumberAttendance) 
    OVER (
        PARTITION BY A.EmployeeNumber, YEAR(A.AttendanceMonth) 
        ORDER BY A.AttendanceMonth 
        RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS RangeTotal

FROM tblEmployee AS E 
JOIN (
    SELECT * FROM tblAttendance
    UNION ALL
    SELECT * FROM tblAttendance
) AS A
ON E.EmployeeNumber = A.EmployeeNumber
ORDER BY A.EmployeeNumber, A.AttendanceMonth

--where A.AttendanceMonth < '20150101'
--order by A.EmployeeNumber, A.AttendanceMonth

--unbounded preceding and current row
--current row and unbounded following
--unbounded preceding and unbounded following - RANGE and ROWS

ALTER TABLE tblSales
ADD CustomerID INT;

UPDATE tblSales
SET CustomerID = CASE 
    WHEN SaleID = 1 THEN 101
    WHEN SaleID = 2 THEN 102
    WHEN SaleID = 3 THEN 103
    WHEN SaleID = 4 THEN 101
    WHEN SaleID = 5 THEN 104
END;

SELECT * FROM tblSales;

-- ROWS when -> row-by-row calculations.
-- RANGE when -> value-based grouping calculations.
SELECT SaleID, SaleDate, CustomerID, SaleAmount,
SUM(SaleAmount) OVER (ORDER BY SaleAmount 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningTotal_ROWS
FROM tblSales;

SELECT SaleID, SaleDate, CustomerID, SaleAmount,
SUM(SaleAmount) OVER (ORDER BY SaleAmount 
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningTotal_RANGE
FROM tblSales;

-- Omitting Range/Row
SELECT 
    A.EmployeeNumber, 
    A.AttendanceMonth, 
    A.NumberAttendance,
    SUM(A.NumberAttendance) 
    OVER(
        PARTITION BY E.EmployeeNumber, YEAR(A.AttendanceMonth)
        ORDER BY A.AttendanceMonth  -- Window frame with ROWS or RANGE must have an ORDER BY clause.
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS TotalAttendance
FROM tblEmployee AS E 
JOIN tblAttendance AS A
ON E.EmployeeNumber = A.EmployeeNumber;


select sum(NumberAttendance) from tblAttendance

SELECT 
    A.EmployeeNumber, 
    A.AttendanceMonth, 
    A.NumberAttendance,
    SUM(A.NumberAttendance) OVER(
        PARTITION BY E.EmployeeNumber, YEAR(A.AttendanceMonth)
        ORDER BY A.AttendanceMonth
    ) AS SumAttendance
FROM tblEmployee AS E 
JOIN (SELECT * FROM tblAttendance UNION ALL SELECT * FROM tblAttendance) AS A
ON E.EmployeeNumber = A.EmployeeNumber
ORDER BY A.EmployeeNumber, A.AttendanceMonth;

--range between unbounded preceding and unbounded following  - DEFAULT where there is no ORDER BY
--rows/range between unbounded preceding and current row     - DEFAULT where there IS an ORDER BY

SELECT 
    EmployeeNumber, 
    DateOfTransaction, 
    Amount,
    SUM(Amount) OVER(PARTITION BY EmployeeNumber ORDER BY DateOfTransaction) AS TotalAmount
FROM dbo.tblTransaction;

-- ROW_NUMBER (Transact-SQL)

SELECT 
    E.EmployeeNumber, A.AttendanceMonth, 
	ROW_NUMBER() OVER(PARTITION BY A.EmployeeNumber ORDER BY A.EmployeeNumber , A.AttendanceMonth) AS TheRowNumber
FROM tblEmployee AS E JOIN tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber


-- No PARTITION BY.
SELECT A.EmployeeNumber, A.AttendanceMonth,  
A.NumberAttendance,  
ROW_NUMBER() OVER(ORDER BY E.EmployeeNumber, A.AttendanceMonth) as TheRowNumber, --always continues without gaps
RANK() OVER(ORDER BY E.EmployeeNumber, A.AttendanceMonth) as TheRank, --gaps happen.
DENSE_RANK() OVER(ORDER BY E.EmployeeNumber, A.AttendanceMonth) as TheDenseRank --No gaps even if tied.
from tblEmployee as E 
join (Select * from tblAttendance union all select * from tblAttendance) as A 
ON E.EmployeeNumber = A.EmployeeNumber


--Now PARTITION BY EmployeeNumber.
SELECT A.EmployeeNumber, A.AttendanceMonth,  
A.NumberAttendance,  
ROW_NUMBER() OVER(PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth) as TheRowNumber, 
RANK()       OVER(PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth) as TheRank, 
DENSE_RANK() OVER(PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth) as TheDenseRank
from tblEmployee as E 
join (Select * from tblAttendance union all select * from tblAttendance) as A 
ON E.EmployeeNumber = A.EmployeeNumber

-- selects all columns from tblAttendance no specific order (since (select null)
SELECT *, row_number() over(order by (select null)) from tblAttendance  


-- NTILE 
SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth,  
  A.NumberAttendance,  
  -- 1. Automatically splits rows into 10 equal groups (tiles) for each Employee
  NTILE(10) OVER(
      PARTITION BY E.EmployeeNumber 
      ORDER BY A.AttendanceMonth
  ) AS TheNTile,
  -- 2. Manually calculates the same 10 groups
  CONVERT(INT, (
      (ROW_NUMBER() OVER(
           PARTITION BY E.EmployeeNumber 
           ORDER BY A.AttendanceMonth
       ) - 1)
      /
      (COUNT(*) OVER(
           PARTITION BY E.EmployeeNumber 
           ORDER BY A.AttendanceMonth 
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) / 10.0)
  ) + 1) AS MyNTile
FROM tblEmployee AS E 
JOIN  tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
WHERE A.AttendanceMonth < '2015-05-01'

  -- test 
SELECT EmployeeID, Salary,
	RANK() OVER(ORDER BY Salary DESC) AS SalaryRank
FROM Employees;


--FIRST_VALUE and LAST_VALUE
SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth,
  A.NumberAttendance,
  FIRST_VALUE(NumberAttendance)
  OVER(PARTITION BY E.EmployeeNumber 
       ORDER BY A.AttendanceMonth) AS FirstMonth,

  LAST_VALUE(NumberAttendance)
  OVER(PARTITION BY E.EmployeeNumber 
       ORDER BY A.AttendanceMonth
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastMonth

FROM tblEmployee AS E 
JOIN tblAttendance AS A 
  ON E.EmployeeNumber = A.EmployeeNumber


  --using a WINDOW clause instead of repeating the OVER(...)clause 
SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth,
  A.NumberAttendance,

  FIRST_VALUE(NumberAttendance) OVER attendance_window AS FirstMonth, --Attendance value of the first month per employee
  LAST_VALUE(NumberAttendance)  OVER attendance_window AS LastMonth  --Attendance value of the last month per employee
  --NTH_VALUE(NumberAttendance, 2) OVER attendance_window AS SecondValue --Attendance value of the second month per employee

FROM tblEmployee AS E 
JOIN tblAttendance AS A 
  ON E.EmployeeNumber = A.EmployeeNumber

WINDOW attendance_window AS (
  PARTITION BY E.EmployeeNumber 
  ORDER BY A.AttendanceMonth
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
);

WITH AttendanceRanked AS (
  SELECT 
    A.EmployeeNumber,
    A.AttendanceMonth,
    A.NumberAttendance,
    ROW_NUMBER() OVER (PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth) AS rn
  FROM tblEmployee AS E
  JOIN tblAttendance AS A
    ON E.EmployeeNumber = A.EmployeeNumber
)
SELECT * 
FROM AttendanceRanked
WHERE rn = 2; -- emulate NTH_VALUE() using ROW_NUMBER() and a CTE

--LAG and LEAD
SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth,
  A.NumberAttendance,

  LAG(NumberAttendance, 1,999) OVER (
    PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth
  ) AS MyLag,

  LEAD(NumberAttendance, 1,999) OVER (
    PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth
  ) AS MyLead,

  NumberAttendance - LAG(NumberAttendance, 1,999) OVER (
    PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth
  ) AS MyDiff,

    NumberAttendance - LEAD(NumberAttendance, 1,999) OVER (
    PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth
  ) AS MyDiff

FROM tblEmployee AS E 
JOIN tblAttendance AS A 
  ON E.EmployeeNumber = A.EmployeeNumber


--PERCENTILE_CONT and PERCENTILE_DISC
SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth,
  A.NumberAttendance,
  -- Built-in cumulative distribution
  CUME_DIST() OVER (
    PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth
  ) AS MyCume_Dist,--Shows the cumulative distribution: the proportion of rows less than or equal to the current row. Range: (0, 1].
  -- Built-in percent rank
  PERCENT_RANK() OVER (
    PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth
  ) AS MyPercent_Rank, --Shows the relative rank of a row among all rows. First row = 0. Last row = 1. Range: [0, 1].
  -- Manual CUME_DIST = ROW_NUMBER / COUNT
  CAST(ROW_NUMBER() OVER (PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth) AS DECIMAL(9,5)) /--Assigns unique sequential numbers to rows per partition.
  COUNT(*) OVER (PARTITION BY E.EmployeeNumber) AS CalcCume_Dist,
  -- Manual PERCENT_RANK = (ROW_NUMBER - 1) / (COUNT - 1)
  CAST(ROW_NUMBER() OVER (PARTITION BY E.EmployeeNumber ORDER BY A.AttendanceMonth) - 1 AS DECIMAL(9,5)) /
  (COUNT(*) OVER (PARTITION BY E.EmployeeNumber) - 1) AS CalcPercent_Rank --Total rows per partition (i.e., per Employee).
FROM tblEmployee AS E 
JOIN tblAttendance AS A 
  ON E.EmployeeNumber = A.EmployeeNumber

  SELECT 
  A.EmployeeNumber, 
  A.AttendanceMonth,
  A.NumberAttendance,
  FIRST_VALUE(NumberAttendance) OVER (
    PARTITION BY E.EmployeeNumber 
    ORDER BY A.AttendanceMonth 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS FirstMonth,
  LAST_VALUE(NumberAttendance) OVER (
    PARTITION BY E.EmployeeNumber 
    ORDER BY A.AttendanceMonth 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS LastMonth
  --NTH_VALUE(NumberAttendance, 2) OVER (
  --  PARTITION BY E.EmployeeNumber 
  --  ORDER BY A.AttendanceMonth 
  --  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  --) AS SecondValue
FROM tblEmployee AS E 
JOIN tblAttendance AS A 
  ON E.EmployeeNumber = A.EmployeeNumber;
 

-- 1. Adding Totals Detail: Attendance by Department, Employee, and Month
SELECT 
    E.Department, 
    E.EmployeeNumber, 
    A.AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance
FROM 
    tblEmployee AS E
JOIN 
    tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
GROUP BY 
    E.Department, E.EmployeeNumber, A.AttendanceMonth
UNION

-- 2. Subtotal: Total Attendance per Employee (no month)
SELECT 
    E.Department, 
    E.EmployeeNumber, 
    NULL AS AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance
FROM 
    tblEmployee AS E
JOIN 
    tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
GROUP BY 
    E.Department, E.EmployeeNumber

UNION

-- 3. Subtotal: Total Attendance per Department (no employee, no month)
SELECT 
    E.Department, 
    NULL AS EmployeeNumber, 
    NULL AS AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance
FROM 
    tblEmployee AS E
JOIN 
    tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
GROUP BY 
    E.Department

UNION

-- 4. Grand Total: Overall Attendance (no department, employee, or month)
SELECT 
    NULL AS Department, 
    NULL AS EmployeeNumber, 
    NULL AS AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance
FROM 
    tblEmployee AS E
JOIN 
    tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
ORDER BY 
    Department, EmployeeNumber, AttendanceMonth;

SELECT DISTINCT EmployeeNumber,
    PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY NumberAttendance)
        OVER (PARTITION BY EmployeeNumber) AS AverageCont,
    PERCENTILE_DISC(0.4) WITHIN GROUP (ORDER BY NumberAttendance)
        OVER (PARTITION BY EmployeeNumber) AS AverageDisc
FROM tblAttendance;


WITH ProductWithCumeDist AS (
    SELECT  EmployeeNumber,
        CUME_DIST() OVER (ORDER BY EmployeeNumber DESC) * 100.0 AS cume_dist_percentage
    FROM  tblAttendance
)
SELECT  EmployeeNumber,
    CAST(ROUND(cume_dist_percentage, 2) AS VARCHAR) + '%' AS cume_dist_percentage
FROM 
    ProductWithCumeDist
WHERE 
    cume_dist_percentage <= 30.0;

SELECT 
    EmployeeNumber, 
    DateOfTransaction, 
    Amount,
  --  LAST_VALUE(Amount) OVER (
  --      PARTITION BY EmployeeNumber 
  --      ORDER BY DateOfTransaction
  --      -- No ROWS or RANGE clause
		--RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  --  ) AS TotalAmount
  --Correct Usage
  LAST_VALUE(Amount) OVER (
    PARTITION BY EmployeeNumber 
    ORDER BY DateOfTransaction DESC
    --RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS TotalAmount
FROM dbo.tblTransaction;

--The order of arguments for both LAG() and LEAD() functions in T-SQL
SELECT 
    Employees.DepartmentID
    Salary,
    LAG(Salary, 1, 0) OVER (PARTITION BY DepartmentID ORDER BY Salary) AS PreviousSalary,
    LEAD(Salary, 1, 0) OVER (PARTITION BY DepartmentID ORDER BY Salary) AS NextSalary
FROM Employees;

SELECT POWER(4, 3) AS Result1;
SELECT FLOOR(PI()) AS Result2;
SELECT CEILING(PI()) AS Result3;

-- Monthly attendance per employee
SELECT 
    E.Department, 
    E.EmployeeNumber, 
    A.AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance,
    'Monthly Detail' AS RowType
FROM tblEmployee AS E 
JOIN tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
GROUP BY E.Department, E.EmployeeNumber, A.AttendanceMonth

UNION

-- Total attendance per employee
SELECT 
    E.Department, 
    E.EmployeeNumber, 
    NULL AS AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance,
    'Employee Total' AS RowType
FROM tblEmployee AS E 
JOIN tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
GROUP BY E.Department, E.EmployeeNumber

UNION

-- Total attendance per department
SELECT 
    E.Department, 
    NULL AS EmployeeNumber, 
    NULL AS AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance,
    'Department Total' AS RowType
FROM tblEmployee AS E 
JOIN tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber
GROUP BY E.Department

UNION

-- Grand total
SELECT 
    NULL AS Department, 
    NULL AS EmployeeNumber, 
    NULL AS AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance,
    'Grand Total' AS RowType
FROM tblEmployee AS E 
JOIN tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber

-- Final sort for a readable report
ORDER BY 
    Department, 
    EmployeeNumber, 
    AttendanceMonth;

-- ROLLUP replaces multiple UNION queries
SELECT 
  E.Department, 
  E.EmployeeNumber, 
  A.AttendanceMonth,
  SUM(A.NumberAttendance) AS NumberAttendance,

  GROUPING(E.EmployeeNumber) AS EmployeeNumberGroupedBy, -- tells if EmployeeNumber was NULL due to ROLLUP
  GROUPING_ID(E.Department, E.EmployeeNumber, A.AttendanceMonth) AS EmployeeNumberGroupedID -- helps identify row type

FROM tblEmployee AS E
JOIN tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber

GROUP BY ROLLUP (E.Department, E.EmployeeNumber, A.AttendanceMonth)
ORDER BY Department, EmployeeNumber, AttendanceMonth;


--GROUPING SETS
--ROUPING SETS lets specify exactly which groupings you want, unlike ROLLUP or CUBE, which follow a predefined logic.
SELECT 
    E.Department, 
    E.EmployeeNumber, 
    A.AttendanceMonth, 
    SUM(A.NumberAttendance) AS NumberAttendance,
    
    -- Useful for identifying subtotal and total rows
    GROUPING(E.EmployeeNumber) AS EmployeeNumberGroupedBy,
    GROUPING_ID(E.Department, E.EmployeeNumber, A.AttendanceMonth) AS GroupingLevel,
    
    -- Label rows based on grouping level
    CASE 
        WHEN GROUPING_ID(E.Department, E.EmployeeNumber, A.AttendanceMonth) = 0 THEN 'Monthly Detail'
        WHEN GROUPING_ID(E.Department, E.EmployeeNumber, A.AttendanceMonth) = 3 THEN 'Department Total'
        WHEN GROUPING_ID(E.Department, E.EmployeeNumber, A.AttendanceMonth) = 7 THEN 'Grand Total'
        ELSE 'Other'
    END AS RowType

FROM 
    tblEmployee AS E 
JOIN 
    tblAttendance AS A ON E.EmployeeNumber = A.EmployeeNumber

GROUP BY 
    GROUPING SETS (
        (E.Department, E.EmployeeNumber, A.AttendanceMonth), -- full detail
        (E.Department),                                      -- department total
        ()                                                   -- grand total
    )

ORDER BY 
    -- Place non-NULLs first
    CASE WHEN E.Department IS NULL THEN 1 ELSE 0 END, 
    E.Department,
    
    CASE WHEN E.EmployeeNumber IS NULL THEN 1 ELSE 0 END, 
    E.EmployeeNumber,
    
    CASE WHEN A.AttendanceMonth IS NULL THEN 1 ELSE 0 END, 
    A.AttendanceMonth;


BEGIN TRAN
DROP TABLE IF EXISTS tblGeom
CREATE TABLE tblGeom 
(GXY geometry , DESCRIPTION VARCHAR(30),
IDtblGeom INT CONSTRAINT PK_tblGeom PRIMARY KEY IDENTITY(1,1))
INSERT INTO tblGeom
VALUES (geometry::STGeomFromText('POINT(3 4)',0),'First point'),
    (geometry::STGeomFromText('POINT (3 4)', 0), 'First point'),
    (geometry::STGeomFromText('POINT (3 5)', 0), 'Second point'),
    (geometry::Point(4, 6, 0), 'Third Point'),
    (geometry::STGeomFromText('MULTIPOINT ((1 2), (2 3), (3 4))', 0), 'Three Points')

SELECT * FROM tblGeom 
ROLLBACK TRAN


----COMMIT TRAN --to keep the table
BEGIN TRAN;
CREATE TABLE tblGeom (
    GXY geometry,
    DESCRIPTION VARCHAR(30),
    IDtblGeom INT CONSTRAINT PK_tblGeom PRIMARY KEY IDENTITY(1,1)
);
INSERT INTO tblGeom
VALUES (geometry::STGeomFromText('POINT(3 4)', 0), 'First point');
-- Commit the transaction so the data is saved
COMMIT TRAN;

INSERT INTO tblGeom
VALUES 
(geometry::STGeomFromText('POINT(4 4)', 0), 'Second point'),
(geometry::STGeomFromText('POINT(10 10)', 0), 'Far point');

--To find Points Within 2 Units of (3, 4)
DECLARE @center geometry = geometry::STGeomFromText('POINT(3 4)', 0);
SELECT * 
FROM tblGeom
WHERE GXY.STDistance(@center) <= 2;

--Visualize All Data
SELECT 
    IDtblGeom,
    DESCRIPTION,
    GXY.ToString() AS GeometryText,
    GXY.STAsText() AS WKT,
    GXY.STX AS X,
    GXY.STY AS Y
FROM tblGeom;

| Operator    | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| `UNION`     | Combines rows from both queries, removes duplicates          |
| `UNION ALL` | Combines all rows, keeps duplicates                          |
| `INTERSECT` | Keeps only rows common to both                               |
| `EXCEPT`    | Returns rows from the first query that are not in the second |

-- To find duplicate EmployeeNumber in both tblEmployee tblAttendance!
SELECT EmployeeNumber
FROM (
    SELECT EmployeeNumber FROM tblEmployee
    UNION ALL
    SELECT EmployeeNumber FROM tblAttendance
) AS Combined
GROUP BY EmployeeNumber
HAVING COUNT(*) > 1;

--geometry data
BEGIN TRAN;

-- Drop table if it exists
IF OBJECT_ID('tblGeom', 'U') IS NOT NULL
    DROP TABLE tblGeom;

CREATE TABLE tblGeom (
    GXY geometry,                           -- The spatial geometry object
    Description VARCHAR(20),                -- Description of the shape
    IDtblGeom INT IDENTITY(5,1) PRIMARY KEY -- Auto-incrementing ID starting at 5
);

INSERT INTO tblGeom (GXY, Description)
VALUES
    (geometry::STGeomFromText('LINESTRING (1 1, 5 5)', 0), 'First line'),
    (geometry::STGeomFromText('LINESTRING (5 1, 1 4, 2 5, 5 1)', 0), 'Second line'),
    (geometry::STGeomFromText('MULTILINESTRING ((1 5, 2 6), (1 4, 2 5))', 0), 'Third line'),
    (geometry::STGeomFromText('POLYGON ((4 1, 6 3, 8 3, 6 1, 4 1))', 0), 'Polygon'),
    (geometry::STGeomFromText('CIRCULARSTRING (1 0, 0 1, -1 0, 0 -1, 1 0)', 0), 'Circle');

SELECT * FROM tblGeom;

SELECT
    IDtblGeom,
    GXY.STGeometryType() AS GeometryType,                      -- Type of geometry
    GXY.STStartPoint().ToString() AS StartPoint,               -- Starting point (for lines)
    GXY.STEndPoint().ToString() AS EndPoint,                   -- Ending point (for lines)
    GXY.STPointN(1).ToString() AS FirstPoint,                  -- First vertex
    GXY.STPointN(2).ToString() AS SecondPoint,                 -- Second vertex (if exists)
    GXY.STPointN(1).STX AS FirstPointX,                        -- X of first point
    GXY.STPointN(1).STY AS FirstPointY,                        -- Y of first point
    GXY.STBoundary().ToString() AS Boundary,                   -- Boundary (polygon edges)
    GXY.STLength() AS Length,                                  -- Total length
    GXY.STNumPoints() AS NumberOfPoints                        -- Number of points in shape

FROM tblGeom;

DECLARE @circle geometry;
SELECT @circle = GXY FROM tblGeom WHERE IDtblGeom = 5;

SELECT
    IDtblGeom,
    GXY.STIntersection(@circle).ToString() AS Intersection,    -- Overlapping geometry
    GXY.STDistance(@circle) AS DistanceFromCircle              -- Distance from the circle
FROM tblGeom;

SELECT
    GXY.STUnion(@circle) AS CombinedGeometry,
    Description
FROM tblGeom
WHERE IDtblGeom = 8;

SELECT 
    GXY.STStartPoint().ToString() AS StartingPoint
FROM 
    tblGeom;

SELECT 
    GXY.STPointN(2).STX AS SecondPointX
FROM 
    tblGeom
WHERE 
  GXY.STGeometryType() = 'MultiPoint';

ROLLBACK TRAN;

-- quiz line quereis 
-- To calculate the number of points in a polygon in T-SQL using the STNumPoints()

DECLARE @poly geometry
SET @poly = geometry::STGeomFromText('POLYGON ((4 1, 6 3, 8 3, 6 1, 4 1))', 0)

SELECT @poly.STNumPoints() AS NumberOfPoints

-- Geography Data
-- Drop table if it exists 
IF OBJECT_ID('tblGeog', 'U') IS NOT NULL
    DROP TABLE tblGeog;

BEGIN TRANSACTION

CREATE TABLE tblGeog (
    GXY geography, 
    Description varchar(255),  
    IDtblGeog int CONSTRAINT PK_tblGeog PRIMARY KEY IDENTITY(1,1)  
);

INSERT INTO tblGeog (GXY, Description)
VALUES
    (geography::STGeomFromText('POINT (9.993682 53.551086)', 4326), 'Hamburg, Germany'),
    (geography::STGeomFromText('POINT (9.8688 53.3316)', 4326), 'Buchholz in der Nordheide, Germany'),
    (geography::STGeomFromText('LINESTRING (9.993682 53.551086, 9.8688 53.3316)', 4326), 'Connection Hamburg-Buchholz');

SELECT * FROM tblGeog;

DECLARE @g geography;
SELECT @g = GXY FROM tblGeog WHERE IDtblGeog = 1;  

-- Performing spatial operations like geometry type, points, distance, intersection...
SELECT 
    IDtblGeog, 
    GXY.STGeometryType() AS GeometryType,  -- Returns the geometry type (Point, LineString, etc.)
    GXY.STStartPoint().ToString() AS StartingPoint,  -- Start point of geometry
    GXY.STEndPoint().ToString() AS EndingPoint,  -- End point of geometry (if applicable)
    GXY.STPointN(1).ToString() AS FirstPoint,  -- First point in geometry (for lines or multi-points)
    GXY.STPointN(2).ToString() AS SecondPoint,  -- Second point in geometry (for lines or multi-points)
    GXY.STLength() AS Length,  -- Length of the geometry (relevant for LineString)
    GXY.STIntersection(@g).ToString() AS Intersection,  -- Intersection with selected geography
    GXY.STNumPoints() AS NumPoints,  -- Number of points in the geometry (useful for complex geometries)
    GXY.STDistance(@g) AS DistanceFromFirstPoint  -- Distance from selected geography point (Hamburg)
FROM tblGeog;

DECLARE @h geography;
SELECT @g = GXY FROM tblGeog WHERE IDtblGeog = 1;  -- Hamburg
SELECT @h = GXY FROM tblGeog WHERE IDtblGeog = 2;  -- Buchholz

-- Calculate the distance between two points (Hamburg and Buchholz)
SELECT @g.STDistance(@h) AS DistanceBetweenHamburgAndBuchholz;

-- Union of geometries (combining two geographical areas)
SELECT GXY.STUnion(@g) AS UnionGeometry
FROM tblGeog
WHERE IDtblGeog = 2;  -- Buchholz geometry with Hamburg geometry

ROLLBACK TRANSACTION;

SELECT * FROM sys.spatial_reference_systems;

--Quiz Geo 
SELECT geography::UnionAggregate(GXY) AS MergedShapes--  Merges multiple geography/geometry rows into a single shape.
FROM tblGeog;

--method that creates a box around the shapes of a collection is geometry::EnvelopeAggregat
-- Start a transaction to allow rollback (safe testing)
BEGIN TRAN
IF OBJECT_ID('dbo.tblGeog', 'U') IS NOT NULL
    DROP TABLE dbo.tblGeog;
/*
| Code | Object Type			      |
| -----| ---------------------------- |
| 'U'  | User-defined table           |
| 'V'  | View                         |
| 'P'  | Stored procedure             |
| 'FN' | Scalar function              |
| 'IF' | Inline table-valued function |
| 'TF' | Table-valued function        |
*/

CREATE TABLE dbo.tblGeog(
    GXY geography,
    Description VARCHAR(100),
    IDtblGeog INT IDENTITY(1,1) PRIMARY KEY
);

INSERT INTO dbo.tblGeog (GXY, Description)
VALUES
    (geography::STGeomFromText('POINT (10.000654 53.551086)', 4326), 'Hamburg, Germany'),
    (geography::STGeomFromText('POINT (9.8656 53.3336)', 4326), 'Buchholz in der Nordheide, Germany');

SELECT * FROM dbo.tblGeog;
SELECT geography::EnvelopeAggregate(GXY).ToString() AS BoundingBox
FROM dbo.tblGeog;

SELECT 
    IDtblGeog,
    Description,
    GXY.STAsText() AS WKT,
    GXY.Lat AS Latitude,
    GXY.Long AS Longitude
FROM dbo.tblGeog;

ROLLBACK TRAN;

------------------------------------------- SESSION 6-------------------------------------------------------------
SELECT *
FROM tblTransaction AS T
INNER JOIN tblEmployee AS E ON T.EmployeeNumber = E.EmployeeNumber
--LEFT JOIN tblEmployee AS E ON T.EmployeeNumber = E.EmployeeNumber;
WHERE E.EmployeeNumber = T.EmployeeNumber

-- 5. Subquery – WHERE and NOT
--return records from tblTransaction for employees whose last names start with 'Y'.
SELECT T.*
FROM tblTransaction AS T
INNER JOIN tblEmployee AS E ON E.EmployeeNumber = T.EmployeeNumber --Explicit join with tblEmployee
WHERE E.EmployeeLastName LIKE 'y%'
ORDER BY T.EmployeeNumber;

--Using IN subquery
SELECT *
FROM tblTransaction AS T
WHERE EmployeeNumber IN (   --No join, filters using IN subquery
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
)
ORDER BY EmployeeNumber;

--SELECT EmployeeNumber FROM tblEmployee  WHERE EmployeeLastName LIKE 'y%'
SELECT *
FROM tblTransaction AS T
WHERE EmployeeNumber NOT IN (
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
)
ORDER BY EmployeeNumber;-- must be in tblEmployee and tblTransaction and not 126-129
						-- INNER JOIN

SELECT *
FROM tblTransaction AS T
WHERE EmployeeNumber  IN (
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName NOT LIKE 'y%'
)
ORDER BY EmployeeNumber;-- must be in tblTransaction and not 126-129
						-- left join

SELECT T.*, E.EmployeeLastName
FROM tblTransaction AS T
JOIN tblEmployee AS E ON T.EmployeeNumber = E.EmployeeNumber
WHERE T.EmployeeNumber NOT IN (
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
)
ORDER BY T.EmployeeNumber;

SELECT T.*
FROM tblTransaction AS T
INNER JOIN tblEmployee AS E ON T.EmployeeNumber = E.EmployeeNumber
WHERE E.EmployeeLastName NOT LIKE 'y%'
ORDER BY T.EmployeeNumber;

--6 Subquery – WHERE and ANY, SOME and ALL
SELECT EmployeeNumber
FROM tblEmployee
WHERE EmployeeLastName LIKE 'y%'

-- <> ANY Almost always TRUE, unless all employee numbers match exactly.
SELECT *
FROM tblTransaction AS T
WHERE EmployeeNumber <> ANY (
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
) --126 <> 126 OR 126 <> 127 OR 126 <> 128 OR 126 <> 129
  --FALSE OR TRUE OR TRUE OR TRUE → TRUE

SELECT *
FROM tblTransaction AS T
WHERE EmployeeNumber <> ALL (
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
)	-- Equivalent to WHERE EmployeeNumber NOT IN (126,127,128,129)

SELECT *
FROM tblTransaction AS T
WHERE EmployeeNumber <= ALL (
    SELECT EmployeeNumber
    FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
)	-- WHERE EmployeeNumber <= 126

| Clause           | Equivalent To			| Notes                      |
| ---------------- | ------------------		| -------------------------- |
| = ANY / = SOME   | IN (...)				|  Common and safe           |
| <> ALL           | NOT IN (...)			|  Correct for exclusions    |
| <> ANY           | Unsafe, misleading		|  Avoid for filtering       |
| <= ALL           | <= MIN(...)			|  For threshold comparisons |

-- 7. Subqueries in the FROM clause
--Subquery in the FROM clause (filtered before the join)
SELECT *
FROM tblTransaction AS T
LEFT JOIN (
    SELECT * FROM tblEmployee
    WHERE EmployeeLastName LIKE 'y%'
) AS E
ON E.EmployeeNumber = T.EmployeeNumber
ORDER BY T.EmployeeNumber;

--Filter in the WHERE clause (after join)
SELECT *
FROM tblTransaction AS T
LEFT JOIN tblEmployee AS E
ON E.EmployeeNumber = T.EmployeeNumber
WHERE E.EmployeeLastName LIKE 'y%'
ORDER BY T.EmployeeNumber;

--Filter in the ON clause (correct LEFT JOIN with condition)
SELECT *
FROM tblTransaction AS T
LEFT JOIN tblEmployee AS E
ON E.EmployeeNumber = T.EmployeeNumber
AND E.EmployeeLastName LIKE 'y%'
ORDER BY T.EmployeeNumber;

| Join Type | Filter Location        | Result                                               |
| --------- | ---------------------- | ---------------------------------------------------- |
| LEFT JOIN | Inside Subquery (FROM) | Only 'y%' employees join, others = NULL              |
| LEFT JOIN | In WHERE clause        | Behaves like INNER JOIN (filters out unmatched rows) |
| LEFT JOIN | In ON clause           | Proper LEFT JOIN with conditional join               |

select *
from tblTransaction as T
inner join (
select * from tblEmployee
where EmployeeLastName like 'y%'
) as E
on E.EmployeeNumber = T.EmployeeNumber
order by T.EmployeeNumber

--8. Subquery – Select Clause (correlated subquery in the SELECT)

SELECT 
    E.*, 
    (
        SELECT COUNT(*) 
        FROM tblTransaction AS T 
        WHERE T.EmployeeNumber = E.EmployeeNumber
    ) AS NumTransactions,
    (
        SELECT SUM(Amount) 
        FROM tblTransaction AS T 
        WHERE T.EmployeeNumber = E.EmployeeNumber
    ) AS TotalAmount

FROM tblEmployee AS E
WHERE E.EmployeeLastName LIKE 'y%';

-- Using EXISTS (For each row in tblTransaction, this checks whether a matching employee exists in tblEmployee)
SELECT *
FROM tblTransaction AS T
WHERE EXISTS (
    SELECT EmployeeNumber
    FROM tblEmployee AS E
    WHERE EmployeeLastName LIKE 'y%'
    AND T.EmployeeNumber = E.EmployeeNumber
)
ORDER BY EmployeeNumber;

--2. Using NOT EXISTS (It selects only those transactions where no matching employee (with last name starting with 'y') is found)
SELECT *
FROM tblTransaction AS T
WHERE NOT EXISTS (
    SELECT EmployeeNumber
    FROM tblEmployee AS E
    WHERE EmployeeLastName LIKE 'y%'
    AND T.EmployeeNumber = E.EmployeeNumber
)
ORDER BY EmployeeNumber;


-- Using IN
SELECT *
FROM tblTransaction
WHERE EmployeeNumber IN (
    SELECT EmployeeNumber FROM tblEmployee WHERE EmployeeLastName LIKE 'y%'
);

-- Using EXISTS
SELECT *
FROM tblTransaction AS T
WHERE EXISTS (
    SELECT 1 FROM tblEmployee AS E
    WHERE E.EmployeeLastName LIKE 'y%' AND E.EmployeeNumber = T.EmployeeNumber
);


-- Differences between IN and EXISTS 
-- If the subquery returns NULL, this fails
WHERE EmployeeNumber IN (NULL) --  No match
-- EXISTS doesn't care
WHERE EXISTS (...) --  Still works (faster with correlated subqueries )

--10. Top 10 from various categories
SELECT * 
FROM (
    SELECT 
        D.Department, 
        EmployeeNumber, 
        EmployeeFirstName, 
        EmployeeLastName,
        RANK() OVER (PARTITION BY D.Department ORDER BY E.EmployeeNumber) AS TheRank
    FROM tblDepartment AS D
    JOIN tblEmployee AS E 
        ON D.Department = E.Department
) AS MyTable
WHERE TheRank <= 10
ORDER BY Department, EmployeeNumber;

--11. With Statement
WITH tblWithRanking AS (
    SELECT 
        D.Department, 
        EmployeeNumber, 
        EmployeeFirstName, 
        EmployeeLastName,
        DENSE_RANK() OVER (PARTITION BY D.Department ORDER BY E.EmployeeNumber) AS TheRank
    FROM tblDepartment AS D  --RANK() can result in gaps, if there are ties then DENSE_RANK()
    JOIN tblEmployee AS E ON D.Department = E.Department -- if no gaps ROW_NUMBER() gives strict top-N without ties.
),
Transaction2014 AS (
    SELECT * 
    FROM tblTransaction 
    WHERE DateOfTransaction < '2015-01-01'
)
SELECT * 
FROM tblWithRanking
LEFT JOIN Transaction2014 
    ON tblWithRanking.EmployeeNumber = Transaction2014.EmployeeNumber
WHERE TheRank <= 5
ORDER BY Department, tblWithRanking.EmployeeNumber;

-- Exercises 
WITH Numbers AS ( -- CTE 1: Numbers
    SELECT TOP (SELECT MAX(EmployeeNumber) FROM tblTransaction)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNumber
    FROM tblTransaction AS U
),

Transactions2014 AS ( -- CTE 2: Filters transactions that happened in 2014 only
    SELECT * 
    FROM tblTransaction 
    WHERE DateOfTransaction >= '2014-01-01' AND DateOfTransaction < '2015-01-01'
),

tblGap AS ( -- CTE 3: Uses LAG() and LEAD() to group contiguous gaps
    SELECT 
        U.RowNumber,
        RowNumber - LAG(RowNumber) OVER (ORDER BY RowNumber) AS PreviousRowNumber,
        LEAD(RowNumber) OVER (ORDER BY RowNumber) - RowNumber AS NextRowNumber,
        CASE 
            WHEN RowNumber - LAG(RowNumber) OVER (ORDER BY RowNumber) = 1 THEN 0 
            ELSE 1 
        END AS GroupGap
    FROM Numbers AS U
    LEFT JOIN Transactions2014 AS T
        ON U.RowNumber = T.EmployeeNumber
    WHERE T.EmployeeNumber IS NULL
),

tblGroup AS ( -- CTE 4: Assigns a group ID to each gap segment
    SELECT *, 
           SUM(GroupGap) OVER (ORDER BY RowNumber) AS TheGroup
    FROM tblGap
)

-- Final SELECT: Show ranges of missing employee numbers
SELECT  
    MIN(RowNumber) AS StartingEmployeeNumber, 
    MAX(RowNumber) AS EndingEmployeeNumber,
    MAX(RowNumber) - MIN(RowNumber) + 1 AS NumberEmployees
FROM tblGroup
GROUP BY TheGroup
ORDER BY TheGroup;

-- 14. Pivot
WITH CTETable AS (
    SELECT 
        YEAR(DateOfTransaction) AS TheYear,  
        MONTH(DateOfTransaction) AS TheMonth, 
        Amount 
    FROM tblTransaction
)

SELECT 
    TheYear, 
    [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12]
FROM CTETable
PIVOT (
    SUM(Amount) 
    FOR TheMonth IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])
) AS myPvt
ORDER BY TheYear;

--15. Replacing Nulls with Zeros in Pivot
WITH myTable AS (
    SELECT 
        YEAR(DateOfTransaction) AS TheYear, 
        MONTH(DateOfTransaction) AS TheMonth, 
        Amount 
    FROM tblTransaction
)
SELECT 
    TheYear, 
    ISNULL([1], 0) AS ['mbarek'],
    ISNULL([2], 0) AS [2],
    ISNULL([3], 0) AS [3],
    ISNULL([4], 0) AS [4],
    ISNULL([5], 0) AS [5],
    ISNULL([6], 0) AS [6],
    ISNULL([7], 0) AS [7],
    ISNULL([8], 0) AS [8],
    ISNULL([9], 0) AS [9],
    ISNULL([10], 0) AS [10],
    ISNULL([11], 0) AS [11],
    ISNULL([12], 0) AS [12]
FROM myTable
PIVOT (
    SUM(Amount) 
    FOR TheMonth IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])
) AS myPvt
ORDER BY TheYear;

-- 16. UnPivot
SELECT *
FROM [tblPivot]
UNPIVOT (Amount FOR Month IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])) AS tblUnPivot

where Amount <> 0

--17. Self Joins
--join the table to itself:
BEGIN TRAN;
ALTER TABLE tblEmployee 
ADD Manager INT;
GO
UPDATE tblEmployee
SET Manager = ((EmployeeNumber - 123) / 10) + 123
WHERE EmployeeNumber > 123;
SELECT 
    E.EmployeeNumber, E.EmployeeFirstName, E.EmployeeLastName,
    M.EmployeeNumber AS ManagerNumber, M.EmployeeFirstName AS ManagerFirstName,
    M.EmployeeLastName AS ManagerLastName
FROM tblEmployee AS E
LEFT JOIN tblEmployee AS M
ON E.Manager = M.EmployeeNumber;

SELECT 
    E.EmployeeFirstName AS Employee,
    M.EmployeeFirstName AS Manager
FROM tblEmployee AS E
LEFT JOIN tblEmployee AS M
    ON E.Manager = M.EmployeeNumber;
-- COMMIT TRAN; -- Keep the changes
ROLLBACK TRAN; -- Undo everything

--18. Recursive CTE
--	 -.- Syntax 
WITH cte_name AS (
    -- Anchor part
    SELECT ...
    FROM ...
    WHERE ... -- base condition
    UNION ALL
    -- Recursive part
    SELECT ...
    FROM table_name t
    JOIN cte_name c ON ...
)
SELECT * FROM cte_name;

-- CTE (Common Table Expression) practice
BEGIN TRAN;
-- Add a Manager Column:
ALTER TABLE tblEmployee
ADD Manager INT;
GO
UPDATE tblEmployee
SET Manager = ((EmployeeNumber - 123) / 10) + 123
WHERE EmployeeNumber > 123;

WITH CTETable AS (

 -- Anchor member: the top-level bosses (who have no manager)
    SELECT --The anchor member (the base case).
        EmployeeNumber, 
        EmployeeFirstName, 
        EmployeeLastName, 
        0 AS BossLevel 
    FROM tblEmployee
    WHERE Manager IS NULL

    UNION ALL --The statement terminated. Recursive CTEs require UNION ALL.

	-- Recursive member: find people managed by those in the anchor
	SELECT --The recursive member (repeated until no new rows are produced).
        E.EmployeeNumber, 
        E.EmployeeFirstName, 
        E.EmployeeLastName, 
        CTETable.BossLevel + 1 AS BossLevel
    FROM tblEmployee AS E
    JOIN CTETable ON E.Manager = CTETable.EmployeeNumber
)

-- Select all employees with their respective boss levels
SELECT * 
FROM CTETable;
ROLLBACK TRAN;

-- Section 36 
-- T-SQL functions are classified as deterministic or non-deterministic 
--Deterministic Functions 
SELECT ABS(-10)
SELECT UPPER('M''BAREK'); --SQL Server uses single quotes ' for string literals.
SELECT ROUND(123.456, 2) 

--Non-Deterministic Functions
SELECT GETDATE()
SELECT NEWID() as nDate
SELECT RAND()
SELECT CURRENT_TIMESTAMP, SYSDATETIME()

--19. Scalar Functions 1
--Creating a Scalar Function
CREATE FUNCTION AmountPlusOne(@Amount smallmoney)
RETURNS smallmoney
AS
BEGIN
    RETURN @Amount + 1
END
GO

--Using (Calling) the Function in a SELECT
SELECT 
    DateOfTransaction, 
    EmployeeNumber, 
    Amount, 
    dbo.AmountPlusOne(Amount) AS AmountAndOne-- return a single value and can be used in SELECT, WHERE, ORDER BY
FROM tblTransaction 

--Executing (using EXEC) the Function into a Variable
DECLARE @myValue smallmoney
EXEC @myValue = dbo.AmountPlusOne @Amount = 345.67
SELECT @myValue

-- 20. Scalar Functions 2
IF EXISTS(SELECT * FROM sys.objects WHERE name = 'NumberOfTransactions' ) --type FN
	DROP FUNCTION NumberOfTransactions;

IF OBJECT_ID(N'NumberOfTransactions', N'FN') IS NOT NULL
    DROP FUNCTION NumberOfTransactions;
GO

CREATE FUNCTION NumberOfTransactions(@EmployeeNumber INT)
RETURNS INT
AS
BEGIN
    DECLARE @NumberOfTransactions INT;

    SELECT @NumberOfTransactions = COUNT(*)
    FROM tblTransaction
    WHERE EmployeeNumber = @EmployeeNumber;

    RETURN @NumberOfTransactions;
END

SELECT * 
FROM sys.objects 
WHERE name = 'NumberOfTransactions' AND type = 'FN';

-- comparison between Scalar functions and using traditional select and joining the tables 
-- This function returns the number of transactions 

--Using a Scalar Function:
--Syntax 
CREATE FUNCTION [dbo].[FunctionName]
(
    @param1 int,
	@param2 int
)
RETURNS INT
AS
BEGIN

    RETURN @param1 + @param2

END

SELECT 
    EmployeeNumber, 
    dbo.NumberOfTransactions(EmployeeNumber) AS TransactionCount
FROM tblEmployee;

--Using JOIN + GROUP BY (Set-Based Approach)
SELECT 
    E.EmployeeNumber, 
    E.EmployeeFirstName, 
    E.EmployeeLastName, 
    COUNT(T.EmployeeNumber) AS TransactNumber
FROM tblEmployee AS E
LEFT JOIN tblTransaction AS T
    ON E.EmployeeNumber = T.EmployeeNumber
GROUP BY 
    E.EmployeeNumber, 
    E.EmployeeFirstName, 
    E.EmployeeLastName;

-- 21. Inline Table Function
--Create Inline Table-Valued Function
--Syntax 
CREATE FUNCTION [dbo].[FunctionName]
(
    @param1 int,
    @param2 char(5)
)
RETURNS TABLE AS RETURN
(
    SELECT @param1 AS c1,
	       @param2 AS c2
)

CREATE FUNCTION TransactionList(@EmployeeNumber INT)
RETURNS TABLE
AS
RETURN (
    SELECT * FROM tblTransaction
    WHERE EmployeeNumber = @EmployeeNumber
)--It can be used in SELECT, JOIN, or EXISTS

--Get All Transactions for Employee 123
SELECT *
FROM dbo.TransactionList(123)

-- Find All Employees Who Have At Least One Transaction
SELECT *
FROM tblEmployee
WHERE EXISTS (
    SELECT * FROM dbo.TransactionList(EmployeeNumber)
)

--Same Result Using JOIN + DISTINCT
SELECT DISTINCT E.*
FROM tblEmployee AS E
JOIN tblTransaction AS T
    ON E.EmployeeNumber = T.EmployeeNumber

--Same Result Using a Simple Subquery
SELECT *
FROM tblEmployee AS E
WHERE EXISTS (
    SELECT 1
    FROM tblTransaction AS T
    WHERE E.EmployeeNumber = T.EmployeeNumber
)

--22. Multi-statment Table Function 
--Syntax 
CREATE FUNCTION [dbo].[FunctionName]
(
    @param1 int,
    @param2 char(5)
)
RETURNS @returntable TABLE 
(
	[c1] int,
	[c2] char(5)
)
AS
BEGIN
    INSERT @returntable
    SELECT @param1, @param2
    RETURN 
END

--Check Column Names
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'tblTransaction';

CREATE FUNCTION GetTransactionsOverAmount
(
    @MinAmount MONEY
)
RETURNS @Result TABLE
(
    EmployeeNumber INT,
    Amount MONEY,
    DateOfTransaction DATE
)
AS
BEGIN
    INSERT INTO @Result
    SELECT 
        EmployeeNumber, 
        Amount, 
        DateOfTransaction
    FROM tblTransaction
    WHERE Amount > @MinAmount

    -- additional logic here if needed...

    RETURN
END

-- Get all transactions over 500
SELECT * FROM dbo.GetTransactionsOverAmount(500)

/*
	differences between Scalar vs Inline table-valued vs Multi-statement table-valued :
	 1. Scalar returns a single (scalar) value.
	 2. Inline returns a table contains a single SELECT statement (no BEGIN...END, no procedural logic)
	 3. Multi-Statement returns a table, but built in multiple steps. (can have procedural logic
													, multiple statements,variable declarations)
-- Syntax of Multi-statement table-valued :  
CREATE FUNCTION dbo.SplitString(@Input NVARCHAR(MAX))
RETURNS @Result TABLE (Item NVARCHAR(100))
AS
BEGIN
    DECLARE @Pos INT = CHARINDEX(',', @Input)
    WHILE @Pos > 0
    BEGIN
        INSERT INTO @Result(Item) VALUES (LEFT(@Input, @Pos - 1))
        SET @Input = SUBSTRING(@Input, @Pos + 1, LEN(@Input))
        SET @Pos = CHARINDEX(',', @Input)
    END
    RETURN
END
*/

-- 22. Apply
SELECT * FROM dbo.GetTransactionsOverAmount(500)

GO

SELECT * , (SELECT COUNT(*) FROM dbo.GetTransactionsOverAmount(E.EmployeeNumber)) AS GottenNumOffTransaction
FROM tblEmployee AS E

/*
APPLY allows you to evaluate a table-valued function per row in the outer query (tblEmployee).
LEFT JOIN expects a fixed table, not something that depends on a per-row input.
*/
SELECT * FROM tblEmployee AS E
-- LEFT JOIN dbo.GetTransactionsOverAmount(E.EmployeeNumber) AS T -- istead of left join we outer apply 
OUTER APPLY  dbo.GetTransactionsOverAmount(E.EmployeeNumber) AS T
--ON E.EmployeeNumber = T.EmployeeNumber

SELECT *
FROM tblEmployee AS E
CROSS APPLY dbo.GetTransactionsOverAmount(E.EmployeeNumber) AS T

-- outer apply all of tblEmployee , UDF 0 + rows
-- cross apply UDF RETURN 1 + rows

-- OUTER APPLY = Like LEFT JOIN, includes all left rows
-- CROSS APPLY = Like INNER JOIN, excludes rows with no matches

--Using TVF in the WHERE close 
--For each employee, it counts the number of transactions returned by GetTransactionsOverAmount().
SELECT * 
FROM tblEmployee AS E
WHERE (SELECT COUNT(*) FROM dbo.GetTransactionsOverAmount(E.EmployeeNumber)) > 3

-- 23. Synonyms :
--Synonyms in T-SQL, feature for simplifying object references 
--especially when dealing with long names, remote servers, or complex schemas.

SELECT * FROM sys.views

--creating Local Table Synonym
CREATE SYNONYM EmployeeTable FOR tblEmployee;

--EmployeeTable becomes a friendly alias for tblEmployee
SELECT * FROM EmployeeTable;

CREATE SYNONYM DateTable FOR tblDate;
--SELECT * FROM DateTable; -- will not work becouse the is no tblDate object exists !

-- Check if the object tblDate exists
--IF OBJECT_ID('tblDate', 'U') IS NOT NULL
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'tblDate')
BEGIN
    -- If tblDate exists, select from DateTable (synonym)
    SELECT * FROM DateTable;
END
ELSE
BEGIN
    PRINT 'The object tblDate does not exist.';
END

-- creat remote Table Synonym
CREATE SYNONYM RemoteTable FOR OVERTHERE.70-461remote.dbo.tblRemote;-- Instead of always writing SELECT * FROM OVERTHERE.70-461remote.dbo.tblRemote;
SELECT * FROM RemoteTable;

--24. Dynamic Queries

select * from tblEmployee where EmployeeNumber = 129;
go

--Dynamic SQL with Hardcoded Command
declare @command as varchar(255);
set @command = 'select * from tblEmployee where EmployeeNumber = 129;'
set @command = 'select * from tblTransaction'
execute (@command);
go

--Dynamic SQL with Potential for SQL Injection
declare @command as varchar(255), @param as varchar(50);
set @command = 'select * from tblEmployee where EmployeeNumber = '
set @param = '129 or 1=1'
execute (@command + @param);  -- SQL Injection potential
go

--Mitigating SQL Injection with sp_executesql (safer approach of sp_executesql) 
declare @command as nvarchar(255), @param as nvarchar(50);
set @command = N'select * from tblEmployee where EmployeeNumber = @ProductID'
set @param = N'129'
--set @param = '129 or 1=1' --it will throw an Error converting data type nvarchar to int.
execute sys.sp_executesql @statement = @command, @params = N'@ProductID int', @ProductID = @param;
go
--Always using sp_executesql when working with dynamic SQL that includes user input

--The least likely to have SQL Injection problems is this version:
DECLARE @command AS NVARCHAR(255), @param AS NVARCHAR(50);

SET @command = N'SELECT * FROM tblEmployee WHERE EmployeeNumber = @ProductID'
SET @param = N'129'

EXECUTE sys.sp_executesql 
    @statement = @command, 
    @params = N'@ProductID INT', 
    @ProductID = @param;

--It supports parameterized queries
DECLARE @sql NVARCHAR(MAX) = N'SELECT * FROM tblEmployee WHERE EmployeeNumber = @uid';
EXEC sp_executesql @sql, N'@uid INT', @uid = 129;

--Avoids unsafe string concatenation
-- EXEC('SELECT * FROM Users WHERE UserId = ' + @userInput)

-- Example of injection:
--SET @userInput = '1; DROP TABLE Users; --'
--sp_executesql with parameters like using PreparedStatement in Java 

-- 25. Problems with IDENTITY
-- How to make the row uniqe
CREATE TABLE tblEmployee3 (
    EmployeeID INT IDENTITY(1,1),
    EmployeeName VARCHAR(50)
);

BEGIN TRAN;
INSERT INTO tblEmployee3 VALUES ('New Name');
SELECT * FROM  tblEmployee3;
ROLLBACK TRAN;

--DELETE from tblEmployee3 
--TRUNCATE TABLE tblEmployee3;

BEGIN TRAN;
INSERT INTO tblEmployee3 VALUES ('Mbarek'); -- Identity = 1
SELECT * FROM  tblEmployee3;
ROLLBACK TRAN;

INSERT INTO tblEmployee3 VALUES ('Saidwww');   -- Identity = 2 (not 1)

--26 GUIDs (uniqueidentifier) in SQL Server using NEWID() and NEWSEQUENTIALID()

DECLARE @newvalue AS uniqueidentifier  
SET @newvalue = NEWID()   --Generates a completely random GUID. Good for uniqueness, not for indexing.
SELECT @newvalue AS TheNewID  

--Random seed from time:
DECLARE @randomnumbergenerator INT = 
    DATEPART(MILLISECOND, SYSDATETIME()) +
    1000 * (
        DATEPART(SECOND, SYSDATETIME()) +
        60 * (DATEPART(MINUTE, SYSDATETIME()) + 60 * DATEPART(HOUR, SYSDATETIME()))
    );

SELECT RAND(@randomnumbergenerator) AS RandomNumber;

--Creating and testing a table with NEWID()
CREATE TABLE tblEmployee4 (
    UniqueID uniqueidentifier CONSTRAINT df_tblEmployee4_UniqueID DEFAULT NEWID(),
    EmployeeNumber INT CONSTRAINT uq_tblEmployee4_EmployeeNumber UNIQUE
)

--Creating and testing a table with NEWSEQUENTIALID()
--CONSTRAINT is a keyword used to define rules on columns or tables to enforce data integrity
CREATE TABLE tblEmployee5 (
    UniqueID uniqueidentifier CONSTRAINT df_tblEmployee4_UniqueID DEFAULT NEWSEQUENTIALID(),
    EmployeeNumber INT CONSTRAINT uq_tblEmployee4_EmployeeNumber UNIQUE
)

--NEWSEQUENTIALID() can only be used as a column default
SET @newvalue = NEWSEQUENTIALID() --Not allowed in T-SQL unless used as a default
--NEWSEQUENTIALID() : when GUID column is a primary key or index
--NEWID()			: if pure randomness is more important than index performance.

--DEFAULT NEWSEQUENTIALID() —> This auto-generates a sequential GUID whenever a row is inserted
BEGIN TRAN
CREATE TABLE tblEmployee6 (
    UniqueID uniqueidentifier 
        CONSTRAINT df_tblEmployee6_UniqueID DEFAULT NEWSEQUENTIALID(),
    EmployeeNumber int 
        CONSTRAINT uq_tblEmployee6_EmployeeNumber UNIQUE
)
INSERT INTO tblEmployee6(EmployeeNumber)
VALUES (1), (2), (3)
SELECT * FROM tblEmployee6
ROLLBACK TRAN

/* result 
	F2529DA5-F52E-F011-A2C0-047BCBB46567	1
	F3529DA5-F52E-F011-A2C0-047BCBB46567	2
	F4529DA5-F52E-F011-A2C0-047BCBB46567	3
*/

-- 27. Defining SEQUENCES
BEGIN TRAN  -- A sequence object of type BIGINT
CREATE SEQUENCE newSeq AS BIGINT --It starts at 1
START WITH 1    
INCREMENT BY 1 -- Each new number goes up by 1
MINVALUE 1  -- The smallest number it can produce
MAXVALUE 999999
CYCLE
CACHE 50 --SQL Server will pre-allocate 50 values for performance
CREATE SEQUENCE secondSeq AS INT
SELECT * FROM sys.sequences  --to see metadata about all sequence objects in the database
ROLLBACK TRAN

--Sequences vs Identity
| Feature            | IDENTITY       | SEQUENCE                   |
| ------------------ | -------------- | -------------------------- |
| Table-bound        | Yes            | No (global object)         |
| Manual access      | No (auto only) | Yes (via `NEXT VALUE FOR`) |
| Reusability        | No             | Yes                        |
| CYCLE option       | No             | Yes                        |
| Gaps (restartable) | Hard           | Easier to restart/reset    |


--28. NEXT VALUE FOR sequence
--Create and Use a SEQUENCE Temporarily
BEGIN TRAN
CREATE SEQUENCE newSeq AS BIGINT
START WITH 1
INCREMENT BY 1
MINVALUE 1
CACHE 50
select NEXT VALUE FOR newSeq as NextValue;
ROLLBACK TRAN --NextValue always 1

--. start with Creating Sequence to Use in a Table
CREATE SEQUENCE newSeq AS BIGINT
START WITH 1
INCREMENT BY 1
MINVALUE 1
CACHE 50

--Add a Column to tblTransaction That Uses the Sequence
ALTER TABLE tblTransaction
ADD NextNumber INT CONSTRAINT DF_Transaction DEFAULT NEXT VALUE FOR newSeq

--Dropping and Re-adding the Column and Constraint
ALTER TABLE tblTransaction DROP DF_Transaction
ALTER TABLE tblTransaction DROP COLUMN NextNumber
ALTER TABLE tblTransaction ADD NextNumber INT
ALTER TABLE tblTransaction ADD CONSTRAINT DF_Transaction DEFAULT NEXT VALUE FOR newSeq FOR NextNumber

--Insert a Row and Populate the Sequence Value
BEGIN TRAN
SELECT * FROM tblTransaction
INSERT INTO tblTransaction (Amount, DateOfTransaction, EmployeeNumber)
VALUES (1, '2017-01-01', 123)
SELECT * FROM tblTransaction WHERE EmployeeNumber = 123
--new row is added, then the NextNumber is set manually using NEXT VALUE FOR
UPDATE tblTransaction
SET NextNumber = NEXT VALUE FOR newSeq
WHERE NextNumber IS NULL

SELECT * FROM tblTransaction
ROLLBACK TRAN

-- end with resetting and Cleaning Up
ALTER SEQUENCE newSeq RESTART WITH 1

ALTER TABLE tblTransaction DROP DF_Transaction
ALTER TABLE tblTransaction DROP COLUMN NextNumber
DROP SEQUENCE newSeq

--Summary
| Feature                | Purpose                                                                |
| ---------------------- | ---------------------------------------------------------------------- |
| SEQUENCE               | Generates unique numeric values, reusable across tables or procedures. |
| NEXT VALUE FOR         | Gets the next number in the sequence.                                  |
| DEFAULT NEXT VALUE FOR | Automatically assigns next sequence value on insert.                   |
| ALTER SEQUENCE RESTART | Resets sequence value (like reseeding).                                |
| ROLLBACK TRAN          | Keeps your DB clean during experiments.                                |

--31. Introducing XML
DECLARE @xml XML
SET @xml = '<Shopping ShopperName="Phillip Burton" Weather="Nice">
  <ShoppingTrip ShoppingTripID="L1">
    <Item Cost="5">Bananas</Item>
    <Item Cost="4">Apples</Item>
    <Item Cost="3">Cherries</Item>
  </ShoppingTrip>
  <ShoppingTrip ShoppingTripID="L2">
    <Item>Emeralds</Item>
    <Item>Diamonds</Item>
    <Item>Furniture</Item>
  </ShoppingTrip>
</Shopping>'
select @xml

--ALTER TABLE dbo.tblEmployee
--ADD XMLOutput XML;

UPDATE [dbo].[tblEmployee]
SET XMLOutput = @xml
WHERE EmployeeNumber = 200

--INSERT INTO dbo.tblEmployee (EmployeeNumber, XMLOutput)
--VALUES (200, @xml);

SELECT * FROM [dbo].[tblEmployee]

-- Get shopper name and weather
SELECT 
  XMLOutput.value('(/Shopping/@ShopperName)[1]', 'VARCHAR(50)') AS ShopperName,
  XMLOutput.value('(/Shopping/@Weather)[1]', 'VARCHAR(50)') AS Weather
FROM dbo.tblEmployee
WHERE EmployeeNumber = 200;

--32. FOR XML RAW
--ALTER TABLE dbo.tblEmployee
--DROP COLUMN XMLOutput ;
SELECT 
    E.EmployeeNumber, 
    E.EmployeeFirstName, 
    E.EmployeeLastName,
    E.DateOfBirth, 
    T.Amount, 
    T.DateOfTransaction
FROM [dbo].[tblEmployee] AS E
LEFT JOIN [dbo].[tblTransaction] AS T
    ON E.EmployeeNumber = T.EmployeeNumber
WHERE E.EmployeeNumber BETWEEN 200 AND 202
--ELEMENTS Forces SQL Server to render each column as a child element, not an attribute.
FOR XML RAW('MyRow'), ELEMENTS

--33. FOR XML AUTO
--  AUTO Mode Automatically names the XML elements after the table aliases in the query (E, T).
SELECT 
    E.EmployeeNumber, 
    E.EmployeeFirstName, 
    E.EmployeeLastName,
    E.DateOfBirth, 
    T.Amount, 
    T.DateOfTransaction
FROM [dbo].[tblEmployee] AS E
LEFT JOIN [dbo].[tblTransaction] AS T
    ON E.EmployeeNumber = T.EmployeeNumber
WHERE E.EmployeeNumber BETWEEN 200 AND 202
FOR XML AUTO, ELEMENTS
--AUTO is ideal when you want nested XML where joined rows (like transactions) are children of a parent row (like employee).
--This is very useful for APIs, XML exports, or integrations.

--AUTO vs RAW
| Feature       | FOR XML AUTO                       | FOR XML RAW                         |
| ------------- | ---------------------------------- | ----------------------------------- |
| Structure     | Hierarchical, based on table joins | Flat list (1 XML element per row)   |
| Element Names | Derived from table aliases         | Manually specified (RAW('MyRow')) |
| Nested Tags   | Yes (parent-child based on joins)  | No – flat unless manually handled   |


--34. FOR XML PATH
--FOR XML PATH('Employees'): Each row becomes an <Employees> element.
--ROOT('MyXML'): Wraps the entire XML result with a root node <MyXML>
--Attributes: Prefixed with @EmployeeFirstName becomes an XML attribute of <Employees>.
SELECT 
    E.EmployeeFirstName AS '@EmployeeFirstName',
    E.EmployeeLastName AS '@EmployeeLastName',
    E.EmployeeNumber,
    E.DateOfBirth,
    T.Amount AS 'Transaction/Amount',
    T.DateOfTransaction AS 'Transaction/DateOfTransaction'
FROM [dbo].[tblEmployee] AS E
LEFT JOIN [dbo].[tblTransaction] AS T
    ON E.EmployeeNumber = T.EmployeeNumber
WHERE E.EmployeeNumber BETWEEN 200 AND 202
FOR XML PATH('Employees'), ROOT('MyXML');

--35. FOR XML EXPLICIT
-- FOR XML EXPLICIT, which gives fine grained control over XML structure 
-- but it's also the most complex and error prone of the XML modes in T-SQL.
-- Alternative: FOR XML PATH (Much Easier)
-- Tag and Parent: Define the XML hierarchy.
SELECT
    1 AS Tag,
    NULL AS Parent, 
    E.EmployeeFirstName AS [Elements!1!EmployeeFirstName],
    E.EmployeeLastName AS [Elements!1!EmployeeLastName],
    E.EmployeeNumber AS [Elements!1!EmployeeNumber],
    E.DateOfBirth AS [Elements!1!DateOfBirth],
    NULL AS [Elements!2!Amount],
    NULL AS [Elements!2!DateOfTransaction]
FROM dbo.tblEmployee AS E
WHERE E.EmployeeNumber BETWEEN 200 AND 202

UNION ALL

SELECT
    2 AS Tag,    -- Tag = 1 ->  parent row (Employee)
    1 AS Parent, -- Tag = 2 ->  child row (Transaction)
    NULL,
    NULL,
    T.EmployeeNumber,
    NULL,
    T.Amount,
    T.DateOfTransaction
FROM dbo.tblTransaction AS T
INNER JOIN dbo.tblEmployee AS E ON T.EmployeeNumber = E.EmployeeNumber
WHERE T.EmployeeNumber BETWEEN 200 AND 202
----If the column alias [Elements!2!Amount] is in the first query, it should also appear in the second (and vice versa).
ORDER BY EmployeeNumber, [Elements!2!Amount]
FOR XML EXPLICIT;

--35. XQuery Value method
declare @xml xml
set @xml='<Shopping ShopperName="M barek " >
	<ShoppingTrip ShoppingTripID="L1" >
		<Item Cost="5">Bananas</Item>
		<Item Cost="4">Apples</Item>
		<Item Cost="3">Cherries</Item>
	</ShoppingTrip>
	<ShoppingTrip ShoppingTripID="L2" >
		<Item>Emeralds</Item>
		<Item>Diamonds</Item>
		<Item>Furniture</Item>
	</ShoppingTrip>
</Shopping>'
select @xml

-- XQuery .value() Method Syntax: xml_variable.value('XQuery_expression', 'SQL_data_type') to retrieve the data from xml
SELECT @xml.value('(/Shopping/ShoppingTrip/Item/@Cost)[1]', 'varchar(50)')

-- All Item Names and Costs:
SELECT 
    Items.value('.', 'varchar(50)') AS ItemName,
    Items.value('@Cost', 'varchar(50)') AS Cost
FROM @xml.nodes('/Shopping/ShoppingTrip/Item') AS X(Items)

--36. XQuery Modify method
DECLARE @xml XML;
SET @xml = '
<Shopping ShopperName="Mbarek">
  <ShoppingTrip ShoppingTripID="L1">
    <Item Cost="5">Bananas</Item>
    <Item Cost="4">Apples</Item>
    <Item Cost="3">Cherries</Item>
  </ShoppingTrip>
  <ShoppingTrip ShoppingTripID="L2">
    <Item>Emeralds</Item>
    <Item>Diamonds</Item>
    <Item>Furniture</Item>
  </ShoppingTrip>
</Shopping>';

-- Display the original XML
SELECT @xml AS OriginalXML;

-- Modify the third Item in the first ShoppingTrip
-- Apply the update with SET
SET @xml.modify('
  replace value of (/Shopping/ShoppingTrip[1]/Item[3]/@Cost)[1]
  with "6.0"
');
-- Display the modified XML
SELECT @xml AS ModifiedXML;

--Insert a new <Item> into the second <ShoppingTrip>
SET @xml.modify('
  insert <Item Cost="5">New Food</Item>
  into (/Shopping/ShoppingTrip)[2]
');

-- View the final result
SELECT @xml AS FinalXML;


--37. XQuery Query and FLWOR 1  (For-Let-Where-Order-Return) 
-- Return each <Item> element
SELECT @xml.query('
  for $ValueRetrieved in /Shopping/ShoppingTrip/Item
  return $ValueRetrieved
');

--Return just the text content of each item
SELECT @xml.query('
  for $ValueRetrieved in /Shopping/ShoppingTrip/Item
  return string($ValueRetrieved)
');
--concatinat string 
select '1'+ '1'

-- Return text from first ShoppingTrip only, with a semicolon separator
SELECT @xml.query('
  for $ValueRetrieved in /Shopping/ShoppingTrip[1]/Item
  return concat(string($ValueRetrieved), ";")
'); -- -> Bananas; Apples; Cherries;

| Query                      | Purpose                         | Output            |
| -------------------------- | ------------------------------- | ------------------|
| $ValueRetrieved            | Returns whole nodes             | Full XML elements |
| string($ValueRetrieved)    | Returns text inside nodes       | Plain text        |
| concat(string(...), ";")   | Appends string with separator   | Text with ;       |


--39. nodes using Variable (shredding a variable)
DECLARE @xml XML;
SET @xml = '
<Shopping ShopperName="Mbarek">
  <ShoppingTrip ShoppingTripID="L1">
    <Item Cost="5">Bananas</Item>
    <Item Cost="4">Apples</Item>
    <Item Cost="3">Cherries</Item>
  </ShoppingTrip>
  <ShoppingTrip ShoppingTripID="L2">
    <Item>Emeralds</Item>
    <Item>Diamonds</Item>
    <Item>Furniture</Item>
  </ShoppingTrip>
</Shopping>';
select tbl.col.value('.', 'varchar(50)') as Item,
       tbl.col.value('@Cost','varchar(50)') as Cost
into tblTemp
from @xml.nodes('/Shopping/ShoppingTrip/Item') as tbl(col)

select * from tblTemp
drop table tblTemp

| Feature       | .nodes() Approach                     | FLWOR (XQuery)                                 |
| ------------- | ------------------------------------- | ---------------------------------------------- |
| Output format | Relational rows (T-SQL table)         | XML (or string/XML fragments)                  |
| Syntax style  | T-SQL with XML methods                | XQuery (more declarative)                      |
| Use case      | Easy data extraction and joins        | Complex filtering, formatting, and nesting     |
| Performance   | Usually faster and easier to optimize | Slightly more overhead, used for formatted XML |

--40. notes using table (shredding a table)
declare @x1 xml, @x2 xml
set @x1 = '<Shopping ShopperName="Phillip Burton">
  <ShoppingTrip ShoppingTripID="L1">
    <Item Cost="5">Bananas</Item>
    <Item Cost="4">Apples</Item>
    <Item Cost="3">Cherries</Item>
  </ShoppingTrip>
</Shopping>'
set @x2 = '<Shopping ShopperName="Phillip Burton">
  <ShoppingTrip ShoppingTripID="L2">
    <Item>Emeralds</Item>
    <Item>Diamonds</Item>
    <Item>Furniture</Item>
  </ShoppingTrip>
</Shopping>'

create table #tblXML(pkXML INT PRIMARY KEY, xmlCol XML)
insert into #tblXML(pkXML, xmlCol) VALUES (1, @x1)
insert into #tblXML(pkXML, xmlCol) VALUES (2, @x2)

select tbl.col.value('@Cost','varchar(50)')
from #tblXML
CROSS APPLY xmlCol.nodes('/Shopping/ShoppingTrip/Item') as tbl(col)

select t.pkXML,
       tbl.col.value('.', 'varchar(50)') as Item,
       tbl.col.value('@Cost','varchar(50)') as Cost
from #tblXML t
CROSS APPLY t.xmlCol.nodes('/Shopping/ShoppingTrip/Item') as tbl(col)

| Concept                 | Meaning                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| xmlCol.nodes(...)       | Selects multiple XML nodes as a rowset                                 |
| CROSS APPLY             | Applies a function/table-valued result per each row of the outer table |
| .value('@Attr', type)   | Extracts attribute value                                               |
| .value('.', type)       | Extracts node inner text                                               |

--41. Importing and exporting XML using the bcp utility (Bulk Copy Program) 
--Exporting data from a SQL Server table to a file
--Importing data from a file into a SQL Server table
--bcp [70-461].dbo.tblDepartment out mydata.out -N -T

| Part                            | Meaning                                                   |
| ------------------------------- | --------------------------------------------------------- |
|  bcp                            | Starts the Bulk Copy operation                            |
|  [70-461S5].dbo.tblDepartment   | Database and table to export                              |
|  out mydata.out                 | Export to a file named "mydata.out"                       |
|  -N                             | Use **native format** (best for SQL Server-to-SQL Server) |
|  -T                             | Use a **trusted connection** (Windows Authentication)     |

-- to Create the Destination Table (It must match the structure of the source table (tblDepartment) )
create table dbo.tblDepartment2
(
  [Department] varchar(19) null,
  [DepartmentHead] varchar(19) null
)

-- 42. Bulk Insert and Openrowset
DROP TABLE #tblXML;
CREATE TABLE #tblXML(XmlCol XML);

BULK INSERT #tblXML
FROM 'C:\xml\SampleDataBulkInsert.txt';

SELECT * FROM #tblXML;
DROP TABLE #tblXML;

--OPENROWSET + BULK (Better for single XML)
DROP TABLE #tblXML;
CREATE TABLE #tblXML(XmlCol XML);

BULK INSERT #tblXML
FROM 'C:\XML\SampleDataBulkInsert.txt';

SELECT * FROM #tblXML;
DROP TABLE #tblXML;

--OPENROWSET + BULK (Better for single XML)
CREATE TABLE #tblXML(IntCol INT IDENTITY(1,1), XmlCol XML);
INSERT INTO #tblXML(XmlCol)
SELECT * FROM OPENROWSET(
    BULK 'C:\XML\SampleDataOpenRowset.txt',
    SINGLE_BLOB
) AS x;

SELECT * FROM #tblXML;

--Alternative: Use OPENROWSET (Better! )
SELECT * FROM OPENROWSET(
    BULK 'C:\xml\SampleDataBulkInsert.txt',
    SINGLE_BLOB
) AS x;

--To find the SQL Server service account:
SELECT servicename, service_account
FROM sys.dm_server_services;

-- 43. Schema :
SELECT 
    E.EmployeeNumber, 
    E.EmployeeFirstName, 
    E.EmployeeLastName,
    T.Amount, 
    T.DateOfTransaction
FROM [dbo].[tblEmployee] AS E
LEFT JOIN [dbo].[tblTransaction] AS T
    ON E.EmployeeNumber = T.EmployeeNumber
WHERE E.EmployeeNumber BETWEEN 200 AND 202
FOR XML RAW, XMLSCHEMA;

--FOR XML RAW: Returns rows as raw XML elements (<row ... />).
--XMLSCHEMA: Appends an in-line XML Schema Definition (XSD) at the top of the output.

| XML Type               | Meaning                        | SQL Type Equivalent                      |
| ---------------------- | ------------------------------ | ---------------------------------------- |
| i4 or int              | Integer (whole number)         | int, smallint                            |
| boolean                | Logical true/false (0/1)       | bit                                      |
| dateTime.iso8601       | ISO 8601 datetime              | datetime, smalldatetime, datetime2       |
| double                 | Double-precision float         | float, real                              |
| string                 | Character string               | varchar, nvarchar, text                  |
| nil (with xsi:nil)     | Explicit NULL                  | null                                     |


-- 46. XML Indexes 

--  1: Declare and Set XML Variables
declare @x1 xml, @x2 xml

set @x1 = '<Shopping ShopperName="Phillip Burton">
  <ShoppingTrip ShoppingTripID="L1">
    <Item Cost="5">Bananas</Item>
    <Item Cost="4">Apples</Item>
    <Item Cost="3">Cherries</Item>
  </ShoppingTrip>
</Shopping>'

set @x2 = '<Shopping ShopperName="Phillip Burton">
  <ShoppingTrip ShoppingTripID="L2">
    <Item>Emeralds</Item>
    <Item>Diamonds</Item>
    <Item>Furniture<Color></Color></Item>
  </ShoppingTrip>
</Shopping>'

--  2: Create Temporary Table (Drop if it already exists)
if object_id('tempdb..#tempTblXML') is not null
    drop table #tempTblXML;

create table #tempTblXML (
  pkXML INT PRIMARY KEY,
  xmlCol XML
)

--  3: Insert XML Data into Temporary Table
insert into #tempTblXML(pkXML, xmlCol) values (1, @x1)
insert into #tempTblXML(pkXML, xmlCol) values (2, @x2)

--  4: Create Primary XML Index
create primary xml index pk_tempTblXML on #tempTblXML(xmlCol)
go

--  5: Create Secondary XML Indexes
-- Note: These must be in a separate batch (after GO)

-- a. PATH Index: Speeds up path-based XQuery (e.g., using .exist or .query)
create xml index secpk_tempTblXML_Path on #tempTblXML(xmlCol)
using xml index pk_tempTblXML for PATH

-- b. VALUE Index: Speeds up queries that retrieve specific values via .value() or .exist()
create xml index secpk_tempTblXML_Value on #tempTblXML(xmlCol)
using xml index pk_tempTblXML for VALUE

-- c. PROPERTY Index: Optimizes access to XML typed with XML schema (not strictly needed for untyped XML)
create xml index secpk_tempTblXML_Property on #tempTblXML(xmlCol)
using xml index pk_tempTblXML for PROPERTY

--  6: Example Query using PATH index
-- Retrieves rows where an <Item> has an attribute Cost="5"
select pkXML
from #tempTblXML
where xmlCol.exist('/Shopping/ShoppingTrip/Item[@Cost="5"]') = 1


-- 47. JSON in SQL Server 
-- Declare and Set JSON Variable
declare @json NVARCHAR(4000)

set @json = '
{
  "name": "M barek",
  "ShoppingTrip": {
    "ShoppingTripItem": "L1",
    "Items": [
      {"Item":"Bananas", "Cost":5},
      {"Item":"Apples", "Cost":4},
      {"Item":"Cherries", "Cost":3}
    ]
  }
}'

-- Validate JSON Format: Returns 1 if valid, 0 if not
select isjson(@json)

-- Extract Data with JSON_VALUE
-- Case-sensitive key; this will return NULL
select json_value(@json, '$."Name"')         

-- Correct: Key is "name", not "Name"
select json_value(@json, 'strict $.name')     -- Returns: M barek

-- Extract Specific Item in Array
-- Returns the second item's name ("Apples")
select json_value(@json, 'strict $.ShoppingTrip.Items[1].Item')

-- Modify the second item's name to "Big Bananas"
select json_modify(@json, 'strict $.ShoppingTrip.Items[1].Item', 'Big Bananas')

-- Replace the entire second item object
select json_modify(@json, 'strict $.ShoppingTrip.Items[1]', '{"Item":"Big Apples", "Cost":1}')

-- Same as above, using JSON_QUERY to treat the new value as a JSON object
select json_modify(@json, 'strict $.ShoppingTrip.Items[1]', json_query('{"Item":"Big Apples", "Cost":1}'))

-- Add a new property "Date" at the root level
select json_modify(@json, '$.Date', '2022-01-01')

-- Parse JSON with OPENJSON
-- Returns key-value pairs from the top-level JSON object
select * from openjson(@json)

-- Returns each item in the Items array as a row (as raw JSON)
select * from openjson(@json, '$.ShoppingTrip.Items')

-- Parse and return tabular data (structured columns)
select * from openjson(@json, '$.ShoppingTrip.Items')
with (
  Item varchar(10),
  Cost int
)

-- Convert a SQL table into a JSON object
select 'Bananas' as Item, 5 as Cost
union
select 'Apples', 4
union
select 'Cherries', 3
for json path, root('MyShoppingList')

-- Expected output:
-- {
--   "MyShoppingTrip": [
--     { "Item": "Bananas", "Cost": 5 },
--     { "Item": "Apples", "Cost": 4 },
--     { "Item": "Cherries", "Cost": 3 }
--   ]
-- }

-- Summary of JSON Functions:

-- | Function        | Purpose                                |
-- |-----------------|----------------------------------------|
-- | ISJSON()        | Checks if string is valid JSON         |
-- | JSON_VALUE()    | Extracts a scalar value                |
-- | JSON_QUERY()    | Extracts a full object or array        |
-- | JSON_MODIFY()   | Modifies or adds elements to JSON      |
-- | OPENJSON()      | Parses JSON into a table               |
-- | FOR JSON PATH   | Converts SQL result set into JSON      |

DECLARE @json2 NVARCHAR(MAX) = '
{
  "Items": [
    {"Item":"Bananas", "Cost":5},
    {"Item":"Apples", "Cost":4},
    {"Item":"Cherries", "Cost":3}
  ]
}'

-- Extract structured data from the JSON array
SELECT *
FROM OPENJSON(@json2, '$.Items')
/*
The WITH clause you're referring to is used in SQL Server with the OPENJSON function 
to define a schema (i.e., a table structure) for the JSON data being parsed.*/
WITH (
  Item VARCHAR(10),
  Cost INT
)

-- 48 Temporal Table (also called a system-versioned table) 
/*
    Create a temporal table to track employee records over time.
    A temporal table automatically keeps a history of all changes.
*/

CREATE TABLE [dbo].[tblEmployeeTemporal2] (
    -- Basic employee data
    [EmployeeNumber] INT NOT NULL PRIMARY KEY CLUSTERED,         -- Unique employee ID
    [EmployeeFirstName] VARCHAR(50) NOT NULL,                    
    [EmployeeMiddleName] VARCHAR(50) NULL,                       
    [EmployeeLastName] VARCHAR(50) NOT NULL,                    
    [EmployeeGovernmentID] CHAR(10) NOT NULL,                    
    [DateOfBirth] DATE NOT NULL,                                 
    [Department] VARCHAR(19) NULL,                              

    -- Temporal system-versioning columns
    [ValidFrom] DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL,  -- Row valid from
    [ValidTo] DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL,      -- Row valid to

    -- Declare system time period for versioning
    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])
)
WITH (
    SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[tblEmployeeHistory2])  -- Enable versioning and specify history table
);

--To make temporal columns hidden in SQL Server
CREATE TABLE [dbo].[tblEmployeeTemporalColHidden] (
    [EmployeeNumber] INT NOT NULL PRIMARY KEY CLUSTERED,
    [EmployeeFirstName] VARCHAR(50) NOT NULL,
    [EmployeeMiddleName] VARCHAR(50) NULL,
    [EmployeeLastName] VARCHAR(50) NOT NULL,
    [EmployeeGovernmentID] CHAR(10) NOT NULL,
    [DateOfBirth] DATE NOT NULL,
    [Department] VARCHAR(19) NULL,

    -- Hidden system-versioned columns
    [ValidFrom] DATETIME2(2) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
    [ValidTo] DATETIME2(2) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,

    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])
)
WITH (
    SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[tblEmployeeHistory])
);

--HIDDEN hides the column from SELECT *, but you can still reference it explicitly:
--SELECT ValidFrom, ValidTo FROM dbo.tblEmployeeTemporal;
SELECT * FROM dbo.[tblEmployeeTemporalColHidden];

GO

/*
    Insert initial employee records into the temporal table.
    These represent the current valid data tracked over time.
*/
INSERT INTO [dbo].[tblEmployeeTemporal2]
([EmployeeNumber], [EmployeeFirstName], [EmployeeMiddleName], [EmployeeLastName],
 [EmployeeGovernmentID], [DateOfBirth], [Department])
VALUES
	(123, 'Jane', NULL, 'Zwilling', 'AB123456G', '1985-01-01', 'Customer Relations'),
	(124, 'Carolyn', 'Andrea', 'Zimmerman', 'AB234578H', '1975-06-01', 'Commercial'),
	(125, 'Jane', NULL, 'Zabokritski', 'LUT778728T', '1977-12-09', 'Commercial'),
	(126, 'Ken', 'J', 'Yukish', 'PO201903O', '1969-12-27', 'HR'),
	(127, 'Terri', 'Lee', 'Yu', 'ZH206496W', '1986-11-14', 'Customer Relations'),
	(128, 'Roberto', NULL, 'Young', 'EH793082D', '1967-04-05', 'Customer Relations');

GO

-- View the current data in the temporal table
SELECT * FROM dbo.tblEmployeeTemporal2;

GO

/*
    Update the employee's last name.
    This will cause SQL Server to archive the previous version in the history table.
*/
UPDATE [dbo].[tblEmployeeTemporal2]
SET EmployeeLastName = 'Smith'
WHERE EmployeeNumber = 124;

GO

-- Second update to the same employee - previous row archived again
UPDATE [dbo].[tblEmployeeTemporal2]
SET EmployeeLastName = 'Albert'
WHERE EmployeeNumber = 124;

GO

-- View the current version of the data
SELECT * FROM dbo.tblEmployeeTemporal2;

-- Optional: view history data
SELECT * FROM dbo.tblEmployeeHistory2;

GO

/*
    Before dropping the table, disable system versioning first.
    This removes the system-versioning linkage between current and history tables.
*/
ALTER TABLE [dbo].[tblEmployeeTemporal2]
SET (SYSTEM_VERSIONING = OFF);

GO

-- Drop the main temporal table
DROP TABLE [dbo].[tblEmployeeTemporal2];

-- Drop the associated history table
DROP TABLE [dbo].[tblEmployeeHistory2];


--49 Alter Existing Table to Temporal Table
-- Add temporal columns with constraints and default values
ALTER TABLE [dbo].[tblEmployee]
ADD
    [ValidFrom] DATETIME2(2) GENERATED ALWAYS AS ROW START 
        CONSTRAINT [DF_tblEmployee_ValidFrom] DEFAULT SYSUTCDATETIME(),

    [ValidTo] DATETIME2(2) GENERATED ALWAYS AS ROW END 
        CONSTRAINT [DF_tblEmployee_ValidTo] DEFAULT CONVERT(DATETIME2(2), '9999-12-31 23:59:59'),

    PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo]);

-- Step 2: Enable system versioning and specify the history table
ALTER TABLE [dbo].[tblEmployee]
SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[tblEmployeeHistory2]));
--Error -> Msg 13575, Level 16, State 0, Line 2476
-- ADD PERIOD FOR SYSTEM_TIME failed because table '70-461.dbo.tblEmployee' 
-- contains records where end of period is not equal to MAX datetime.

-- to fix the Problem SQL Server requires all existing rows to follow temporal rules, 

-- Manually add ValidFrom and ValidTo (if not already added)
ALTER TABLE [dbo].[tblEmployee]
ADD 
    ValidFrom DATETIME2(2) NOT NULL DEFAULT SYSUTCDATETIME(),
    ValidTo   DATETIME2(2) NOT NULL DEFAULT CONVERT(DATETIME2(2), '9999-12-31 23:59:59');

--Update existing rows to match system-versioned requirements
UPDATE [dbo].[tblEmployee]
SET ValidTo = CONVERT(DATETIME2(2), '9999-12-31 23:59:59');

-- Convert the columns into system-versioned temporal columns
ALTER TABLE [dbo].[tblEmployee]
DROP CONSTRAINT [DF_tblEmployee_ValidFrom];

ALTER TABLE [dbo].[tblEmployee]
DROP CONSTRAINT [DF_tblEmployee_ValidTo];


ALTER TABLE [dbo].[tblEmployee]
ALTER COLUMN ValidFrom DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL;

ALTER TABLE [dbo].[tblEmployee]
ALTER COLUMN ValidTo   DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL;

ALTER TABLE [dbo].[tblEmployee]
ADD PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo);

--Enable system versioning
ALTER TABLE [dbo].[tblEmployee]
SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [dbo].[tblEmployeeHistory2]));

-- Full Safe Migration via New Table
--after Msg 13589, Level 16, State 1, Line 2513 known as limitation in SQL Server:
--Column 'ValidFrom' in table '70-461.dbo.tblEmployee' cannot be specified as 'GENERATED ALWAYS' in ALTER COLUMN statement.
-- Step 1: Create the temporal-ready table
CREATE TABLE dbo.tblEmployeeNew (
    EmployeeNumber INT NOT NULL PRIMARY KEY,
    EmployeeFirstName VARCHAR(50) NOT NULL,
    EmployeeMiddleName VARCHAR(50) NULL,
    EmployeeLastName VARCHAR(50) NOT NULL,
    EmployeeGovernmentID CHAR(10) NOT NULL,
    DateOfBirth DATE NOT NULL,
    Department VARCHAR(19) NULL,

    ValidFrom DATETIME2(2) GENERATED ALWAYS AS ROW START NOT NULL,
    ValidTo DATETIME2(2) GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
)
WITH (
    SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tblEmployeeHistory)
);

-- Step 2: Copy data from old table (excluding the system-time columns)
INSERT INTO dbo.tblEmployeeNew (
    EmployeeNumber, EmployeeFirstName, EmployeeMiddleName, EmployeeLastName,
    EmployeeGovernmentID, DateOfBirth, Department
)
SELECT
    EmployeeNumber, EmployeeFirstName, EmployeeLastName,
    EmployeeGovernmentID, DateOfBirth, Department
FROM dbo.tblEmployee;

-- EXEC sp_rename 'dbo.tblEmployee', 'tblEmployeeOld';
-- EXEC sp_rename 'dbo.tblEmployeeNew', 'tblEmployee';

--Querying temporal data at a point of time
SELECT * 
FROM sys.tables 
WHERE name = 'tblEmployeeTemporal2';

SELECT * 
FROM dbo.tblEmployeeTemporal2
FOR SYSTEM_TIME
FROM '2020-01-01' TO '2024-01-01'
WHERE EmployeeNumber = 124;

SELECT * 
FROM dbo.tblEmployeeTemporal2
--FOR SYSTEM_TIME AS OF '2021-02-01'
FOR SYSTEM_TIME AS OF '2021-02-01T00:00:00';

SELECT *  
FROM dbo.tblEmployeeTemporal2  
FOR SYSTEM_TIME FROM '2020-01-01' TO '2021-02-15'
WHERE EmployeeNumber = 124;

--Querying temporal data between time periods
--FROM ... TO
SELECT *  
FROM dbo.tblEmployeeTemporal2  
FOR SYSTEM_TIME  
FROM '2020-01-01T00:00:00' TO '2026-02-15T00:00:00';

--BETWEEN ... AND
SELECT *  
FROM dbo.tblEmployeeTemporal2  
FOR SYSTEM_TIME  
BETWEEN '2020-01-01T00:00:00' AND '2026-02-15T00:00:00';

--CONTAINED IN (start, end)
SELECT *  
FROM dbo.tblEmployeeTemporal2  
FOR SYSTEM_TIME  
CONTAINED IN ('2020-01-01T00:00:00', '2026-02-15T00:00:00');

-- Add Filtering (WHERE)
SELECT *  
FROM dbo.tblEmployeeTemporal2  
FOR SYSTEM_TIME  
FROM '2020-01-01T00:00:00' TO '2026-02-01T00:00:00'
WHERE EmployeeNumber = 124;

--ALL
SELECT *  
FROM dbo.tblEmployeeTemporal2  
FOR SYSTEM_TIME ALL;

-- SESSION /7/
use [70-461S7]

-- Update without an explicit transaction (auto-commit)
update [dbo].[tblEmployee]
set EmployeeNumber = 123
where EmployeeNumber = 122;

-- View current data
select * from [dbo].[tblEmployee];

-- Check active transactions (should be 0, no open tran)
select @@TRANCOUNT;  -- Output: 0
--Begin Transactions
begin tran;
	select @@TRANCOUNT;  -- Output: 1 (1 transaction started)

	begin tran;
	-- SQL Server supports **nested transaction counters**, but not truly nested transactions.
	-- This just increases the counter, but there is still only one actual transaction.

			update [dbo].[tblEmployee]
			set EmployeeNumber = 122
			where EmployeeNumber = 123;

			select @@TRANCOUNT;  -- Output: 2 (counter increased)

	--Commit Only One Level
	commit tran;
	select @@TRANCOUNT;  --  Output: 1 (one level committed, but transaction is still open)

-- Conditionally Final Commit
if @@TRANCOUNT > 0
    commit tran;

select @@TRANCOUNT;  --  Output: 0 (fully committed, transaction closed)

select * from [dbo].[tblEmployee];


-- Transaction block using BEGIN TRY...END TRY and BEGIN CATCH...END CATCH

BEGIN TRY
    BEGIN TRAN;

    -- Sample update inside TRY block
    UPDATE [dbo].[tblEmployee]
    SET EmployeeNumber = 123
    WHERE EmployeeNumber = 122;

    -- Commit the transaction if no error
    COMMIT;
END TRY
BEGIN CATCH
    -- Rollback if a transaction is still open
    IF @@TRANCOUNT > 0
        ROLLBACK;

    -- Optional: Return detailed error information
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    
    -- Rethrow the error to propagate it up
    THROW;
END CATCH;

-- Final check: display table content
SELECT * FROM [dbo].[tblEmployee];

-- Scope and typs of locks waitfor statment
BEGIN TRAN
	SELECT * FROM [dbo].[tblEmployee]
COMMIT TRAN

--Scope of Locks(In SQL Server, locks are used to ensure transactional consistency and concurrency control.)

| Lock Scope     | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| Row-level      | Locks a single row. Very fine-grained; used for high concurrency.           |
| Page-level     | Locks an 8KB page (may include multiple rows).                              |
| Table-level    | Locks the entire table. Least concurrent; used when many rows are affected. |
| Key-level      | Applied in indexes to lock a specific key range.                            |
| Extent-level   | Locks 8 contiguous pages (rare).                                            |
| Database-level | Rare, only during operations like backups or maintenance.                   |

-- Types of Locks
--SQL Server uses different lock types depending on the operation:
| Lock Type           | Description                                                               |
| ------------------- | ------------------------------------------------------------------------- |
| Shared (S)          | For read operations (SELECT). Multiple shared locks can coexist.          |
| Exclusive (X)       | For write operations (INSERT, UPDATE, DELETE). Prevents other access.     |
| Update (U)          | Used during updates, to prevent deadlocks (intermediate between S and X). |
| Intent (IS, IX, IU) | Signifies intent to acquire a lower-level lock. Helps lock hierarchy.     |
| Schema              | Acquired when schema changes occur (DDL operations).                      |
| Bulk Update (BU)    | For bulk insert operations.                                               |

--WAITFOR Statement
--The WAITFOR statement delays execution. Useful for simulating locks or testing.

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

--Check Blocking: Who is blocking whom?
SELECT 
    blocking_session_id AS Blocker,
    session_id AS Blocked,
    wait_type, wait_time, wait_resource
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

/* 
 -- Create Table and Insert Data
*/

DROP TABLE IF EXISTS dbo.LockTest;
GO

CREATE TABLE dbo.LockTest (
    ID INT PRIMARY KEY,
    Name VARCHAR(50)
);

INSERT INTO dbo.LockTest (ID, Name)
VALUES (1, 'Alice'), (2, 'Bob');
GO


/*
 Blocking Simulation - Run in Session 1
*/

-- Begin a transaction and hold a lock on ID = 1
BEGIN TRAN;

UPDATE dbo.LockTest
SET Name = 'BlockingUser'
WHERE ID = 1;

-- Hold the lock for 1 minute (simulate long-running transaction)
WAITFOR DELAY '00:01:00';

-- After delay, commit manually to release lock
-- COMMIT;
GO


/*
 Blocking Test - Run in Session 2
*/

-- This will be BLOCKED until Session 1 commits
UPDATE dbo.LockTest
SET Name = 'BlockedUser'
WHERE ID = 1;
GO


/*
 Monitor Blocking - Run in Session 3
*/

-- Check blocking status (run repeatedly during blocking)
SELECT
    r.session_id AS BlockedSession,
    r.blocking_session_id AS BlockedBy,
    r.status,
    r.wait_type,
    r.wait_time,
    r.command
FROM sys.dm_exec_requests r
WHERE r.blocking_session_id <> 0;
GO


/*
 Deadlock Simulation - Use Two Sessions
*/

-- Session 1
BEGIN TRAN;
UPDATE dbo.LockTest SET Name = 'A1' WHERE ID = 1;
-- Wait for Session 2 to take a lock on ID = 2

-- Then:
UPDATE dbo.LockTest SET Name = 'A2' WHERE ID = 2; -- This will deadlock if Session 2 tries reverse
-- COMMIT;

-- Session 2
BEGIN TRAN;
UPDATE dbo.LockTest SET Name = 'B2' WHERE ID = 2;
-- Then:
UPDATE dbo.LockTest SET Name = 'B1' WHERE ID = 1; -- Deadlock risk
-- COMMIT;
GO


/*
  Lock Escalation Demonstration
*/
-- Drop and recreate test table for escalation
DROP TABLE IF EXISTS dbo.LockEscalationTest;
GO

CREATE TABLE dbo.LockEscalationTest (
    ID INT PRIMARY KEY,
    Data CHAR(100)
);

-- Insert many rows to force lock escalation
INSERT INTO dbo.LockEscalationTest
SELECT TOP 10000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'Sample'
FROM sys.all_objects a, sys.all_objects b;
GO

-- Start transaction and update many rows
BEGIN TRAN;

UPDATE dbo.LockEscalationTest
SET Data = 'Updated'
WHERE ID <= 10000;

-- Check current locks (look for escalation to table-level)
SELECT resource_type, request_mode, request_status
FROM sys.dm_tran_locks
WHERE request_session_id = @@SPID;

-- COMMIT after verification
-- COMMIT;
GO


--   Transaction Isolation Levels in SQL Server

-- Transaction 1 – Writer (Session 1) 
-- This transaction makes modifications to test isolation effects

BEGIN TRAN;

-- Modify existing record (creates lock)
UPDATE [dbo].[tblEmployee]
SET EmployeeNumber = 122
WHERE EmployeeNumber = 123;

-- Commit the change
COMMIT;

-- Make another change after first transaction
UPDATE [dbo].[tblEmployee]
SET EmployeeNumber = 123
WHERE EmployeeNumber = 122;

-- Insert new row
INSERT INTO [dbo].[tblEmployee] (
    [EmployeeNumber],
    [EmployeeFirstName],
    [EmployeeMiddleName],
    [EmployeeLastName],
    [EmployeeGovernmentID],
    [DateOfBirth],
    [Department]
)
VALUES (122, 'H', 'I', 'T', 'H', '2010-01-01', 'H');

-- Delete that row
DELETE FROM [dbo].[tblEmployee]
WHERE EmployeeNumber = 122;

-- Transaction 2 – Reader (Session 2)
-- This simulates reading during concurrent writes

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;  -- Default level

BEGIN TRAN;

-- Read before writer changes
SELECT * FROM [dbo].[tblEmployee];

-- Pause to allow Session 1 to make changes
WAITFOR DELAY '00:00:20';

-- Read again to observe if changes are visible now
SELECT * FROM [dbo].[tblEmployee];

COMMIT;

--Under READ COMMITTED, Session 2 will block if Session 1 is modifying rows and hasn’t committed yet.
--If Session 1 commits between the two reads in Session 2, the second SELECT will reflect new data.
--This avoids dirty reads but still allows non-repeatable reads (values might change between reads in the same transaction).

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;   -- Allows dirty reads
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;    -- Prevents non-repeatable reads
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;       -- Full locking; prevents phantom reads
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;           -- Uses row versioning; avoids locks

-- HEAP
CREATE TABLE tblDemoHeap 
(field1 int ,
field2 int )

-- 48 Clustered Index
--Create a Table with Clustered Index

-- Create [tblEmployeeCluster] primary table
CREATE TABLE [dbo].[tblEmployeeCluster] (
    EmployeeNumber INT,
    EmployeeFirstName VARCHAR(50),
    EmployeeMiddleName VARCHAR(50),
    EmployeeLastName VARCHAR(50),
    EmployeeGovernmentID VARCHAR(50),
    DateOfBirth DATE,
    Department VARCHAR(50)
);

-- Create a clustered index (can only be one per table)
CREATE CLUSTERED INDEX idx_tblEmployee 
ON [dbo].[tblEmployee]([EmployeeNumber]);

-- if i drop the index the search back to scann
DROP INDEX  idx_tblEmployee ON  [dbo].[tblEmployee]

SELECT * FROM [dbo].[tblEmployee] --INDEX SCAN
SELECT * FROM [dbo].[tblEmployee] WHERE [EmployeeNumber] =127  --INDEX SEEK

ALTER TABLE [dbo].[tblEmployee] ADD CONSTRAINT pk_tblEmployee PRIMARY KEY([EmployeeNumber])
--The CREATE UNIQUE INDEX statement terminated because a duplicate key was found for the object name 'dbo.tblEmployee'
-- and the index name 'pk_tblEmployee'. The duplicate key value is (131).

-- Drop the clustered index
DROP INDEX idx_tblEmployee ON [dbo].[tblEmployee];

-- Copy Data to Another Table
-- Copy selected data into a new table
SELECT *
INTO [dbo].[tblEmployee2]
FROM [dbo].[tblEmployee]
WHERE EmployeeNumber <> 131;

-- Add a primary key constraint, which creates a clustered index by default
ALTER TABLE [dbo].[tblEmployee2]
ADD CONSTRAINT pk_tblEmployee2 PRIMARY KEY (EmployeeNumber);

--Query the Table Using Index (Seek vs. Scan)
-- Seek: optimized access if index exists
SELECT * 
FROM [dbo].[tblEmployee2]
WHERE [EmployeeNumber] = 127;

-- Scan: entire table read if WHERE clause doesn't match indexed column
SELECT * 
FROM [dbo].[tblEmployee2];

-- Create another simple table with a primary key
CREATE TABLE myTable (
    Field1 INT PRIMARY KEY  -- Creates clustered index by default
);

--QUIZ 62 CLUSTERED INDEX

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,   -- Creates a clustered index by default
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50)
);

--The script will fail at the ALTER TABLE step because you are trying 
--to add a PRIMARY KEY constraint on Field1, but the table contains duplicate values 
--(3 appears twice). A primary key requires all values to be unique and not null.
CREATE TABLE #tblDemo
(Field1 INT NOT NULL);
INSERT INTO #tblDemo(Field1)
VALUES (1), (2), (3), (3), (4);  -- Duplicate value: 3

ALTER TABLE #tblDemo
ADD CONSTRAINT pk_tblDemo PRIMARY KEY(Field1);  --  Error: Duplicate values
--to fix it
DELETE FROM #tblDemo
WHERE Field1 NOT IN (
    SELECT MIN(Field1)
    FROM #tblDemo
    GROUP BY Field1
);

ALTER TABLE #tblDemo
ADD CONSTRAINT pk_tblDemo PRIMARY KEY(Field1);


-- Will this create a valid CLUSTERED INDEX?
    CREATE TABLE #tblDemo (Field1 INT NOT NULL)
    INSERT INTO #tblDemo(Field1)
    VALUES (1), (2), (3), (3), (4)
    CREATE CLUSTERED INDEX idx_tblDemo on #tblDemo(Field1)
-- Answer Yes, this will create a valid CLUSTERED INDEX , even with duplicate values 
-- because a CLUSTERED INDEX does not require uniqueness by default.

-- Unlike a PRIMARY KEY, which must be unique,
-- a CLUSTERED INDEX can contain duplicates unless specified as UNIQUE.

SELECT * FROM #tblDemo ORDER BY Field1;
CREATE UNIQUE CLUSTERED INDEX idx_tblDemo ON #tblDemo(Field1); -- Will fail due to duplicates

-- CLUSTERED INDEX does not require uniqueness among the indexed values. This means that you can create a CLUSTERED INDEX 
-- on a column with duplicate entries, allowing for efficient data organization and retrieval.

CREATE CLUSTERED INDEX idx_Field1 ON MyTable(Field1); -- duplicates allowed
CREATE UNIQUE CLUSTERED INDEX idx_Field1 ON MyTable(Field1); -- duplicates NOT allowed

--Question 4:
--As clustered indexes physically re-order the data, can you have more than one clustered index in a single table?
--Answer No,it cannot have more than one clustered index on a single table in SQL Server.
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,  -- This creates a clustered index by default
    LastName NVARCHAR(100),
    DepartmentID INT
);
-- Try to create another clustered index
CREATE CLUSTERED INDEX idx_Dept ON Employees(DepartmentID);--This will fail: Cannot create more than one clustered index on table 'Employees'.
--> instead we should use 
--Use multiple non-clustered indexes:
CREATE NONCLUSTERED INDEX idx_Dept ON Employees(DepartmentID);

--Use included columns or composite indexes:
CREATE NONCLUSTERED INDEX idx_DeptInc ON Employees(DepartmentID) INCLUDE (LastName);

-- Change the clustered index to a different column:
DROP INDEX PK_Employees ON Employees;
CREATE CLUSTERED INDEX idx_Dept ON Employees(DepartmentID);


--PRIMARY KEY  vs CLUSTERED INDEX Attempt
CREATE TABLE #tblDemo(Field1 INT NOT NULL);
INSERT INTO #tblDemo(Field1)
VALUES (1), (2), (3), (3), (4);
ALTER TABLE #tblDemo
ADD CONSTRAINT pk_tblDemo PRIMARY KEY(Field1); --This will fail becaise , A PRIMARY KEY constraint must enforce uniqueness.

--CLUSTERED INDEX Attempt
CREATE TABLE #tblDemo(Field1 INT NOT NULL);
INSERT INTO #tblDemo(Field1)
VALUES (1), (2), (3), (3), (4);
CREATE CLUSTERED INDEX idx_tblDemo on #tblDemo(Field1);--This will succeed: A clustered index does not require uniqueness by default.


--Non-clustered Index
-- Create Non-Clustered Indexes :
-- Single-column non-clustered index
CREATE NONCLUSTERED INDEX idx_tblEmployee_DateOfBirth 
ON [dbo].[tblEmployee] ([DateOfBirth]);

-- Multi-column non-clustered index (composite index)
CREATE NONCLUSTERED INDEX idx_tblEmployee_DateOfBirth_Department 
ON [dbo].[tblEmployee] ([DateOfBirth], Department);

-- Drop an Index :
DROP INDEX idx_tblEmployee_DateOfBirth ON [dbo].[tblEmployee];


-- Selects for Seek vs Scan Demonstration :
-- Likely uses index seek (if indexed on EmployeeNumber)
SELECT * 
FROM [dbo].[tblEmployee2] 
WHERE [EmployeeNumber] = 127;

-- No filter = full scan
SELECT * 
FROM [dbo].[tblEmployee2];

-- Likely uses index seek on DateOfBirth or composite index
SELECT DateOfBirth, Department
FROM [dbo].[tblEmployee]
WHERE DateOfBirth >= '1992-01-01' AND DateOfBirth < '1993-01-01';

-- Add Unique Constraint :
-- Ensures Department names are unique
ALTER TABLE [dbo].[tblDepartment]
ADD CONSTRAINT unq_tblDepartment UNIQUE(Department);

| Concept               | Explanation                                                                  |
| --------------------- | ---------------------------------------------------------------------------- |
|  Non-Clustered Index  | Logical structure for faster lookups, doesn t affect physical row order.     |
|  Index Seek           | Efficient; used when index helps directly locate rows.                       |
|  Index Scan           | Reads the entire index; happens when filter is broad or index is not useful. |
|  UNIQUE constraint    | Enforces uniqueness and creates a unique non-clustered index.                |


--49 Non-clustered Index :
-- Drop existing index if it exists
DROP INDEX IF EXISTS idx_tblEmployee_DateOfBirth ON [dbo].[tblEmployee];
DROP INDEX IF EXISTS idx_tblEmployee_DateOfBirth_Department ON [dbo].[tblEmployee];

-- Create Non-Clustered Index on a single column
CREATE NONCLUSTERED INDEX idx_tblEmployee_DateOfBirth 
ON [dbo].[tblEmployee]([DateOfBirth]);

-- Create Composite Non-Clustered Index
CREATE NONCLUSTERED INDEX idx_tblEmployee_DateOfBirth_Department 
ON [dbo].[tblEmployee]([DateOfBirth], Department);

-- Attempting to drop an index (example — only drop if it exists)
DROP INDEX IF EXISTS idx_tblEmployee ON [dbo].[tblEmployee];

-- Query optimized by index (Seek vs. Scan demo)
-- Seek: uses index if WHERE condition is selective and matches index key
SELECT * 
FROM [dbo].[tblEmployee2] 
WHERE [EmployeeNumber] = 127;

-- Scan: will occur when WHERE clause is absent or not selective
SELECT * 
FROM [dbo].[tblEmployee2];

-- Filtered search using index
SELECT DateOfBirth, Department
FROM [dbo].[tblEmployee]
WHERE DateOfBirth >= '1992-01-01' 
  AND DateOfBirth < '1993-01-01';

-- Seek = Efficient access using index (narrow range or equality)
-- Scan = Full table/index traversal (less efficient)

-- Add a UNIQUE constraint to enforce unique values in Department column
ALTER TABLE [dbo].[tblDepartment]
ADD CONSTRAINT unq_tblDepartment UNIQUE(Department);

-- Create a Filtered Index (non-clustered) — excellent for selective queries
CREATE NONCLUSTERED INDEX idx_tblEmployee_Employee
ON dbo.tblEmployee(EmployeeNumber)
WHERE EmployeeNumber < 139;

| Concept                 | Explanation                                                                                            |
| ----------------------- | ------------------------------------------------------------------------------------------------------ |
| Non-Clustered Index     | A separate structure from the table that speeds up queries by maintaining pointers to the actual rows. |
| Composite Index         | An index on multiple columns. Useful when queries filter/sort by both columns.                         |
| Filtered Index          | Indexes only a subset of data based on a  WHERE  clause. Great for large tables with sparse queries.   |
| Seek                    | Uses index efficiently to find rows.                                                                   |
| Scan                    | Walks the entire index/table when no good index is available.                                          |
| Unique Constraint       | Prevents duplicate values in a column, automatically backed by a unique index.                         |

-- INCLUDE :
-- Create Non-Clustered Index with INCLUDE ( index on EmployeeNumber)
/*The INCLUDE clause allows additional columns to be added to the leaf level of the index 
they are not part of the key but can be returned by the index, avoiding expensive lookups to the base table*/

-- Create a non-clustered index on EmployeeNumber
-- Include EmployeeFirstName in the index leaf level to avoid key lookups
CREATE NONCLUSTERED INDEX idx_tblEmployee_Employee
ON dbo.tblEmployee(EmployeeNumber)
INCLUDE (EmployeeFirstName);

-- Drop the index when it's no longer needed
DROP INDEX idx_tblEmployee_Employee ON dbo.tblEmployee;

| Clause                      | Purpose                                                                   |
| --------------------------- | ------------------------------------------------------------------------- |
| CREATE NONCLUSTERED INDEX   | Creates a secondary index that does not affect the physical row order.    |
| INCLUDE                     | Adds non-key columns to the index leaf for performance (reduces lookups). |
| DROP INDEX                  | Removes the specified index from the table.                               |


-- Include Client Statistics

SELECT * 
FROM [dbo].[tblEmployee];

-- Create a non-clustered index if it doesn't already exist
CREATE NONCLUSTERED INDEX idx_tblEmployee_EmployeeNumber
ON dbo.tblEmployee(EmployeeNumber);

--Rerun Your Query with SET STATISTICS IO and TIME
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

SELECT *
FROM [dbo].[tblEmployee] AS E
WHERE E.EmployeeNumber = 134;

-- Make sure the table has many rows for meaningful testing
SELECT COUNT(*) FROM dbo.tblEmployee;

-- Then run:
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT EmployeeFirstName, EmployeeLastName
FROM dbo.tblEmployee
WHERE EmployeeNumber = 134;








































































