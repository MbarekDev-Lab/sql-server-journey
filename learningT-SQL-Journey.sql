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






