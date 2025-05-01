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





