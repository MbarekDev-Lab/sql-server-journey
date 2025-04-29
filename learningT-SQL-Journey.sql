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

FROM 
  tblEmployee AS E 
JOIN 
  tblAttendance AS A 
ON 
  E.EmployeeNumber = A.EmployeeNumber

WHERE 
  A.AttendanceMonth < '2015-05-01'
