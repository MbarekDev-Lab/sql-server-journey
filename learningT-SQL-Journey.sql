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









