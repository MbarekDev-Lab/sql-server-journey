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



