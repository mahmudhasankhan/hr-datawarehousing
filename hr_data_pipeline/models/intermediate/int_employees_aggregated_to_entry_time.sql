WITH aggregate_employee_attendance_to_min_time as (
	SELECT 
	authDate as DATE, employeeid as id,
	name,
	cast( min(authTime) as time) as entry_time 
FROM {{ source('hr_source', 'attendance') }}
GROUP BY authDate, employeeID, name
)

SELECT * FROM aggregate_employee_attendance_to_min_time
