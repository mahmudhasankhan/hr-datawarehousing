WITH aggregate_employee_attendance_to_max_time as (
	SELECT 
	authDate as DATE, employeeid as id,
	name,
	cast(max(authTime) as time) as out_time 
FROM {{ source('hr_source', 'attendance') }}
GROUP BY authDate, employeeID, name
)

SELECT * FROM aggregate_employee_attendance_to_max_time
