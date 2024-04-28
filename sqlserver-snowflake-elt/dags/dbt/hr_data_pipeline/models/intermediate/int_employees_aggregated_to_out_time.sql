WITH aggregate_employee_attendance_to_max_time as (
	SELECT 
	DATE,
	id,
	employee_name,
	cast(max(time) as time) as out_time 
FROM {{ ref('stg_hr_attendance') }}
GROUP BY Date, id, employee_name
)

SELECT * FROM aggregate_employee_attendance_to_max_time
