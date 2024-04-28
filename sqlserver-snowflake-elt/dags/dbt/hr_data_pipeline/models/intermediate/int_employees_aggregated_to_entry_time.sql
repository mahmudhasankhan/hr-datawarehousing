WITH aggregate_employee_attendance_to_min_time as (
	SELECT DATE,
	id,
	employee_name,
	cast( min(time) as time) as entry_time 
FROM {{ ref('stg_hr_attendance') }}
GROUP BY date, id, employee_name
)

SELECT * FROM aggregate_employee_attendance_to_min_time
