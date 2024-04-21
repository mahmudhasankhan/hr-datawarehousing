SELECT 
	serialno as seraialnum,
	employeeid as id,
	authdatetime as date_time,
	authdate as date,
	authtime as time,
	devicename as location,
	name as employee_name
FROM {{ source('hr_source', 'attendance') }}
