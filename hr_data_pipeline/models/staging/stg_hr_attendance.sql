SELECT 
	serialno as seraialnum,
	employeeid as id,
	name as employee_name
	authdatetime as date_time,
	authdate as date,
	authtime as time,
	devicename as location,
FROM {{ source('hr_source', 'attendance') }}
