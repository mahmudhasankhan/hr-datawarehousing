{% macro elapsed_time(entry_time, out_time) %}
	CAST(CAST(datediff(minute, {{ entry_time }}, {{ out_time }}) AS DECIMAL(10,2)) / 60 AS DECIMAL (10,2))
{% endmacro %}
