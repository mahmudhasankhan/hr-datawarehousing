SELECT et.DATE,
et.id,
et.employee_name,
CASE 
	WHEN et.id IN (581,956) THEN 'Credit Control'
	WHEN et.id IN (880,1428,1477,1596) THEN 'Customer Service'
	WHEN et.id IN (134,201,719,809,1104,1215,1250,1438) THEN 'Finance & Accounts'
	WHEN et.id IN (214,957,1413,513,545,1362,1368,1412,1565) THEN 'HR & Admin'
	WHEN et.id IN (47,50,206,568,638,668,1093,1200,1252,1290,1411,1426,1517,1535) THEN 'Information Technology'
	WHEN et.id IN (1359,1491,1496,1536) THEN 'Marketing'
	WHEN et.id IN (1382,1554) THEN 'MIS & Operations'
	WHEN et.id IN (320,1353,1572) THEN 'Operations & Fulfillment'
	WHEN et.id IN (180,315,319,341,493,497,1178,1425) THEN 'Sales & Business Development'
	WHEN et.id IN (771,1140,1202,1332,1336,1403,1490,1506,1519,1561,1568,1573,1593,1594) THEN 'HORECA Sales'
	WHEN et.id IN (588,688,784,785,884,914,1004,1123,1186,1393,1427,1444,917,1521,1526,1574) THEN 'INDUSTRIAL Sales'
	WHEN et.id IN (113,172,178,452,564,669,703,843,1023,1056,1223,1244,1291,1440,1473,1485,1501,1555) THEN 'Supply Chain'
	WHEN et.id IN (747,773,955,1212,1365,1397,1404,1436,1441,1474,1475,1484,1518,1527,1571,1595) THEN 'SME Sales'
	WHEN et.id IN (1405,1549,1556,1570,1590,1592) THEN 'Banani Hub'
	WHEN et.id IN (1431,1443,1537,1583) THEN 'Banashree Hub'
	WHEN et.id IN (1586,1587,1589) THEN 'Dhakkhin Khan Hub'
	WHEN et.id IN (1432,1541,1575,1585) THEN 'Mirpur Hub'
	WHEN et.id IN (1407,1471,1495,1544,1582) THEN 'Mohammadpur Hub'
	WHEN et.id IN (29,90,95,250,412,796,804,812,994,1166,1192,1237,1272,1281,1282,1298,1363,1408,1409,1422,1424,1446,1483,1492,1507,1529,1530,1531,1542,1547,1548,1558,1559,1564,1566,1567,1569,1576,1584,1588,1598,1599,1600,1064) THEN 'Ops & F (CWH)'
	ELSE NULL
END AS Department,
et.entry_time,
ot.out_time, 
CASE 
	WHEN Entry_Time > '09:15:00' AND et.id NOT IN (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206) THEN 'LATE'
	WHEN Entry_Time > '10:15:00' AND et.id IN (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206) THEN 'LATE'
	WHEN Entry_Time < '10:15:00' AND et.id IN (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206) THEN 'ON TIME'
	WHEN Entry_Time < '09:15:00' AND et.id NOT IN (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206) THEN 'ON TIME'
END AS Attendance,
{{ elapsed_time('et.Entry_Time', 'ot.Out_Time') }} AS Elapsed_Time
FROM {{ ref('int_employees_aggregated_to_entry_time') }} AS et
JOIN {{ ref('int_employees_aggregated_to_out_time') }} AS ot
ON et.DATE = ot.DATE
AND et.id = ot.id
AND et.employee_name = ot.employee_name
ORDER BY DATE, Department, Entry_Time



