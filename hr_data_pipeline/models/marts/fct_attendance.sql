create temporary table attendance as (
select et.DATE,
et.id,
et.name,
CAST(NULL as varchar(512)) AS Department,
et.entry_time,
ot.out_time, 
Cast(NULL as varchar(512)) as Attendance,
DATEDIFF(hour,et.Entry_Time,ot.Out_Time) as Hours
from {{ ref('int_employees_aggregated_to_entry_time') }} as et
join {{ ref('int_employees_aggregated_to_out_time')}} as ot
on et.DATE = ot.DATE
and et.id = ot.id
and et.Name = ot.Name
)



UPDATE attendance 
SET Attendance = 'LATE'
WHERE Entry_Time > '09:15:00'
and ID not in (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206)

UPDATE attendance 
SET Attendance = 'LATE'
WHERE Entry_Time > '10:15:00'
and ID in (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206)

UPDATE attendance
SET Attendance = 'PRESENT'
WHERE Entry_Time < '10:15:00'
and ID in (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206)

UPDATE attendance 
SET Attendance = 'PRESENT'
WHERE Entry_Time < '09:15:00'
and ID not in (568, 638, 668, 1093, 1200, 1252, 1290, 1411, 1426, 1517, 1535, 47, 50, 206)

UPDATE attendance
SET Department = 'Credit Control'
WHERE ID IN (581,956)

UPDATE attendance 
SET Department = 'Customer Service'
WHERE ID IN (880,1428,1477,1596)

UPDATE attendance 
SET Department = 'Finance & Accounts'
WHERE ID IN (134,201,719,809,1104,1215,1250,1438)

UPDATE attendance
SET Department = 'HR & Admin'
WHERE ID IN (214,957,1413,513,545,1362,1368,1412,1565)

UPDATE attendance
SET Department = 'Information Technology'
WHERE ID IN (47,50,206,568,638,668,1093,1200,1252,1290,1411,1426,1517,1535)


UPDATE attendance
SET Department = 'Marketing'
WHERE ID IN (1359,1491,1496,1536)


UPDATE attendance
SET Department = 'MIS & Operations'
WHERE ID IN (1382,1554)


UPDATE attendance
SET Department = 'Operations & Fulfillment'
WHERE ID IN (320,1353,1572)


UPDATE attendance
SET Department = 'Sales & Business Development'
WHERE ID IN (180,315,319,341,493,497,1178,1425)


UPDATE attendance
SET Department = 'Sales & Business Development (Horeca)'
WHERE ID IN (771,1140,1202,1332,1336,1403,1490,1506,1519,1561,1568,1573,1593,1594)



UPDATE attendance
SET Department = 'Sales & Business Development (Industrial)'
WHERE ID IN (588,688,784,785,884,914,1004,1123,1186,1393,1427,1444,917,1521,1526,1574)



UPDATE attendance
SET Department = 'Supply Chain'
WHERE ID IN (113,172,178,452,564,669,703,843,1023,1056,1223,1244,1291,1440,1473,1485,1501,1555)


UPDATE attendance
SET Department = 'Sales & Business Development (FTS)'
WHERE ID IN (1360,1448,1449,1581,1597)


UPDATE attendance
SET Department = 'Sales & Business Development (SME & SALES)'
WHERE ID IN (747,773,955,1212,1365,1397,1404,1436,1441,1474,1475,1484,1518,1527,1571,1595)


UPDATE attendance
SET Department = 'Banani HUB'
WHERE ID IN (1405,1549,1556,1570,1590,1592)


UPDATE attendance
SET Department = 'Banashree Hub'
WHERE ID IN (1431,1443,1537,1583)



UPDATE attendance 
SET Department = 'Dhakkhin Khan HUB'
WHERE ID IN (1586,1587,1589)



UPDATE attendance 
SET Department = 'Mirpur Hub'
WHERE ID IN (1432,1541,1575,1585)


UPDATE attendance 
SET Department = 'Mohammadpur Hub'
WHERE ID IN (1407,1471,1495,1544,1582)

UPDATE att 
SET Department = 'Ops & F (CWH)'
WHERE ID IN (29,90,95,250,412,796,804,812,994,1166,1192,1237,1272,1281,1282,1298,1363,1408,1409,1422,1424
,1446,1483,1492,1507,1529,1530,1531,1542,1547,1548,1558,1559,1564,1566,1567,1569,1576,1584,1588,1598,1599,1600,1064
)


select * from attendance
order by DATE, Department, Entry_Time




