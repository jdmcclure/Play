import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
import os
import config

#Set database connection details
server = '192.168.213.19'
database = 'reporting_system'
username =  os.environ.get('DB_USERNAME')
password =  os.environ.get('DB_PASSWORD')

sql_query = """
WITH incidents AS
(SELECT
	Master_Incident_Number AS 'incident'
	,CASE
		WHEN RVeh.Radio_Name LIKE '5%' THEN 'CSUPD'
		WHEN RVeh.Radio_Name LIKE '9%' THEN 'LCSO'
		WHEN RVeh.Radio_Name LIKE '1%' THEN 'FCPS'
	END AS 'agency'
	,CASE
		WHEN RMI.problem LIKE '%EXTRA PATROL%' THEN 'EXTRA PATROL'
		WHEN RMI.problem LIKE '%DIRECTED PATROL' THEN 'DIRECTED PATROL'
		ELSE RMI.problem
	END AS 'problem'
	,CASE
		WHEN RVeh.Radio_Name LIKE '5%' AND (Call_Back_Phone IS NULL OR Call_Back_Phone = '') THEN 'Department-Initiated'
		WHEN RVeh.Radio_Name LIKE '5%' AND NOT(Call_Back_Phone IS NULL) THEN 'Call-for-Service'
		ELSE NULL
	END AS 'origin'
	,Response_Date AS 'date'
	,RMI.Address AS 'call_location'
	,VL.SecondaryLocationAddress AS 'unit_location'
	,RVeh.Radio_Name AS 'officer'
	,Time_CallEnteredQueue AS 'time_call_entered_queue'
	,Time_Assigned AS 'time_unit_assigned'
	,RVeh.Time_ArrivedAtScene AS 'time_unit_arrived'
	,DATEDIFF(s, RVeh.Time_Assigned, RVeh.Time_ArrivedAtScene)/60.0 AS 'response_time_mins'  -- Calculates the minutes between the time of call entry and the time first unit arrived on-scene
	,DATEDIFF(s, RVeh.Time_Assigned, GETDATE())/60.0 AS 'mins_on_call'
	,DATEDIFF(s, Time_CallEnteredQueue, GETDATE())/60.0 AS 'mins_call_open'  -- Calculates the minutes between the time the call was entered and the call was completed
	,ROW_NUMBER() OVER (partition BY Master_Incident_Number ORDER BY RVeh.Radio_Name DESC) AS 'incident_count'
	,ROW_NUMBER() OVER (partition BY RVeh.Radio_Name ORDER BY Master_Incident_Number) AS 'test_count'

FROM  
	Reporting_System.dbo.Response_Master_Incident RMI 
		INNER JOIN Reporting_System.dbo.Response_Vehicles_Assigned RVeh	
			ON	RMI.ID = RVeh.Master_Incident_ID
		LEFT JOIN Reporting_System.dbo.VehicleLocations VL
			ON RMI.Id = VL.MasterIncidentID

WHERE
	(RVeh.Radio_Name LIKE '5%'
	OR
	RVeh.Radio_Name LIKE '9%'
	OR
	RVeh.Radio_Name LIKE '1%'
	OR
	RVeh.Radio_Name IS NULL) --Pulls CSUPD, FCPS, LCSO, and unassigned calls for service
	AND
	(Time_CallEnteredQueue IS NOT NULL AND Time_CallClosed IS NULL) --There is a timestamp for call entry, but not for call closed
	AND
	Response_Date >= DATEADD(day, -5 + DATEDIFF(day, '19000101', GETDATE()), '19000101') -- Pull only incidents within the last 5 days
)
SELECT
	incident
	,agency
	,problem
	,CASE
		WHEN officer LIKE '_E%' THEN LEFT(officer, 1) + 'E.' + RIGHT(officer, CHARINDEX('E', officer))
		ELSE officer
	END AS 'officer'
	,FORMAT(date, 'MM/dd/yyyy HH:mm') AS 'date'
	,call_location
	,CASE
		WHEN unit_location IS NULL THEN call_location
		ELSE unit_location
	END AS 'unit_location'
	,CASE
		WHEN time_unit_arrived IS NULL THEN 'ENROUTE'
		ELSE 'ON-SCENE'
	END AS 'status'
	,FORMAT(time_call_entered_queue, 'MM/dd/yyyy HH:mm') AS 'time_call_entered_queue'
	,FORMAT(time_unit_arrived, 'MM/dd/yyyy HH:mm') AS 'time_unit_arrived'
	,CASE
		WHEN response_time_mins < 0 THEN 0 --Reopened calls show negative response times
		ELSE CAST(ROUND(response_time_mins, 2) AS numeric(36,2))
	END AS 'response_time_mins'
	,CAST(ROUND(mins_on_call, 2) AS numeric(36,2)) AS 'mins_on_call'
	,FORMAT(GETDATE(), 'MM/dd/yyyy HH:mm:ss') AS 'last_refresh'

FROM
	incidents

WHERE
	((agency in ('LCSO', 'FCPS') AND incident_count = 1)
	OR
	(test_count = 1	AND	agency = 'CSUPD'))
	AND
	NOT(problem = '_BLANK'
		OR
		problem = 'TEST CALL'
		OR
		incident = ''
		OR
		problem = 'INFORMATION ONLY'
		OR
		problem = 'ON FOOT'
		OR
		problem LIKE 'OUT%'
		)

ORDER BY
	agency,incident, date DESC
"""

#Set output file location
output_file = 'C:\Clery\CAD\Live_Calls\cad_incidents.csv'

#Create the database connection string
db_connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(db_connection_string)

#Execute query and retrieve results
df = pd.read_sql_query(sql=text(sql_query), con=engine.connect())

#Export to CSV
df.to_csv(output_file, index=False, float_format='{:.2f}'.format, encoding='utf-8')
