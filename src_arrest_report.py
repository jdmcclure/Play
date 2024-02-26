import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from openpyxl import Workbook
import time
import os

server = os.environ.get('RMSServer')
database = os.environ.get('RMSDatabase')
username = os.environ.get('RMS_User')
password = os.environ.get('RMS_PW')

query = """
SELECT
	CASE
		WHEN Arrest.AgencyName LIKE 'CSU%'
			THEN 'CSUPD'
		WHEN Arrest.AgencyName LIKE 'ESTES%'
			THEN 'EPD'
		WHEN Arrest.AgencyName LIKE 'Drug%'
			THEN 'DTF'
		WHEN Arrest.AgencyName LIKE 'FORT%'
			THEN 'FCPS'
		WHEN Arrest.AgencyName LIKE 'LOVELAND%'
			THEN 'LPD'
		WHEN Arrest.AgencyName LIKE 'Larimer%'
			THEN 'LCSO'
		ELSE Arrest.AgencyName
	END AS 'Agency'
	,Arrest.CaseNumber AS 'Case Number'
	,Arrest.SequenceNotation AS 'Supp #'
	,FORMAT(ArrestEvent.startDate, 'MM/dd/yyyy') AS 'Date'
	,UPPER(ArrestPerson.lastName + ',' + ArrestPerson.firstName + ' ' + CASE WHEN ArrestPerson.middleName IS NULL THEN '' 
			ELSE ArrestPerson.middleName 
		END) AS 'Name'
	,FORMAT(ArrestPerson.dateOfBirth, 'MM/dd/yyyy') AS 'DOB'
	,ArrestCharge.ViolationCodeReference_Code AS 'Charge'
	,ArrestCharge.ViolationCodeReference_Description AS 'Charge_Literal'
	,ArrestCharge.FelonyMisdemeanor_Description AS 'Charge Level'
	,ArrestEvent.address_streetAddress AS 'Arrest_Location'
	,ArrestOfficer.officerName_Code AS 'Officer'

FROM
	InformRMSReports.Reporting.Arrest Arrest
	INNER JOIN InformRMSReports.Reporting.ArrestEvent ArrestEvent
		ON Arrest.Id = ArrestEvent.Arrest_Id
	INNER JOIN InformRMSReports.Reporting.ArrestPerson ArrestPerson
		ON Arrest.Id = ArrestPerson.Arrest_Id
	INNER JOIN InformRMSReports.Reporting.ArrestCharge ArrestCharge
		ON Arrest.Id = ArrestCharge.Arrest_Id
	INNER JOIN InformRMSReports.Reporting.ArrestOfficer ArrestOfficer
		ON Arrest.Id = ArrestOfficer.Arrest_Id

WHERE
	(Arrest.AgencyName LIKE 'CSU%'
	OR
	Arrest.AgencyName LIKE 'FORT%'
	OR
	Arrest.AgencyName LIKE 'Larimer%')
	AND
	ArrestOfficer.involvementDate BETWEEN DATEADD(DAY, DATEDIFF(day, 1, getdate()), '07:00:00')
	AND DATEADD(DAY, DATEDIFF(day, 0, getdate()), '07:00:00')
	--AND NOT(ArrestEvent.startDate IS NULL) 
	AND
	ArrestPerson.InvolvementType_Code LIKE 'Arrestee'
	AND
	ArrestOfficer.involvementType_Code LIKE 'Report'
	AND
	NOT(ArrestCharge.ViolationCodeReference_Code LIKE '16-3-102%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE 'Mittar%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-02%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-03%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-04%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-05%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-06%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-07%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-08%'
		OR 
		ArrestCharge.ViolationCodeReference_Code LIKE '42-3%'
		OR 
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-10%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-11%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-12%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-1%'
		OR 
		ArrestCharge.ViolationCodeReference_Code LIKE '42-20%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-10%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-11%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-12%'
		OR 
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-130%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-131%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-132%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-134%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-137%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-138%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-139%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-14%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-2%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-3%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-2-4%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-1302%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-1305%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-138%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-140%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-1410%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-1411%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-1412%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-15%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-17%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-19%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-2%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-3%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-4%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-5%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-6%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-7%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-8%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-4-9%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-5%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-6%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-7%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-8%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '42-9%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '43-3%'
		OR
		ArrestCharge.ViolationCodeReference_Code LIKE '43-5%'
		)
ORDER BY
	lastName;
"""
#Create connection string and engine
db_connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(db_connection_string)

df = pd.read_sql_query(sql=text(query), con=engine.connect())

todays_date = time.strftime("%Y-%m-%d")
file_format = time.strftime("%Y_%m_%d")
current_year = time.strftime("%Y")

file_name = "SRC Arrest Report - " + todays_date
excel_file = f'C:\\Users\\jmcclure\\OneDrive - Colostate\\Reporting\\SRC\\{file_name}.xlsx'
src_path = f'P:\{file_format}.xlsx'
df.to_excel(excel_file, index=False)
df.to_excel(src_path, index=False)

