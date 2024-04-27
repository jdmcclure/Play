import traceback
import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
import openpyxl
from datetime import datetime, timedelta
import sys, os
import time

try:
	server = os.environ.get('RMSServer')
	database = os.environ.get('RMSDatabase')
	username = os.environ.get('RMS_User')
	password = os.environ.get('RMS_PW')


	sql_query = """
		WITH incidents AS(
		SELECT 
			--Transform charge to be more broad and easily understood
			CASE
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Warrant Arrest%'	THEN 'Warrant Arrest'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Theft%' THEN 'Theft'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Criminal Mischief%'THEN 'Criminal Mischief'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Harassment%' THEN 'Harassment'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Noise%' THEN 'Noise'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%42-4-1301%' THEN 'DUI/DWAI'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Alcohol%' THEN 'Liquor Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Liquor%' THEN 'Liquor Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Open Container%' THEN 'Liquor Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Trespass%' THEN 'Trespass'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Extortion%' THEN 'Extortion'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Burglary%' THEN 'Burglary'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Accident%' THEN 'Traffic Crash'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%IMPROPER/UNSAFE BACKING' THEN 'Traffic Crash'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%MVA%' THEN 'Traffic Crash'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Left Scene%' THEN 'Traffic Crash'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Driving%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%SAME LANE PROHIBIT%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%42-%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '603(1)%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Unregistered Vehicle%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%No Proof of Insurance%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Expired Plates%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Speeding%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Designated Lane%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Failed TO STOP AT RED LIGHT%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%FAILED TO SIGNAL FROM PARK%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '1409(2)%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%DROVE WRONG WAY%' THEN 'Traffic Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Possessed (Fictitiou%' THEN 'False ID'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%FC-17-70%' THEN 'False ID'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Bodily Waste%' THEN 'Depositing Bodily Waste'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Fire Agency%' THEN 'Assist to Fire Dept'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Fire Authority%' THEN 'Assist to Fire Dept'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%AOA Other Medical%' THEN 'Assist to Medical'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Ambulance%' THEN 'Assist to Medical'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%AOA Larimer County%' THEN 'Assist to LCSO'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%AOA Fort Collins%' THEN 'Assist to Fort Collins Police'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%AOA Estes Park Poli%' THEN 'Assist to Estes Park Police'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%AOA LOVELAND POL%' THEN 'Assist to Loveland Police'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE 'CIRT%' THEN 'Assist to Other LE Agency'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%AOA Other Law Agency%' THEN 'Assist to Other LE Agency'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-18-405%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Drug%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Marijuana%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Found Contraband%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Schedule I or II%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Unlawful Possess Schedule%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Opium%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Controlled Substance%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Fentanyl%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE 'FC-17-191%' THEN 'Drug Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Protection Order%' THEN 'Protection Order Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Tampering%' THEN 'Criminal Tampering'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Arson%' THEN 'Arson'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Private Image%' THEN 'Posting a Private Image for Pecuniary Gain'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%False Reporting%' THEN 'False Reporting'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%False Crime Report%' THEN 'False Reporting'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Domestic Violence%' THEN 'Domestic Violence'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Defaced Firearm%' THEN 'Weapon Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Concealed Weapon%' THEN 'Weapon Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Possess Weapon%' THEN 'Weapon Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-12-106%' THEN 'Weapon Law Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Obstructing a Peace%' THEN 'Obstruction'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%FC-17-63%' THEN 'Obstruction'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Indecent Exposure%' THEN 'Indecent Exposure'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Public Indecency%' THEN 'Public Indecency'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Suicide Attempt%' THEN 'Mental Health'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Camping on Private%' THEN 'Camping on Private Property'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Camping on Public%' THEN 'Camping on Public Property'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Resisting Arrest%' THEN 'Resisting Arrest'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Littering%' THEN 'Littering'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Complicity%' THEN 'Complicity'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Juvenile%' THEN 'Juvenile Offense'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%ERROR%' THEN 'Error/Cancelled'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Fireworks%' THEN 'Fireworks Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Disorderly Conduct%' THEN 'Disorderly Conduct'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Degree Assault%' THEN 'Assault'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%FC-Assault' THEN 'Assault'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Detox Hold%' THEN 'Detox Hold'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%FC-Other Municipal%' THEN 'Other Municipal Offense'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Fraud by Check%' THEN 'Fraud'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Menacing%' THEN 'Menacing'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Vehicular Eluding%' THEN 'Vehicular Eluding'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Impersonation%' THEN 'Criminal Impersonation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Child Abuse%' THEN 'Child Abuse'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Sexual%' THEN 'Sex Offense'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Welfare Check%' THEN 'Welfare Check'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%False Imprisonment%' THEN 'False Imprisonment'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Window Peeping%' THEN 'Window Peeping'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Sex Offender Registration%' THEN 'Sex Offender Registration'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Stalking%' THEN 'Stalking'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Abandoned Vehicle%' THEN 'Abandoned Vehicle'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Graffiti%' THEN 'Graffiti'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Lost Property%' THEN 'Lost Property'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Suspicious%' THEN 'Suspicious'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Unattended Death%' THEN 'Unattended Death'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Cybercrime%' THEN 'Cybercrime'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Citizen Assist%' THEN 'Citizen Assist'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Robbery%' THEN 'Robbery'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Found Property%' THEN 'Found Property'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Disturbing the Peace%' THEN 'Disturbing the Peace'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Treatment of Animals%' THEN 'Improper Treatment of Animals'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Identification Documents%' THEN 'ID Document Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%GATHERING IDENTITY INFORMATION%' THEN 'Identity Theft'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Interfere WITH STAFF%' THEN 'Interference'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%ALSE ALARM RESIDENTIA%' THEN 'Burglary Alarm'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%DISTURBANCE/PROBLE%' THEN 'Other Disturbance'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%UICIDE THREAT%' THEN 'Suicide Threat'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%DISABILITIES DOG%' THEN 'Interference with Disabilities Dog'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%PERSONS WITH DISABILITIES%' THEN 'Interference with Persons with Disabilities'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%RECKLESS ENDANGERMENT%' THEN 'Reckless Endangerment'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Robbery%' THEN 'Robbery'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%BIAS MOTIVATED CRIMES%' THEN 'Bias Motivated Crime'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%ANIMAL AT LAR%' THEN 'Animal at Large'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%COMPUTER CRIME%' THEN 'Computer Crime'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%ABUSING TOXIC VAPORS%' THEN 'Abusing Toxic Vapors'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%EAVESDROPPING%' THEN 'Eavesdropping Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%OBSTRUCTION OF TELEPHONE%' THEN 'Obstruction of Telephone Service'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE 'FC-17-102%' THEN 'Throwing Missiles'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%CIVIL MATT%' THEN 'Civil Matter'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-8-212%' THEN 'Bond Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-8-704%' THEN 'Intimidating a Witness or Victim'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '1204(1f)%' THEN 'Parking Violation'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '%Prostitution%' THEN 'Prostitution'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-9-116%' THEN 'Throwing Missiles at Vehicles'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-3-302%' THEN 'Kidnapping'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE 'LC-98-02%' THEN 'Possession of Tobacco by Minors'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE '18-6-701%' THEN 'Contributing to the Delinquency of a Minor'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE 'FC-17-3%' THEN 'Failure to Appear'
				WHEN IncidentOffense.ViolationCodeReference_Description LIKE 'FC-17-128%' THEN 'Obstructing a Passageway'
				ELSE UPPER(LEFT(IncidentOffense.ViolationCodeReference_Description, 1)) + LOWER(RIGHT(IncidentOffense.ViolationCodeReference_Description, LEN(IncidentOffense.ViolationCodeReference_Description)-1))
			END AS 'Description'
			,Incident.CaseNumber AS 'Case_n'
			,FORMAT(IncidentEvent.dateReported, 'MM/dd/yyyy HH:mm') AS 'Reported'
			,CASE
				WHEN IncidentEvent.startDate IS NULL THEN FORMAT(IncidentEvent.dateReported, 'MM/dd/yy HH:mm')
				WHEN IncidentEvent.endDate IS NULL THEN FORMAT(IncidentEvent.startDate, 'MM/dd/yy HH:mm')
				ELSE FORMAT(IncidentEvent.startDate, 'M/dd/yyyy HH:mm') + ' to ' + FORMAT(IncidentEvent.endDate, 'M/dd/yyyy HH:mm')
			END AS 'Occurred'
			,CASE
				--Make location of active sex assault cases confidential
				WHEN (IncidentEvent.description_Description IN ('^POSXASLT^', 'SEXASSLTFR', 'SEXASSLTFF', 'SEX CRIME', 'Sex Crime')
					AND (IncidentEvent.status_Description IN ('Active', 'Pending') OR IncidentEvent.status_Description IS NULL))
					OR (IncidentOffense.ViolationCodeReference_Description LIKE '%Sexual%' 
					AND (IncidentEvent.status_Description IN ('Active', 'Pending') OR IncidentEvent.status_Description IS NULL)) 
					THEN 'Confidential'
				--Then provide common name for locations
					--Residence Halls and Apartments
				WHEN IncidentEvent.address_streetAddress LIKE '501 W PROSPECT%' THEN 'Aggie Village Family'
				WHEN IncidentEvent.address_streetAddress LIKE '551 W Laurel%' THEN 'Allison Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '711 Oval%' THEN 'Ammons Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '800 W Pitkin%' THEN 'AV - Aspen Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '820 W Pitkin%' THEN 'AV - Commons'
				WHEN IncidentEvent.address_streetAddress LIKE '816 W Pitkin%' THEN 'AV - Engineering'
				WHEN IncidentEvent.address_streetAddress LIKE '810 W Pitkin%' THEN 'AV - Honors'
				WHEN IncidentEvent.address_streetAddress LIKE '1101 Braiden%' THEN 'Braiden Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '500 W Pitkin%' THEN 'Braiden Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '801 W Laurel%' THEN 'Corbett Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '511 W Lake%' THEN 'Cottonwood Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '950 W Plum%' THEN 'Durrell Center'
				WHEN IncidentEvent.address_streetAddress LIKE '1001 W Laurel%' THEN 'Durward Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1231 Center%' THEN 'Eddy Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '900 W Pitkin%' THEN 'Edwards Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1000 W Pitkin%' THEN 'Ingersoll Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1400 W Elizabeth%' THEN 'International House'
				WHEN IncidentEvent.address_streetAddress LIKE '910 W Plum%' THEN 'Alpine Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '701 W Laurel%' THEN 'Parmelee Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '920 W Plum%' THEN 'The Pavilion'
				WHEN IncidentEvent.address_streetAddress LIKE '900 W Plum%' THEN 'Pinon Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '521 W Lake%' THEN 'Lodgepole'
				WHEN IncidentEvent.address_streetAddress LIKE '700 W Pitkin%' THEN 'Newsom Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '963 W Pitkin%' THEN 'Summit Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1500 W Plum%' THEN 'University Village - 1500'
				WHEN IncidentEvent.address_streetAddress LIKE '1600 W Plum%' THEN 'University Village - 1600'
				WHEN IncidentEvent.address_streetAddress LIKE '1700 W Plum%' THEN 'University Village - 1700'
				WHEN IncidentEvent.address_streetAddress LIKE '501 W Lake%' THEN 'Walnut'
				WHEN IncidentEvent.address_streetAddress LIKE '1009 W Laurel%' THEN 'Westfall Hall'
					--General Campus Buildings
				WHEN IncidentEvent.address_streetAddress LIKE '900 Oval%' THEN 'Admin Building'
				WHEN IncidentEvent.address_streetAddress LIKE '701 W Pitkin%' THEN 'Alumni Center'
				WHEN IncidentEvent.address_streetAddress LIKE '711 Oval%' Then 'Ammons Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1350 Center%' THEN 'Anatomy/Zoology'
				WHEN IncidentEvent.address_streetAddress LIKE '350 W Pitkin%' THEN 'Animal Sciences'
				WHEN IncidentEvent.address_streetAddress LIKE '1100 Meridian%' THEN 'Aylesworth Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '251 W Pitkin%' THEN 'Biology Building'
				WHEN IncidentEvent.address_streetAddress LIKE '410 W Pitkin%' THEN 'BSB'
				WHEN IncidentEvent.address_streetAddress LIKE '751 W PITKIN%' THEN 'Canvas Stadium'
				WHEN IncidentEvent.address_streetAddress LIKE '1301 Center%' THEN 'Chemistry Building'
				WHEN IncidentEvent.address_streetAddress LIKE '301 W Pitkin%' THEN 'Chemistry Research'
				WHEN IncidentEvent.address_streetAddress LIKE '1200 Center%' THEN 'Clark Building'
				WHEN IncidentEvent.address_streetAddress LIKE '151 W LAKE%' THEN 'CSU Medical Center'
				WHEN IncidentEvent.address_streetAddress LIKE '531 W Plum%' THEN 'CSU Transit Center'
				WHEN IncidentEvent.address_streetAddress LIKE '701 Oval%' THEN 'Danforth Chapel'
				WHEN IncidentEvent.address_streetAddress LIKE '950 W Plum%' THEN 'Durrell Center'
				WHEN IncidentEvent.address_streetAddress LIKE '1231 Center Aven%' THEN 'Eddy Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1231 CENTER%' THEN 'Eddy Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '400 Isotope%' THEN 'Engineering Building'
				WHEN IncidentEvent.address_streetAddress LIKE '501 W Plum%' THEN 'Engineering Lot'
				WHEN IncidentEvent.address_streetAddress LIKE '430 N College%' THEN 'Engines Lab'
				WHEN IncidentEvent.address_streetAddress LIKE '1251 S Mason%' THEN 'GSB'
				WHEN IncidentEvent.address_streetAddress LIKE '920 Oval%' THEN 'Gibbons Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '502 W Lake%' THEN 'Gifford Building'
				WHEN IncidentEvent.address_streetAddress LIKE '901 S Colleg%' THEN 'Glenn Morris Field House'
				WHEN IncidentEvent.address_streetAddress LIKE '451 Isotope%' THEN 'Glover Building'
				WHEN IncidentEvent.address_streetAddress LIKE '750 Meridian%' THEN 'Green Hall - CSUPD'
				WHEN IncidentEvent.address_streetAddress LIKE '750 S Meridian%' THEN 'Green Hall - CSUPD'
				WHEN IncidentEvent.address_streetAddress LIKE '600 Hughes%' THEN 'Hartshorn'
				WHEN IncidentEvent.address_streetAddress LIKE '911 W Plum%' THEN 'HES Building'
				WHEN IncidentEvent.address_streetAddress LIKE '1803 Bay%' THEN 'Horticulture Farm'
				WHEN IncidentEvent.address_streetAddress LIKE '555 S Howes%' THEN 'HSBC'
				WHEN IncidentEvent.address_streetAddress LIKE '821 W Plum%' THEN 'Indoor Practice Facility'
				WHEN IncidentEvent.address_streetAddress LIKE '251 W Laurel%' THEN 'Industrial Sciences'
				WHEN IncidentEvent.address_streetAddress LIKE '1150 S Mason%' THEN 'Jack Track'
				WHEN IncidentEvent.address_streetAddress LIKE '950 Libbie Coy%' THEN 'Johnson Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1508 Center%' THEN 'Lake St Parking Garage'
				WHEN IncidentEvent.address_streetAddress LIKE '600 University%' THEN 'The Lagoon'
				WHEN IncidentEvent.address_streetAddress LIKE '700 Oval%' THEN 'Laurel Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '1101 Center%' THEN 'Lory Student Center'
				WHEN IncidentEvent.address_streetAddress LIKE '401 W Pitkin%' THEN 'Microbiology Building'
				WHEN IncidentEvent.address_streetAddress LIKE '251 University%' THEN 'Military Science Building'
				WHEN IncidentEvent.address_streetAddress LIKE '951 W Plum%' THEN 'Moby Arena'
				WHEN IncidentEvent.address_streetAddress LIKE '1201 Center%' THEN 'Morgan Library'
				WHEN IncidentEvent.address_streetAddress LIKE '201 W Pitkin%' THEN 'Motor Pool'
				WHEN IncidentEvent.address_streetAddress LIKE '1345 Center%' THEN 'MRB'
				WHEN IncidentEvent.address_streetAddress LIKE '400 University%' THEN 'Natural Resources Building'
				WHEN IncidentEvent.address_streetAddress LIKE '1111 S Mason%' THEN 'NCGRP (Seed Lab)'
				WHEN IncidentEvent.address_streetAddress LIKE '1231 LIBBIE COY%' THEN 'NESB'
				WHEN IncidentEvent.address_streetAddress LIKE '850 Oval%' THEN 'Occupational Therapy Building'
				WHEN IncidentEvent.address_streetAddress LIKE '1005 W Laurel%' THEN 'Palmer Center'
				WHEN IncidentEvent.address_streetAddress LIKE '300 W Lake%' THEN 'Pathology'
				WHEN IncidentEvent.address_streetAddress LIKE '630 W Lake%' THEN 'PERC'
				WHEN IncidentEvent.address_streetAddress LIKE '1380 CENTER AVE%' THEN 'Physics Building'
				WHEN IncidentEvent.address_streetAddress LIKE '400 W Lake%' THEN 'Physiology'
				WHEN IncidentEvent.address_streetAddress LIKE '307 University%' THEN 'Plant Sciences'
				WHEN IncidentEvent.address_streetAddress LIKE '1203 CENTER%' THEN 'The Plaza'
				WHEN IncidentEvent.address_streetAddress LIKE '522 W Lake%' THEN 'Richardson Design Center'
				WHEN IncidentEvent.address_streetAddress LIKE '501 W Laurel%' THEN 'Rockwell Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '523 W LAUREL%' THEN 'Rockwell West'
				WHEN IncidentEvent.address_streetAddress LIKE '151 W Laurel%' THEN 'Routt Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '121 W Pitkin%' THEN 'S College Parking Garage'
				WHEN IncidentEvent.address_streetAddress LIKE '700 S Mason%' THEN 'Sage Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '301 University%' THEN 'Shepardson Building'
				WHEN IncidentEvent.address_streetAddress LIKE '150 Old Main%' THEN 'Spruce Hall'
				WHEN IncidentEvent.address_streetAddress LIKE '851 Oval%' THEN 'Statistics Building'
				WHEN IncidentEvent.address_streetAddress LIKE '951 Meridian%' THEN 'Student Rec Center'
				WHEN IncidentEvent.address_streetAddress LIKE '201 W Lake%' THEN 'Surplus Property'
				WHEN IncidentEvent.address_streetAddress LIKE '801 Oval%' THEN 'TILT Building'
				WHEN IncidentEvent.address_streetAddress LIKE '2304 RESEARCH%' THEN 'Tennis Complex'
				WHEN IncidentEvent.address_streetAddress LIKE '2400 Research%' THEN 'Tennis Courts'
				WHEN IncidentEvent.address_streetAddress LIKE '1400 Remington%' THEN 'UCA'
				WHEN IncidentEvent.address_streetAddress LIKE '1504 REMINGTON%' THEN 'University House on Remington'
				WHEN IncidentEvent.address_streetAddress LIKE '1130 Max Guideway%' THEN 'University Max Station'
				WHEN IncidentEvent.address_streetAddress LIKE '601 S Howes%' THEN 'University Services Center'
				WHEN IncidentEvent.address_streetAddress LIKE '300 W Drake%' THEN 'VTH'
				WHEN IncidentEvent.address_streetAddress LIKE '3745 E Prospect%' THEN 'Visitors Center'
				WHEN IncidentEvent.address_streetAddress LIKE '551 W PITKIN%' THEN 'Visual Arts'
				WHEN IncidentEvent.address_streetAddress LIKE '951 Amy Van%' THEN 'Wagar Building'
				WHEN IncidentEvent.address_streetAddress LIKE '300 W PITKIN%' THEN 'Weed Research Facility'
				WHEN IncidentEvent.address_streetAddress LIKE '1330 CENTER AV%' THEN 'Yates Hall'
					--Else the physical Address
				WHEN IncidentEvent.address_streetAddress IS NULL THEN 'Missing Information'
				ELSE IncidentEvent.address_streetAddress
			END AS 'Location'
			,IncidentEvent.status_Description AS 'status' --for easier classification in CASE statement below
			,IncidentEvent.disposition_Description AS 'dispo' --only for dispo classification in next level
			,ROW_NUMBER() OVER (partition BY IncidentOffense.ViolationCodeReference_Description ORDER BY Incident.CaseNumber DESC) AS incident_count
		FROM   
			InformRMSReports.Reporting.Incident Incident
			INNER JOIN InformRMSReports.Reporting.IncidentEvent IncidentEvent
				ON Incident.Id = IncidentEvent.Incident_Id
			INNER JOIN InformRMSReports.Reporting.IncidentOffense IncidentOffense
				ON Incident.Id = IncidentOffense.Incident_Id
			INNER JOIN InformRMSReports.Reporting.IncidentOfficer
				ON Incident.Id = IncidentOfficer.Incident_Id
		WHERE  
			Incident.CaseNumber LIKE 'CS%'
			AND
			IncidentOfficer.involvementType_Description = 'Reporting'
			AND
			Incident.CaseNumber NOT LIKE '%-9999999'
			AND
			IncidentEvent.dateReported BETWEEN DATEADD(DAY, -62, GETDATE()) AND DATEADD(DAY, 0, GETDATE())
			AND
			NOT(IncidentOffense.ViolationCodeReference_Description LIKE '%Information only report%'
				OR 
				IncidentOffense.ViolationCodeReference_Description LIKE '%Mental Health Hold%'
				OR
				IncidentOffense.ViolationCodeReference_Description LIKE '%Private Tow%')
		)
		SELECT
			Description
			,Case_n AS 'Case #'
			,Reported AS 'Reported Time'
			,Occurred AS 'Occurred Date/Time'
			,Location
			,CASE
				WHEN Description IN ('Found Property', 'Lost Property') THEN 'Closed: No Further Action Necessary'
				WHEN status IS NULL OR status = '' THEN 'Status Pending'
				WHEN status = 'PENDING' THEN 'Pending'
				WHEN status = 'Unfounded' THEN 'Unfounded'
				WHEN status = 'ACTIVE' THEN 'Active/Open Investigation'
				WHEN Description IN ('Assist to Fort Collins Police', 'Assist to LCSO', 'Assist to Other LE Agency') THEN 'Closed: Assist to Other LE Agency'
				WHEN Description = 'Assist to Fire Dept' THEN 'Closed: Assist to Fire Dept'
				WHEN Description = 'Assist to Medical' THEN 'Closed: Assist to Medical'
				WHEN (Description IN ('Liquor Law Violation', 'Drug Law Violation', 'Weapon Law Violation') AND status IN ('CLOSED - INFORMATION REPORT', 'EXCEPTIONALLY CLEARED', 'CLEARED')) THEN 'Closed: Disciplinary Referral'
				WHEN dispo IN ('Cleared by Arrest', 'Cleared by Arrest by Another Agency') THEN 'Closed: Arrest'
				WHEN status IN ('IN SUSPENSE', 'INACTIVE') THEN 'Closed: In Suspense'
				WHEN dispo IN ('CLOSED', 'EXCEPTIONAL CLEARANCE') THEN 'Closed: No Further Action Necessary'
				ELSE UPPER(LEFT(status,1)) + LOWER(RIGHT(status, LEN(status)-1)) + ': ' + UPPER(LEFT(dispo, 1)) + LOWER(RIGHT(dispo,LEN(dispo)-1))
			END AS 'Disposition'
		FROM
			incidents
		WHERE
			Description NOT IN ('Traffic Violation', 'Error/Cancelled')
		ORDER BY
			Case_n;
	"""

	# Set date and time format for file name
	# Note to self: update to save to y:drive under proper year
	todays_date = time.strftime("%Y-%m-%d")
	current_year = time.strftime("%Y") # for finding folder year

	clery_log_file_name = "daily_log_" + todays_date
	clery_log_file = f"C:\\Clery\\DailyLogArchive\\{clery_log_file_name}.csv"
	output_file = 'C:\\Clery\\Daily_Crime_Log.csv'

	# Create the database connection string and engine
	db_connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
	engine = create_engine(db_connection_string)

	# Execute query and retrieve results
	df_sql = pd.read_sql_query(sql=text(sql_query), con=engine.connect())
	df_excel = pd.read_excel(r'C:\Users\jmcclure\OneDrive - Colostate\Clery\CSA Reports.xlsx')

	# Combine dataframes
	df = pd.concat([df_sql, df_excel], ignore_index=True)

	# Convert the reported time to datetime data type and format to MM/dd/yyyy HH:mm
	df['Reported Time'] = pd.to_datetime(df['Reported Time'])
	df['Reported Time'] = df['Reported Time'].dt.strftime('%m/%d/%Y %H:%M')
	df['Reported Time'] = pd.to_datetime(df['Reported Time'])

	#Set date range to filter for the last 62 days
	end_date = datetime.now()
	start_date = end_date - timedelta(days=62)

	# Filter df to include only records within the above date range
	df_filtered = df[(df['Reported Time'] >= start_date) & (df['Reported Time'] <= end_date)]

	# Export to CSV
	df_filtered.to_csv(output_file, index=False)
	df_filtered.to_csv(clery_log_file, index=False) #For archive

except Exception as e:
	with open('error_log.txt', "w") as f:
		f.write("An error occurred:\n")
		f.write(str(e) + "\n")
		traceback.print.exc(file=f)
