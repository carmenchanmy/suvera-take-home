-- Unfortunately the raw data has poor data quality. How can we handle data quality and integrity?
/*
Guidance for practices when doing data entry
Enforce data type entry (e.g. phone numbers can only contain numeric values, gender with fixed options)
Upstream data cleaning
DBT tests
Ensure analytics layer has cleaned/assumed correct data
*/

------------------------
-- How many patients belong to each PCN?
------------------------

select pcn_name,
count(distinct patient_id) as patients,
count(distinct unique_patient_id) as practice_patients
from {{ ref('patients_practice_pcn') }}
group by all

pcn_name	patients	practice_patients
streamline proactive mindshare PCN	87	87
visualize virtual niches PCN	97	102

/*comment:
concat needed as there are duplicate patient ids by practice,unique by combo of patient id, practice id and email
*/

------------------------
-- What's the average patient age per practice?
------------------------

select pc.practice_name,
round(avg(age),1) as age
from {{ ref('patients_practice_pcn') }}
where age >= 0
and age <= 110
group by all

practice_name	age
"Hayes, Walker and Williams Clinic"	50.3
"Foster, West and Miller Clinic"	53.5
Dominguez Ltd Clinic	44.0
Meza-Smith Clinic	43.1

------------------------
-- Categorize patients into age groups (0-18, 19-35, 36-50, 51+) and show the count per group per PCN
------------------------
    
select pcn_name,
case when age <= 18 then '0-18'
    when age <= 35 then '19-35'
    when age <= 50 then '36-50'
    when age >50 then '51+'
    end as age_group,
count(distinct unique_patient_id) as patients
from {{ ref('patients_practice_pcn') }}
where age >=0
and age <= 110
group by all
order by 1,2

--invalid practice doesn't have a pcn join so filter not needed

pcn_name	age_group	patients
streamline proactive mindshare PCN	0-18	6
streamline proactive mindshare PCN	19-35	19
streamline proactive mindshare PCN	36-50	9
streamline proactive mindshare PCN	51+	35
visualize virtual niches PCN	0-18	18
visualize virtual niches PCN	19-35	16
visualize virtual niches PCN	36-50	20
visualize virtual niches PCN	51+	27

------------------------
-- What percentage of patients have Hypertension at each practice?
------------------------
select practice_name,
count(distinct if(lower(conditions) like '%hypertension%',unique_patient_id,null)) as hyp_patients,
count(distinct unique_patient_id) as total_patients,
round(safe_divide(count(distinct if(lower(conditions) like '%hypertension%',unique_patient_id,null)),
count(distinct unique_patient_id)),3) as hyp_prop
from {{ ref('patients_practice_pcn') }}
group by all


practice_name	hyp_patients	total_patients	hyp_prop
"Hayes, Walker and Williams Clinic"	12	45	0.267
"Foster, West and Miller Clinic"	16	42	0.381
Dominguez Ltd Clinic	22	56	0.393
Meza-Smith Clinic	17	46	0.37

------------------------
-- For each patient, show their most recent activity date
------------------------
    
select
patient_id,
email,
practice_id,
activity_type,
cast(activity_date as date) as latest_activity_date
from {{ ref('patient_activities') }}
qualify row_number() over (partition by unique_patient_id order by activity_date desc) =1


patient_id	email	practice_id	activity_type	latest_activity_date
1135	michelle06@example.com	2	consultation	2024-12-28
1219	juliehernandez@example.com	2	prescription	2025-01-10
1067	onealbobby@example.com	3	prescription	2024-12-03
1018	ambersanchez@example.com	4	consultation	2024-12-20
1048	martinlogan@example.com	2	intro_call	2025-01-19
1056		2	consultation	2025-02-21
1104		4	consultation	2025-03-07
1126	escobarashley@example.net	2	lab_test	2025-02-28
1228	kevinlamb@example.org	3	lab_test	2025-03-03
1086		3	consultation	2025-03-08
1152	pagelawrence@example.com	3	lab_test	2025-01-25
1180	tmcguire@example.org	2	lab_test	2025-02-24
1116	hernandezcory@example.net	2	consultation	2025-02-13
1093	hleach@example.com	4	prescription	2025-03-06
1103	jamie24@example.com	2	lab_test	2024-12-10
1127	sherryparker@example.com	3	lab_test	2025-02-24
1109	curtisward@example.com	2	consultation	2025-01-03
1186	brooke21@example.net	1	prescription	2025-02-26
1194	jesus54@example.net	1	prescription	2025-02-14
1213		3	lab_test	2025-03-06
1007	alewis@example.net	3	prescription	2025-02-22
1058	collinserica@example.com	2	intro_call	2025-02-07
1234	vincentpatrick@example.com	4	consultation	2025-03-03
1248	maria02@example.com	3	lab_test	2025-03-01
1239		1	lab_test	2025-02-23
1193	jeffrey99@example.org	4	prescription	2025-02-12
1061		4	lab_test	2025-02-14
1042	mistyjohnson@example.net	3	intro_call	2025-02-18
1044	jacquelinejohnson@example.net	1	lab_test	2025-02-28
1112	msnyder@example.net	1	prescription	2025-02-25
1059	wesley95@example.com	1	prescription	2025-02-27
1095	kimberlybell@example.org	3	prescription	2025-03-05
1157	kirbyerica@example.com	1	prescription	2025-03-06
1079	jeffreycantu@example.net	3	consultation	2025-02-04
1149	brookschelsea@example.com	2	prescription	2025-02-28
1136	zmcguire@example.com	3	lab_test	2025-03-01
1189	paul63@example.com	2	lab_test	2025-02-07
1179	doylejulie@example.org	2	consultation	2025-03-04
1227	lindabarajas@example.org	1	prescription	2025-02-11
1001	ncollins@example.net	1	lab_test	2025-03-07
1223		1	intro_call	2025-02-18
1192	matthewandersen@example.net	1	lab_test	2025-02-27
1216	santosjessica@example.org	4	consultation	2024-12-29
1224	leblancjason@example.com	2	lab_test	2025-03-08
1240	murraylauren@example.org	4	prescription	2025-02-19
1171	omorales@example.net	4	intro_call	2025-02-08
1100001	jimmy@crimple.org	1	consultation	2023-04-10
1100	idavis@example.net	4	prescription	2025-03-04
1168	meagan93@example.com	2	intro_call	2025-03-07
1191		1	consultation	2025-03-01
1030	johnsonbrenda@example.net	4	intro_call	2025-02-11
1053		2	consultation	2025-03-01
1076	wreynolds@example.net	2	consultation	2025-02-27
1031	robert54@example.org	2	lab_test	2025-02-23
1045	abauer@example.org	4	prescription	2025-02-22
1222	ihart@example.net	3	lab_test	2025-02-22
1091	megan21@example.net	3	lab_test	2025-03-03
1174	trubio@example.net	3	lab_test	2025-02-05
1225	amberbrown@example.org	3	consultation	2025-02-25
1046	robert22@example.org	1	lab_test	2025-01-09
1102	kevin85@example.com	1	consultation	2025-02-14
1055	elizabethmcdonald@example.com	2	lab_test	2025-03-01
1118	michael12@example.org	1	intro_call	2025-02-21
1124	dominique29@example.com	1	prescription	2024-12-11
1035	alvaradoyvonne@example.com	4	lab_test	2024-10-20
1150		1	intro_call	2025-03-05
1147	susan87@example.org	4	intro_call	2025-03-01
1232	cookjacob@example.net	2	consultation	2025-02-10
1120	jsweeney@example.com	3	prescription	2025-01-01
1017	jorgeedwards@example.org	3	lab_test	2025-02-22
1181	ghughes@example.com	3	prescription	2025-03-06
1077	franklinangela@example.com	4	intro_call	2025-03-07
1226	sherrychristensen@example.org	1	consultation	2025-02-09
1085	hallmichelle@example.net	2	intro_call	2025-02-23
1117		1	intro_call	2024-12-06
1202	tylerlee@example.org	1	intro_call	2025-03-02
1037	christopherdudley@example.net	2	consultation	2025-01-25
1054	brentsharp@example.net	1	consultation	2025-02-04
1142	chandlersarah@example.com	1	intro_call	2025-01-13
1131	mmaxwell@example.com	2	lab_test	2025-03-05
1198	jimmymoore@example.net	3	lab_test	2025-03-05
1084	holly34@example.net	1	prescription	2025-03-02
1205	tcarroll@example.org	2	consultation	2025-01-27
1025	kellylong@example.net	4	consultation	2025-03-02
1083	ramosjason@example.com	3	prescription	2025-02-20
1087		4	intro_call	2025-02-16
1089		1	lab_test	2025-03-02
1218	lisa50@example.com	3	consultation	2025-02-26
1010	heatherboone@example.com	3	prescription	2025-03-06
1070	cortezshawn@example.com	4	lab_test	2025-03-06
1032	adamhaas@example.net	3	intro_call	2025-03-02
1215	aguilarrobert@example.com	3	prescription	2025-02-25
1099		3	consultation	2025-02-15
1237	wsharp@example.org	4	prescription	2024-12-07
1006	taylormichael@example.com	3	prescription	2025-02-07
1069	amy14@example.com	2	prescription	2024-12-23
1105	dwarner@example.net	3	lab_test	2025-02-19
1178	edwardgonzalez@example.com	4	intro_call	2025-03-03
1203	jasonallen@example.net	1	consultation	2025-03-06
1250	rchavez@example.org	4	consultation	2025-02-24
1081	bclark@example.com	2	consultation	2025-02-22
1229	bushsamantha@example.org	3	lab_test	2024-10-12
1140	crawfordphilip@example.net	3	intro_call	2024-10-21
1159	sheltonmegan@example.com	1	consultation	2025-02-03
1184	perezdanny@example.com	3	consultation	2025-03-02
1009	ipatel@example.net	2	consultation	2025-01-09
1036	bbrown@example.org	3	intro_call	2025-01-13
1134	hollowayjessica@example.net	2	lab_test	2025-03-01
1200	teresa50@example.net	2	intro_call	2025-02-14
1100003	gobby@pimple.org	1	intro_call	2024-02-20
1153	tracey03@example.net	4	consultation	2025-03-07
1172	tiffany22@example.org	4	lab_test	2025-03-04
1195	lewistimothy@example.org	3	lab_test	2025-02-28
1138	qpark@example.net	2	consultation	2025-03-03
1210	kyle74@example.org	4	intro_call	2025-03-07
1004	vsanchez@example.net	3	lab_test	2025-03-06
1141		2	intro_call	2025-03-06
1244	bbarajas@example.net	4	consultation	2024-10-08
1062	thopkins@example.org	4	lab_test	2025-02-17
1097	jimenezmichele@example.com	4	intro_call	2025-01-28
1217	rosborne@example.org	4	consultation	2025-01-15
1235		4	lab_test	2024-12-31
1199		1	intro_call	2025-03-06
1241	millerdaniel@example.net	3	lab_test	2025-03-05
1012	asims@example.net	1	lab_test	2025-03-05
1075	whitedaniel@example.com	3	lab_test	2025-02-28
1154	kbarrett@example.com	1	prescription	2025-02-26
1201	ilawson@example.org	3	consultation	2024-12-29
1128	kennethgibbs@example.org	2	prescription	2025-01-22
1139	shannon99@example.org	1	consultation	2025-02-01
1187	jacquelinecisneros@example.org	3	consultation	2025-02-26
1041	samuel47@example.net	1	intro_call	2025-03-07
1175	aaron71@example.net	4	consultation	2025-03-07
1132		3	consultation	2024-09-03
1148	michaelguzman@example.net	2	prescription	2025-03-07
1246	samanthacantu@example.com	4	intro_call	2025-03-08
1064	burnettbelinda@example.com	4	prescription	2024-12-19
1107	kjohnson@example.org	1	lab_test	2025-01-02
1043	theresahodge@example.net	2	prescription	2024-08-20
1114	michaelcunningham@example.org	4	consultation	2025-03-06
1144	zsmith@example.com	3	intro_call	2025-02-20
1068	tracy00@example.org	4	intro_call	2025-01-17
1209	ssmith@example.com	3	lab_test	2024-11-24
1163	zpham@example.net	1	intro_call	2025-02-18
1082	osmith@example.com	3	intro_call	2025-03-01
1119	robertsstephanie@example.org	3	lab_test	2024-12-19
1071		1	lab_test	2024-12-09
1183	danielwood@example.org	3	intro_call	2025-02-06
1196	rosszachary@example.com	2	intro_call	2025-02-20
1014	kimberlybrown@example.org	3	intro_call	2025-03-08
1016	joshua42@example.com	1	lab_test	2025-03-07
1023	wilsonallen@example.com	2	intro_call	2025-02-25
1065	kelly81@example.org	4	lab_test	2025-02-26
1080	smoore@example.com	1	intro_call	2024-12-28
1028	johnsonpeter@example.com	2	intro_call	2024-10-31
1115	ssimon@example.com	4	consultation	2025-02-20
1236	anna24@example.org	3	prescription	2025-03-05
1020	nancy79@example.com	3	consultation	2025-02-28
1027	robertsonlaurie@example.net	1	consultation	2025-02-08
1230	alexandra07@example.org	4	intro_call	2025-02-24
1146		2	lab_test	2024-12-16
1211	amandahamilton@example.com	3	prescription	2025-03-01
1063	smiththomas@example.org	2	lab_test	2025-02-07
1057		4	intro_call	2024-11-11
1122	lcrosby@example.org	4	consultation	2024-11-24
1231	twhitney@example.org	1	intro_call	2025-03-01
1024	lwright@example.com	1	intro_call	2025-03-06
1238	jbarr@example.net	1	intro_call	2025-03-07
1078		4	prescription	2025-02-16
1221		2	consultation	2025-02-28
1100002	bobby@crimple.org	1	consultation	2023-02-10
1197		4	prescription	2025-02-24
1022	dclarke@example.org	1	prescription	2025-03-03
1182	karenwilson@example.org	1	intro_call	2025-02-07
1005	kathryncannon@example.org	2	lab_test	2025-02-17
1011	ronald19@example.net	3	intro_call	2025-02-26
1019	cindymendez@example.net	4	consultation	2025-02-23
1088	robert54@example.com	3	intro_call	2024-10-17
------------------------
-- Find Patients who had no activity for 3 months after their first activity
------------------------
    
with merged as (
select
patient_id,
practice_id,
email,
max(event_number) as max_event
from {{ ref('patient_activities') }}
group by all
having max_event = 1

union distinct 

select distinct 
a1.patient_id,
a1.practice_id,
a1.email,
null as max_event
from {{ ref('patient_activities') }} a1
inner join {{ ref('patient_activities') }} a2
on a1.patient_id = a2.patient_id
and a1.practice_id = a2.practice_id
and a1.email = a2.email
and a2.event_number = 2
where a1.event_number = 1
and date_diff(a2.activity_date,a1.activity_date,day) >= 90

)

select * except (max_event) from merged


--count:15
patient_id	practice_id	email
1028	2	johnsonpeter@example.com
1193	4	jeffrey99@example.org
1045	4	abauer@example.org
1219	2	juliehernandez@example.com
1100001	1	jimmy@crimple.org
1140	3	crawfordphilip@example.net
1201	3	ilawson@example.org
1139	1	shannon99@example.org
1105	3	dwarner@example.net
1107	1	kjohnson@example.org
1058	2	collinserica@example.com
1183	3	danielwood@example.org
1048	2	martinlogan@example.com
1088	3	robert54@example.com
1100003	1	gobby@pimple.org
