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

select pcn.pcn_name,
count(distinct patient_id) as patients,
count(distinct concat(practice_id,patient_id,email)) as practice_patients
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
group by all

pcn_name	patients	practice_patients
streamline proactive mindshare PCN	87	74
visualize virtual niches PCN	97	89

/*comment:
concat needed as there are duplicate patient ids by practice,unique by combo of patient id, practice id and email
*/

------------------------
-- What's the average patient age per practice?
------------------------

select pc.practice_name,
round(avg(age),1) as age
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
and pc.id <> 999
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
    
select pcn.pcn_name,
case when age <= 18 then '0-18'
    when age <= 35 then '19-35'
    when age <= 50 then '36-50'
    when age >50 then '51+'
    end as age_group,
count(distinct concat(practice_id,patient_id,email)) as patients
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
where age >=0
and age <= 110
group by all
order by 1,2

--invalid practice doesn't have a pcn join so filter not needed

pcn_name	age_group	patients
streamline proactive mindshare PCN	0-18	4
streamline proactive mindshare PCN	19-35	14
streamline proactive mindshare PCN	36-50	8
streamline proactive mindshare PCN	51+	31
visualize virtual niches PCN	0-18	15
visualize virtual niches PCN	19-35	15
visualize virtual niches PCN	36-50	20
visualize virtual niches PCN	51+	19

------------------------
-- What percentage of patients have Hypertension at each practice?
------------------------
select pc.practice_name,
count(distinct if(lower(conditions) like '%hypertension%',concat(email,patient_id),null)) as hyp_patients,
count(distinct concat(email,patient_id)) as total_patients,
round(safe_divide(count(distinct if(lower(conditions) like '%hypertension%',patient_id,null)),
count(distinct concat(email,patient_id))),3) as hyp_prop
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
and pc.id <> 999
group by all


practice_name	hyp_patients	total_patients	hyp_prop
"Hayes, Walker and Williams Clinic"	12	37	0.324
"Foster, West and Miller Clinic"	16	37	0.432
Dominguez Ltd Clinic	21	51	0.431
Meza-Smith Clinic	15	38	0.447

------------------------
-- For each patient, show their most recent activity date
------------------------
    
with dupes as (
  
  select patient_id,count(*)
  from {{ ref('raw_patients_transformed') }}
  group by 1
  having count(*) > 1
)
  select
a.patient_id,
pt.email,
pt.practice_id,
a.activity_type,
cast(activity_date as date) as latest_activity_date
from {{ ref('raw_activities') }} a
inner join {{ ref('raw_patients_transformed') }} pt
on a.patient_id = pt.patient_id
and pt.practice_id <> 999
left join dupes d
on d.patient_id = a.patient_id
where a.duration_minutes > 0
and d.patient_id is null
qualify row_number() over (partition by a.patient_id,pt.practice_id,email order by activity_date desc) =1


/*count:220, removing those with duplicated patient ids as the activities table doesn't have extra detail to identify the patient (i.e. email)
*/
patient_id	email	practice_id	activity_type	latest_activity_date
1028	johnsonpeter@example.com	2	intro_call	2024-10-31
1043	theresahodge@example.net	2	prescription	2024-08-20
1178	edwardgonzalez@example.com	4	intro_call	2025-03-03
1195	lewistimothy@example.org	3	lab_test	2025-02-28
1004	vsanchez@example.net	3	lab_test	2025-03-06
1064	burnettbelinda@example.com	4	prescription	2024-12-19
1244	bbarajas@example.net	4	consultation	2024-10-08
1030	johnsonbrenda@example.net	4	intro_call	2025-02-11
1056		2	consultation	2025-02-21
1081	bclark@example.com	2	consultation	2025-02-22
1118	michael12@example.org	1	intro_call	2025-02-21
1124	dominique29@example.com	1	prescription	2024-12-11
1059	wesley95@example.com	1	prescription	2025-02-27
1097	jimenezmichele@example.com	4	intro_call	2025-01-28
1020	nancy79@example.com	3	consultation	2025-02-28
1073	kingnatasha@example.org	5	intro_call	2025-03-04
1193	jeffrey99@example.org	4	prescription	2025-02-12
1025	kellylong@example.net	4	consultation	2025-03-02
1027	robertsonlaurie@example.net	1	consultation	2025-02-08
1096	trevor16@example.org	5	lab_test	2025-02-14
1117		1	intro_call	2024-12-06
1171	omorales@example.net	4	intro_call	2025-02-08
1228	kevinlamb@example.org	3	lab_test	2025-03-03
1233	ycunningham@example.com	5	prescription	2025-03-07
1062	thopkins@example.org	4	lab_test	2025-02-17
1068	tracy00@example.org	4	intro_call	2025-01-17
1091	megan21@example.net	3	lab_test	2025-03-03
1061		4	lab_test	2025-02-14
1125	williamsmichael@example.org	5	lab_test	2025-03-01
1163	zpham@example.net	1	intro_call	2025-02-18
1133	todd73@example.org	5	prescription	2025-02-23
1143	williamsonmelissa@example.com	5	prescription	2025-03-05
1046	robert22@example.org	1	lab_test	2025-01-09
1138	qpark@example.net	2	consultation	2025-03-03
1192	matthewandersen@example.net	1	lab_test	2025-02-27
1007	alewis@example.net	3	prescription	2025-02-22
1093	hleach@example.com	4	prescription	2025-03-06
1029	barbara27@example.com	0	lab_test	2025-01-28
1002	williamcook@example.net	0	intro_call	2025-02-21
1017	jorgeedwards@example.org	3	lab_test	2025-02-22
1180	tmcguire@example.org	2	lab_test	2025-02-24
1194	jesus54@example.net	1	prescription	2025-02-14
1226	sherrychristensen@example.org	1	consultation	2025-02-09
1238	jbarr@example.net	1	intro_call	2025-03-07
1100001	jimmy@crimple.org	1	consultation	2023-04-10
1038	sheilaphillips@example.org	0	intro_call	2025-01-16
1109	curtisward@example.com	2	consultation	2025-01-03
1237	wsharp@example.org	4	prescription	2024-12-07
1152	pagelawrence@example.com	3	lab_test	2025-01-25
1083	ramosjason@example.com	3	prescription	2025-02-20
1232	cookjacob@example.net	2	consultation	2025-02-10
1067	onealbobby@example.com	3	prescription	2024-12-03
1141		2	intro_call	2025-03-06
1181	ghughes@example.com	3	prescription	2025-03-06
1205	tcarroll@example.org	2	consultation	2025-01-27
1099		3	consultation	2025-02-15
1119	robertsstephanie@example.org	3	lab_test	2024-12-19
1210	kyle74@example.org	4	intro_call	2025-03-07
1010	heatherboone@example.com	3	prescription	2025-03-06
1031	robert54@example.org	2	lab_test	2025-02-23
1184	perezdanny@example.com	3	consultation	2025-03-02
1196	rosszachary@example.com	2	intro_call	2025-02-20
1009	ipatel@example.net	2	consultation	2025-01-09
1149	brookschelsea@example.com	2	prescription	2025-02-28
1006	taylormichael@example.com	3	prescription	2025-02-07
1011	ronald19@example.net	3	intro_call	2025-02-26
1250	rchavez@example.org	4	consultation	2025-02-24
1045	abauer@example.org	4	prescription	2025-02-22
1075	whitedaniel@example.com	3	lab_test	2025-02-28
1187	jacquelinecisneros@example.org	3	consultation	2025-02-26
1220	baileykevin@example.net	5	prescription	2025-03-07
1168	meagan93@example.com	2	intro_call	2025-03-07
1217	rosborne@example.org	4	consultation	2025-01-15
1177	samuel04@example.net	5	lab_test	2025-02-18
1198	jimmymoore@example.net	3	lab_test	2025-03-05
1021	clementsmatthew@example.org	5	lab_test	2024-12-31
1136	zmcguire@example.com	3	lab_test	2025-03-01
1175	aaron71@example.net	4	consultation	2025-03-07
1240	murraylauren@example.org	4	prescription	2025-02-19
1221		2	consultation	2025-02-28
1069	amy14@example.com	2	prescription	2024-12-23
1078		4	prescription	2025-02-16
1131	mmaxwell@example.com	2	lab_test	2025-03-05
1137	ocabrera@example.org	0	consultation	2025-01-01
1053		2	consultation	2025-03-01
1057		4	intro_call	2024-11-11
1116	hernandezcory@example.net	2	consultation	2025-02-13
1104		4	consultation	2025-03-07
1120	jsweeney@example.com	3	prescription	2025-01-01
1185	jacob13@example.net	5	lab_test	2025-03-06
1222	ihart@example.net	3	lab_test	2025-02-22
1100	idavis@example.net	4	prescription	2025-03-04
1155		5	consultation	2025-03-05
1215	aguilarrobert@example.com	3	prescription	2025-02-25
1065	kelly81@example.org	4	lab_test	2025-02-26
1076	wreynolds@example.net	2	consultation	2025-02-27
1106	dicksonrichard@example.org	5	consultation	2024-12-09
1019	cindymendez@example.net	4	consultation	2025-02-23
1129	arthursmith@example.org	5	prescription	2025-02-28
1189	paul63@example.com	2	lab_test	2025-02-07
1206	richard47@example.com	5	prescription	2025-01-30
1224	leblancjason@example.com	2	lab_test	2025-03-08
1052	kevin88@example.net	5	consultation	2025-02-08
1084	holly34@example.net	1	prescription	2025-03-02
1140	crawfordphilip@example.net	3	intro_call	2024-10-21
1160	williamschristopher@example.org	5	prescription	2025-01-10
1216	santosjessica@example.org	4	consultation	2024-12-29
1241	millerdaniel@example.net	3	lab_test	2025-03-05
1048	martinlogan@example.com	2	intro_call	2025-01-19
1087		4	intro_call	2025-02-16
1144	zsmith@example.com	3	intro_call	2025-02-20
1200	teresa50@example.net	2	intro_call	2025-02-14
1103	jamie24@example.com	2	lab_test	2024-12-10
1114	michaelcunningham@example.org	4	consultation	2025-03-06
1132		3	consultation	2024-09-03
1134	hollowayjessica@example.net	2	lab_test	2025-03-01
1209	ssmith@example.com	3	lab_test	2024-11-24
1139	shannon99@example.org	1	consultation	2025-02-01
1150		1	intro_call	2025-03-05
1243	wolferyan@example.com	5	consultation	2025-02-01
1153	tracey03@example.net	4	consultation	2025-03-07
1172	tiffany22@example.org	4	lab_test	2025-03-04
1179	doylejulie@example.org	2	consultation	2025-03-04
1188	christopher44@example.net	5	intro_call	2025-01-07
1174	trubio@example.net	3	lab_test	2025-02-05
1219	juliehernandez@example.com	2	prescription	2025-01-10
1235		4	lab_test	2024-12-31
1239		1	lab_test	2025-02-23
1159	sheltonmegan@example.com	1	consultation	2025-02-03
1197		4	prescription	2025-02-24
1218	lisa50@example.com	3	consultation	2025-02-26
1204	kellynguyen@example.net	5	intro_call	2025-02-19
1005	kathryncannon@example.org	2	lab_test	2025-02-17
1098	craig06@example.org	5	consultation	2025-02-15
1157	kirbyerica@example.com	1	prescription	2025-03-06
1208	amber43@example.org	5	intro_call	2025-03-06
1225	amberbrown@example.org	3	consultation	2025-02-25
1186	brooke21@example.net	1	prescription	2025-02-26
1016	joshua42@example.com	1	lab_test	2025-03-07
1105	dwarner@example.net	3	lab_test	2025-02-19
1142	chandlersarah@example.com	1	intro_call	2025-01-13
1190	james75@example.com	5	lab_test	2025-03-03
1090		5	consultation	2025-02-16
1122	lcrosby@example.org	4	consultation	2024-11-24
1203	jasonallen@example.net	1	consultation	2025-03-06
1234	vincentpatrick@example.com	4	consultation	2025-03-03
1063	smiththomas@example.org	2	lab_test	2025-02-07
1154	kbarrett@example.com	1	prescription	2025-02-26
1024	lwright@example.com	1	intro_call	2025-03-06
1161	hparker@example.com	5	lab_test	2024-11-27
1162	christianbrown@example.org	5	lab_test	2025-02-10
1032	adamhaas@example.net	3	intro_call	2025-03-02
1077	franklinangela@example.com	4	intro_call	2025-03-07
1040	udavid@example.net	5	prescription	2025-03-08
1044	jacquelinejohnson@example.net	1	lab_test	2025-02-28
1112	msnyder@example.net	1	prescription	2025-02-25
1201	ilawson@example.org	3	consultation	2024-12-29
1079	jeffreycantu@example.net	3	consultation	2025-02-04
1111		5	consultation	2024-12-14
1166	nathan12@example.org	5	lab_test	2025-02-27
1183	danielwood@example.org	3	intro_call	2025-02-06
1213		3	lab_test	2025-03-06
1126	escobarashley@example.net	2	lab_test	2025-02-28
1248	maria02@example.com	3	lab_test	2025-03-01
1055	elizabethmcdonald@example.com	2	lab_test	2025-03-01
1094	bonnie99@example.net	5	lab_test	2025-03-07
1202	tylerlee@example.org	1	intro_call	2025-03-02
1100002	bobby@crimple.org	1	consultation	2023-02-10
1013		5	consultation	2025-03-07
1173	stacyjackson@example.org	5	consultation	2025-02-03
1115	ssimon@example.com	4	consultation	2025-02-20
1080	smoore@example.com	1	intro_call	2024-12-28
1036	bbrown@example.org	3	intro_call	2025-01-13
1041	samuel47@example.net	1	intro_call	2025-03-07
1088	robert54@example.com	3	intro_call	2024-10-17
1022	dclarke@example.org	1	prescription	2025-03-03
1070	cortezshawn@example.com	4	lab_test	2025-03-06
1145	meyerjoseph@example.org	5	prescription	2025-03-08
1107	kjohnson@example.org	1	lab_test	2025-01-02
1148	michaelguzman@example.net	2	prescription	2025-03-07
1014	kimberlybrown@example.org	3	intro_call	2025-03-08
1033	christianalexis@example.org	5	prescription	2024-11-04
1071		1	lab_test	2024-12-09
1182	karenwilson@example.org	1	intro_call	2025-02-07
1223		1	intro_call	2025-02-18
1227	lindabarajas@example.org	1	prescription	2025-02-11
1100003	gobby@pimple.org	1	intro_call	2024-02-20
1127	sherryparker@example.com	3	lab_test	2025-02-24
1191		1	consultation	2025-03-01
1037	christopherdudley@example.net	2	consultation	2025-01-25
1060		5	prescription	2025-03-04
1170		5	prescription	2025-03-06
1058	collinserica@example.com	2	intro_call	2025-02-07
1089		1	lab_test	2025-03-02
1012	asims@example.net	1	lab_test	2025-03-05
1211	amandahamilton@example.com	3	prescription	2025-03-01
1229	bushsamantha@example.org	3	lab_test	2024-10-12
1001	ncollins@example.net	1	lab_test	2025-03-07
1054	brentsharp@example.net	1	consultation	2025-02-04
1082	osmith@example.com	3	intro_call	2025-03-01
1147	susan87@example.org	4	intro_call	2025-03-01
1230	alexandra07@example.org	4	intro_call	2025-02-24
1042	mistyjohnson@example.net	3	intro_call	2025-02-18
1128	kennethgibbs@example.org	2	prescription	2025-01-22
1146		2	lab_test	2024-12-16
1214	beckwendy@example.org	5	prescription	2025-02-06
1102	kevin85@example.com	1	consultation	2025-02-14
1246	samanthacantu@example.com	4	intro_call	2025-03-08
1035	alvaradoyvonne@example.com	4	lab_test	2024-10-20
1156	gregory83@example.net	5	intro_call	2025-03-02
1236	anna24@example.org	3	prescription	2025-03-05
1023	wilsonallen@example.com	2	intro_call	2025-02-25
1085	hallmichelle@example.net	2	intro_call	2025-02-23
1086		3	consultation	2025-03-08
1018	ambersanchez@example.com	4	consultation	2024-12-20
1095	kimberlybell@example.org	3	prescription	2025-03-05
1231	twhitney@example.org	1	intro_call	2025-03-01
1199		1	intro_call	2025-03-06
1110	vsloan@example.com	5	intro_call	2025-02-24
1135	michelle06@example.com	2	consultation	2024-12-28

------------------------
-- Find Patients who had no activity for 3 months after their first activity
------------------------
    
with dupes as (
  
  select patient_id,count(*)
  from {{ ref('raw_patients_transformed') }}
  group by 1
  having count(*) > 1
),
activities_ranked as (

  select
a.patient_id,
pt.email,
pt.practice_id,
a.activity_type,
a.activity_date,
row_number() over (partition by a.patient_id,pt.practice_id,email order by activity_date) as event_number
from {{ ref('raw_activities') }} a
inner join {{ ref('raw_patients_transformed') }} pt
on a.patient_id = pt.patient_id
and pt.practice_id <> 999
left join dupes d
on a.patient_id = d.patient_id
where a.duration_minutes > 0
and d.patient_id is null

),

qualifying_users as (

  select distinct
  patient_id,
  email,
  practice_id,
  from activities_ranked
  where date_diff(current_date(),activity_date,day) >= 90
  and event_number = 1
),


merged as (
select
q.*,
max(event_number) as max_event
from qualifying_users q
inner join activities_ranked a
on q.patient_id = a.patient_id
and q.practice_id = a.practice_id
and q.email = a.email
group by all
having max_event = 1

union distinct 

select distinct q.*,
null as max_event
from qualifying_users q
inner join activities_ranked a1
on q.patient_id = a1.patient_id
and q.practice_id = a1.practice_id
and q.email = a1.email
and a1.event_number = 1
inner join activities_ranked a2
on q.patient_id = a2.patient_id
and q.practice_id = a2.practice_id
and q.email = a2.email
and a2.event_number = 2
where date_diff(a2.activity_date,a1.activity_date,day) >= 90

)

select * except (max_event) from merged


--count:20
patient_id	email	practice_id
1208	amber43@example.org	5
1028	johnsonpeter@example.com	2
1133	todd73@example.org	5
1243	wolferyan@example.com	5
1183	danielwood@example.org	3
1139	shannon99@example.org	1
1048	martinlogan@example.com	2
1100003	gobby@pimple.org	1
1029	barbara27@example.com	0
1201	ilawson@example.org	3
1100001	jimmy@crimple.org	1
1105	dwarner@example.net	3
1193	jeffrey99@example.org	4
1140	crawfordphilip@example.net	3
1002	williamcook@example.net	0
1088	robert54@example.com	3
1219	juliehernandez@example.com	2
1058	collinserica@example.com	2
1107	kjohnson@example.org	1
1045	abauer@example.org	4
