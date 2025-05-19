-- Unfortunately the raw data has poor data quality. How can we handle data quality and integrity?
-- How many patients belong to each PCN?

select pcn.pcn_name,
count(distinct patient_id) as patients,
count(distinct concat(practice_id,patient_id)) as practice_patients
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = cast(pc.id as string)
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
group by all

pcn_name	patients	practice_patients
streamline proactive mindshare PCN	87	87
visualize virtual niches PCN	97	99
    
-- What's the average patient age per practice?

select pc.practice_name,
round(avg(age),1) as age
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
where age >= 0
and age <= 110
group by all

practice_name	age
"Hayes, Walker and Williams Clinic"	50.3
"Foster, West and Miller Clinic"	53.5
Dominguez Ltd Clinic	44.0
Meza-Smith Clinic	43.1
Invalid Practice	51.1

-- Categorize patients into age groups (0-18, 19-35, 36-50, 51+) and show the count per group per PCN

select pcn.pcn_name,
case when age <= 18 then '0.18'
    when age <= 35 then '19-35'
    when age <= 50 then '36-50'
    when age >50 then '51+'
    end as age_group,
count(distinct concat(practice_id,patient_id)) as patients
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
where age >=0
and age <= 110
group by all


pcn_name	age_group	patients
visualize virtual niches PCN	0.18	17
visualize virtual niches PCN	51+	27
visualize virtual niches PCN	36-50	20
visualize virtual niches PCN	19-35	16
streamline proactive mindshare PCN	19-35	19
streamline proactive mindshare PCN	51+	35
streamline proactive mindshare PCN	0.18	6
streamline proactive mindshare PCN	36-50	9

-- What percentage of patients have Hypertension at each practice?
select pc.practice_name,
count(distinct if(lower(conditions) like '%hypertension%',patient_id,null)) as hyp_patients,
count(distinct patient_id) as total_patients,
safe_divide(count(distinct if(lower(conditions) like '%hypertension%',patient_id,null)),
count(distinct patient_id)) as hyp_prop
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
group by all


practice_name	hyp_patients	total_patients	hyp_prop
"Hayes, Walker and Williams Clinic"	12	45	0.26666666666666666
"Foster, West and Miller Clinic"	16	42	0.38095238095238093
Dominguez Ltd Clinic	22	54	0.40740740740740738
Meza-Smith Clinic	17	45	0.37777777777777777
Invalid Practice	5	11	0.45454545454545453
    
-- For each patient, show their most recent activity date
select
a.patient_id,
pt.email,
pt.practice_id,
a.activity_type,
cast(activity_date as date) as latest_activity_date
from {{ ref('raw_activities') }} a
inner join {{ ref('raw_patients_transformed') }} pt
on a.patient_id = pt.patient_id
where a.duration_minutes > 0
qualify row_number() over (partition by a.patient_id,pt.practice_id order by activity_date) =1

patient_id	email	practice_id	activity_type	latest_activity_date
1104		4	intro_call	2025-02-19
1190	james75@example.com	5	intro_call	2024-12-25
1201	ilawson@example.org	3	prescription	2024-06-24
1217	rosborne@example.org	4	consultation	2024-10-09
1008	robertmays@example.net		intro_call	2024-11-08
1015	vickieprice@example.net	999	consultation	2025-01-05
1067	onealbobby@example.com	3	lab_test	2024-04-29
1175	aaron71@example.net	4	consultation	2025-02-08
1114	michaelcunningham@example.org	4	prescription	2024-08-20
1173	stacyjackson@example.org	5	prescription	2024-10-04
1198	jimmymoore@example.net	3	consultation	2024-11-05
1227	lindabarajas@example.org	1	prescription	2024-07-24
1157	kirbyerica@example.com	1	intro_call	2025-01-10
1029	barbara27@example.com	0	prescription	2024-05-18
1180	tmcguire@example.org	2	intro_call	2024-09-24
1168	meagan93@example.com	2	intro_call	2025-02-17
1187	jacquelinecisneros@example.org	3	consultation	2024-07-26
1042	mistyjohnson@example.net	3	consultation	2024-12-21
1046	robert22@example.org	1	prescription	2024-09-14
1058	collinserica@example.com	2	intro_call	2024-06-06
1081	bclark@example.com	2	consultation	2024-07-26
1100	idavis@example.net	4	lab_test	2025-02-23
1026	mackelizabeth@example.com	999	intro_call	2025-01-01
1069	amy14@example.com	2	consultation	2024-09-01
1020	nancy79@example.com	3	lab_test	2024-12-10
1082	osmith@example.com	3	consultation	2024-10-25
1160	williamschristopher@example.org	5	prescription	2024-04-08
1122	lcrosby@example.org	4	consultation	2024-05-14
1147	susan87@example.org	4	consultation	2025-02-10
1203	jasonallen@example.net	1	consultation	2025-02-20
1144	zsmith@example.com	3	consultation	2024-10-10
1245	flemingsamantha@example.net	999	consultation	2024-10-07
1060		5	consultation	2024-12-31
1064	burnettbelinda@example.com	4	prescription	2024-07-18
1087		4	intro_call	2024-10-02
1096	trevor16@example.org	5	intro_call	2024-09-14
1236	anna24@example.org	3	prescription	2025-02-23
1044	jacquelinejohnson@example.net	1	prescription	2025-02-06
1220	baileykevin@example.net	5	intro_call	2024-12-30
1247	glenndaniel@example.net	4	consultation	2024-04-29
1103	jamie24@example.com	2	intro_call	2024-06-26
1148	michaelguzman@example.net	2	consultation	2024-10-23
1192	matthewandersen@example.net	1	prescription	2024-08-20
1193	jeffrey99@example.org	4	prescription	2024-06-06
1243	wolferyan@example.com	5	intro_call	2024-05-10
1003		999	consultation	2024-06-22
1005	kathryncannon@example.org	2	consultation	2024-08-24
1013		5	consultation	2024-11-13
1041	samuel47@example.net	1	lab_test	2025-02-18
1047	arroyosamantha@example.org	999	lab_test	2024-05-17
1145	meyerjoseph@example.org	5	intro_call	2025-02-18
1214	beckwendy@example.org	5	consultation	2024-09-08
1183	danielwood@example.org	3	intro_call	2024-03-30
1057		4	consultation	2024-03-27
1197		4	consultation	2024-12-12
1034	seanmarshall@example.net	4	intro_call	2024-04-04
1121			lab_test	2025-02-19
1223		1	lab_test	2025-01-30
1045	abauer@example.org	4	intro_call	2024-05-10
1204	kellynguyen@example.net	5	consultation	2024-12-30
1071		1	lab_test	2024-08-15
1109	curtisward@example.com	2	intro_call	2024-07-19
1132		3	intro_call	2024-04-27
1195	lewistimothy@example.org	3	intro_call	2024-08-14
1228	kevinlamb@example.org	3	lab_test	2024-08-10
1006	taylormichael@example.com	3	intro_call	2024-09-26
1091	megan21@example.net	3	lab_test	2024-03-29
1117		1	lab_test	2024-04-28
1139	shannon99@example.org	1	prescription	2024-05-02
1152	pagelawrence@example.com	3	intro_call	2024-12-08
1022	dclarke@example.org	1	prescription	2024-12-17
1028	johnsonpeter@example.com	2	consultation	2024-04-26
1072	aellis@example.com		consultation	2024-12-11
1128	kennethgibbs@example.org	2	consultation	2024-06-17
1155		5	consultation	2024-08-07
1049	vkeller@example.net	999	consultation	2024-10-18
1108	thomasjeffrey@example.org		consultation	2024-11-28
1178	edwardgonzalez@example.com	4	prescription	2024-09-04
1031	robert54@example.org	2	intro_call	2025-01-22
1007	alewis@example.net	3	lab_test	2024-06-24
1106	dicksonrichard@example.org	5	consultation	2024-08-30
1110	vsloan@example.com	5	prescription	2024-12-20
1149	brookschelsea@example.com	2	intro_call	2024-12-30
1244	bbarajas@example.net	4	consultation	2024-06-02
1246	samanthacantu@example.com	4	lab_test	2024-04-02
1100001	jimmy@crimple.org	1	intro_call	2023-01-10
1009	ipatel@example.net	2	intro_call	2024-09-15
1154	kbarrett@example.com	1	prescription	2024-09-27
1068	tracy00@example.org	4	prescription	2024-08-03
1084	holly34@example.net	1	prescription	2024-07-03
1143	williamsonmelissa@example.com	5	consultation	2025-01-25
1080	smoore@example.com	1	prescription	2024-07-13
1085	hallmichelle@example.net	2	consultation	2024-10-06
1063	smiththomas@example.org	2	prescription	2024-04-05
1231	twhitney@example.org	1	consultation	2024-11-11
1011	ronald19@example.net	3	prescription	2024-10-06
1238	jbarr@example.net	1	intro_call	2025-01-07
1073	kingnatasha@example.org	5	lab_test	2024-08-24
1061		4	intro_call	2024-08-04
1102	kevin85@example.com	1	prescription	2024-08-29
1181	ghughes@example.com	3	consultation	2024-06-11
1018	ambersanchez@example.com	4	intro_call	2024-09-26
1083	ramosjason@example.com	3	intro_call	2024-11-16
1130	fflynn@example.net	4	consultation	2024-09-09
1237	wsharp@example.org	4	prescription	2024-06-27
1021	clementsmatthew@example.org	5	prescription	2024-07-21
1131	mmaxwell@example.com	2	lab_test	2025-02-21
1161	hparker@example.com	5	prescription	2024-07-07
1025	kellylong@example.net	4	consultation	2025-02-18
1185	jacob13@example.net	5	lab_test	2024-10-17
1004	vsanchez@example.net	3	prescription	2024-11-07
1116	hernandezcory@example.net	2	consultation	2024-04-30
1127	sherryparker@example.com	3	intro_call	2024-12-08
1166	nathan12@example.org	5	prescription	2025-01-27
1235		4	lab_test	2024-08-09
1019	cindymendez@example.net	4	lab_test	2024-04-25
1098	craig06@example.org	5	consultation	2024-07-22
1112	msnyder@example.net	1	intro_call	2025-01-24
1038	sheilaphillips@example.org	0	lab_test	2024-10-16
1170		5	intro_call	2024-06-17
1202	tylerlee@example.org	1	lab_test	2024-10-30
1040	udavid@example.net	5	lab_test	2024-12-31
1179	doylejulie@example.org	2	lab_test	2024-09-12
1034		3	intro_call	2024-04-04
1039	morganjulia@example.org	3	consultation	2024-12-21
1059	wesley95@example.com	1	consultation	2024-11-05
1191		1	prescription	2024-11-03
1216	santosjessica@example.org	4	consultation	2024-09-13
1226	sherrychristensen@example.org	1	lab_test	2024-08-12
1088	robert54@example.com	3	prescription	2024-05-20
1123	kevin45@example.net		lab_test	2024-07-21
1225	amberbrown@example.org	3	lab_test	2024-09-02
1141		2	lab_test	2024-12-14
1079	jeffreycantu@example.net	3	prescription	2024-10-10
1158	smitherica@example.org	3	intro_call	2024-09-10
1182	karenwilson@example.org	1	intro_call	2024-12-17
1205	tcarroll@example.org	2	intro_call	2024-08-30
1207	jason18@example.net		lab_test	2024-11-29
1010	heatherboone@example.com	3	intro_call	2025-02-17
1156	gregory83@example.net	5	intro_call	2025-01-06
1150		1	prescription	2024-09-24
1162	christianbrown@example.org	5	prescription	2024-07-23
1240	murraylauren@example.org	4	consultation	2024-06-23
1186	brooke21@example.net	1	intro_call	2025-02-09
1076	wreynolds@example.net	2	prescription	2024-06-25
1200	teresa50@example.net	2	prescription	2024-07-31
1024	lwright@example.com	1	prescription	2024-11-03
1051			consultation	2024-12-13
1065	kelly81@example.org	4	prescription	2024-11-18
1115	ssimon@example.com	4	prescription	2024-07-14
1163	zpham@example.net	1	consultation	2024-11-24
1048	martinlogan@example.com	2	consultation	2024-04-22
1136	zmcguire@example.com	3	lab_test	2025-01-28
1124	dominique29@example.com	1	prescription	2024-03-15
1165			lab_test	2024-11-20
1151	nicolepeck@example.com	999	intro_call	2024-05-27
1215	aguilarrobert@example.com	3	lab_test	2024-10-13
1077	franklinangela@example.com	4	prescription	2025-01-14
1012	asims@example.net	1	prescription	2024-10-09
1218	lisa50@example.com	3	consultation	2024-07-11
1229	bushsamantha@example.org	3	lab_test	2024-04-25
1194	jesus54@example.net	1	lab_test	2024-12-31
1199		1	lab_test	2025-02-23
1174	trubio@example.net	3	intro_call	2024-05-27
1033	christianalexis@example.org	5	prescription	2024-08-06
1092	chanryan@example.com	999	intro_call	2024-09-30
1097	jimenezmichele@example.com	4	consultation	2024-12-17
1052	kevin88@example.net	5	prescription	2024-09-15
1062	thopkins@example.org	4	consultation	2024-03-30
1066	juliesmith@example.net	999	prescription	2024-09-16
1212	michael01@example.com		lab_test	2024-05-30
1027	robertsonlaurie@example.net	1	prescription	2024-10-27
1032	adamhaas@example.net	3	lab_test	2024-06-06
1094	bonnie99@example.net	5	intro_call	2024-12-28
1125	williamsmichael@example.org	5	prescription	2024-06-05
1054	brentsharp@example.net	1	lab_test	2024-10-05
1206	richard47@example.com	5	lab_test	2024-09-15
1050	wclark@example.net	999	intro_call	2025-01-29
1089		1	consultation	2024-11-19
1169	xford@example.org	999	consultation	2024-09-04
1053		2	consultation	2024-04-23
1070	cortezshawn@example.com	4	prescription	2025-01-08
1129	arthursmith@example.org	5	prescription	2025-01-16
1184	perezdanny@example.com	3	prescription	2024-10-30
1100002	bobby@crimple.org	1	intro_call	2023-01-10
1002	williamcook@example.net	0	after_hours	2023-12-25
1210	kyle74@example.org	4	lab_test	2025-02-10
1036	bbrown@example.org	3	lab_test	2024-04-29
1189	paul63@example.com	2	prescription	2024-09-09
1248	maria02@example.com	3	consultation	2024-06-13
1017	jorgeedwards@example.org	3	consultation	2024-10-18
1095	kimberlybell@example.org	3	lab_test	2025-01-17
1159	sheltonmegan@example.com	1	prescription	2024-07-23
1224	leblancjason@example.com	2	prescription	2024-12-21
1153	tracey03@example.net	4	intro_call	2025-02-01
1188	christopher44@example.net	5	lab_test	2024-06-28
1014	kimberlybrown@example.org	3	intro_call	2025-01-25
1078		4	intro_call	2024-09-19
1001	ncollins@example.net	1	lab_test	2024-11-16
1219	juliehernandez@example.com	2	lab_test	2024-10-05
9999	12345		prescription	2025-01-09
1035	alvaradoyvonne@example.com	4	lab_test	2024-05-17
1101	danielle50@example.org		consultation	2024-10-08
1234	vincentpatrick@example.com	4	lab_test	2024-09-26
1105	dwarner@example.net	3	lab_test	2024-07-17
1003	ghurley@example.com	2	consultation	2024-06-22
1030	johnsonbrenda@example.net	4	prescription	2024-11-04
1230	alexandra07@example.org	4	prescription	2024-12-11
1113	michaeljohnson@example.org		prescription	2025-02-12
1126	escobarashley@example.net	2	intro_call	2024-12-30
1016	joshua42@example.com	1	prescription	2024-12-18
1120	jsweeney@example.com	3	lab_test	2024-08-27
1138	qpark@example.net	2	prescription	2024-06-02
1172	tiffany22@example.org	4	intro_call	2025-01-27
1239		1	consultation	2024-05-02
1100003	gobby@pimple.org	1	intro_call	2024-02-20
1130	william38@example.net	3	consultation	2024-09-09
1135	michelle06@example.com	2	consultation	2024-10-29
1250	rchavez@example.org	4	lab_test	2024-05-25
1086		3	lab_test	2024-12-17
1241	millerdaniel@example.net	3	intro_call	2024-12-30
1037	christopherdudley@example.net	2	consultation	2024-07-19
1171	omorales@example.net	4	prescription	2024-10-11
1075	whitedaniel@example.com	3	consultation	2025-01-11
1093	hleach@example.com	4	lab_test	2025-02-10
1111		5	intro_call	2024-07-04
1208	amber43@example.org	5	prescription	2024-06-16
1221		2	consultation	2025-01-05
1222	ihart@example.net	3	consultation	2024-07-03
1107	kjohnson@example.org	1	intro_call	2024-03-30
1146		2	prescription	2024-06-21
1233	ycunningham@example.com	5	lab_test	2024-09-14
1043	theresahodge@example.net	2	intro_call	2024-07-08
1134	hollowayjessica@example.net	2	intro_call	2024-11-18
1213		3	lab_test	2025-02-24
1023	wilsonallen@example.com	2	intro_call	2025-01-12
1055	elizabethmcdonald@example.com	2	intro_call	2024-11-09
1056		2	consultation	2024-09-29
1090		5	consultation	2024-11-28
1140	crawfordphilip@example.net	3	intro_call	2024-04-28
1142	chandlersarah@example.com	1	prescription	2025-01-07
1196	rosszachary@example.com	2	prescription	2024-10-14
1137	ocabrera@example.org	0	consultation	2024-10-23
1232	cookjacob@example.net	2	prescription	2024-10-04
1119	robertsstephanie@example.org	3	prescription	2024-06-27
1177	samuel04@example.net	5	prescription	2024-06-20
1209	ssmith@example.com	3	intro_call	2024-04-12
1099		3	intro_call	2024-03-05
1118	michael12@example.org	1	intro_call	2024-10-29
1133	todd73@example.org	5	consultation	2024-04-26
1211	amandahamilton@example.com	3	prescription	2024-09-23

-- Find Patients who had no activity for 3 months after their first activity

with activities_ranked as (

  select
a.patient_id,
pt.email,
pt.practice_id,
a.activity_type,
a.activity_date,
row_number() over (partition by a.patient_id,pt.practice_id order by activity_date) as event_number
from {{ ref('raw_activities') }} a
inner join {{ ref('raw_patients_transformed') }} pt
on a.patient_id = pt.patient_id
where a.duration_minutes > 0

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


patient_id	email	practice_id
1201	ilawson@example.org	3
1029	barbara27@example.com	0
1100003	gobby@pimple.org	1
1048	martinlogan@example.com	2
1028	johnsonpeter@example.com	2
1208	amber43@example.org	5
1133	todd73@example.org	5
1243	wolferyan@example.com	5
1183	danielwood@example.org	3
1139	shannon99@example.org	1
1100001	jimmy@crimple.org	1
1002	williamcook@example.net	0
1140	crawfordphilip@example.net	3
1193	jeffrey99@example.org	4
1105	dwarner@example.net	3
1219	juliehernandez@example.com	2
1088	robert54@example.com	3
1045	abauer@example.org	4
1058	collinserica@example.com	2
1107	kjohnson@example.org	1
