-- Unfortunately the raw data has poor data quality. How can we handle data quality and integrity?
-- How many patients belong to each PCN?

select pcn.pcn_name,
count(distinct concat(practice_id,patient_id)) as patients
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = cast(pc.id as string)
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
group by all

-- What's the average patient age per practice?

select pc.practice_name,
round(avg(cast(age as int64)),1) as age
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = cast(pc.id as string)
where cast(age as int64) >=0
and cast(age as int64) <= 110
group by all

-- Categorize patients into age groups (0-18, 19-35, 36-50, 51+) and show the count per group per PCN

select pcn.pcn_name,
case when cast(age as int64) <= 18 then '0.18'
    when cast(age as int64) <= 35 then '19-35'
    when cast(age as int64) <= 50 then '36-50'
    when cast(age as int64) >50 then '51+'
    end as age_group,
count(distinct concat(practice_id,patient_id)) as patients
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = cast(pc.id as string)
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
where cast(age as int64) >=0
and cast(age as int64) <= 110
group by all


-- What percentage of patients have Hypertension at each practice?
select pc.practice_name,
count(distinct if(lower(conditions) like '%hypertension%',patient_id,null)) as hyp_patients,
count(distinct patient_id) as total_patients,
safe_divide(count(distinct if(lower(conditions) like '%hypertension%',patient_id,null)),
count(distinct patient_id)) as hyp_prop
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = cast(pc.id as string)
group by all
  
-- For each patient, show their most recent activity date
select
a.patient_id,
pt.email,
pt.practice_id,
a.activity_type,
cast(activity_date as date) as latest_activity_date
from {{ ref('raw_activities') }} a
inner join {{ ref('raw_patients_transformed') }} pt
on cast(a.patient_id as string) = pt.patient_id
where a.duration_minutes > 0
qualify row_number() over (partition by a.patient_id,pt.practice_id order by activity_date) =1

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
on cast(a.patient_id as string) = pt.patient_id
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
)

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
