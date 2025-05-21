select
pt.unique_patient_id
a.patient_id,
pt.email,
pt.practice_id,
a.activity_type,
a.activity_date,
if(date_diff(current_date(),activity_date,day) >= 90,true,false) as activity_over90d_flag,
row_number() over (partition by a.patient_id,pt.practice_id,email order by activity_date) as event_number
from {{ ref('raw_activities') }} a
inner join {{ ref('patients_practice_pcn') }} pt
on a.patient_id = pt.patient_id
where a.duration_minutes > 0
and pt.valid_patient_id_flag = true
