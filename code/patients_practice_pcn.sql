with dupes as (
  
  select patient_id,count(*)
  from {{ ref('raw_patients_transformed') }}
  group by 1
  having count(*) > 1
)
    
  select 
  pt.*,
  pc.practice_name,
  pc.pcn,
  pcn.pcn_name,
  if(d.patient_id is null,true,false) as valid_patient_id_flag
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
left outer join dupes d
on pt.patient_id = d.patient_id
where pc.id <> 999
