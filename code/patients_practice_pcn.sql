select 
  pt.*,
  pc.practice_name,
  pc.pcn,
  pcn.pcn_name
from {{ ref('raw_patients_transformed') }} pt
inner join {{ ref('raw_practices') }} pc
on pt.practice_id = pc.id
inner join {{ ref('raw_pcns') }} pcn
on pc.pcn = pcn.id
where pc.id <> 999
