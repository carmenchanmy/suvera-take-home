with raw_patients as (
    SELECT
  JSON_VALUE(data, '$.patient_id') AS patient_id,
  JSON_VALUE(data, '$.practice_id') AS practice_id,
  JSON_VALUE(data, '$.age') AS age,
  JSON_VALUE(data, '$.gender') AS gender,
  JSON_VALUE(data, '$.registration_date') AS registration_date,
  JSON_QUERY(data, '$.conditions') AS conditions,
  JSON_VALUE(data, '$.contact.email') AS email,
  JSON_VALUE(data, '$.contact.phone') AS phone
FROM {{ ref('raw_pats') }}
),
data_transform as (
select
safe_cast(patient_id as int64) as patient_id,
safe_cast(practice_id as int64) as practice_id,
safe_cast(age as int64) as age,
gender,
cast(registration_date as date) as registration_date,
conditions,
email,
phone
from raw_patients
)

select 
    concat(patient_id,practice_id,email) as unique_patient_id,
    *
from data_transform
