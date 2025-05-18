SELECT
  JSON_VALUE(data, '$.patient_id') AS patient_id,
  JSON_VALUE(data, '$.practice_id') AS practice_id,
  JSON_VALUE(data, '$.age') AS age,
  JSON_VALUE(data, '$.gender') AS gender,
  JSON_VALUE(data, '$.registration_date') AS registration_date,
  JSON_QUERY(data, '$.conditions') AS conditions,
  JSON_VALUE(data, '$.contact.email') AS email,
  JSON_VALUE(data, '$.contact.phone') AS phone
FROM {{ ref('raw_patients') }}
