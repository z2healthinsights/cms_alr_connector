SELECT
  BENE_MBI_ID as current_bene_mbi_id,
  CAST( {{ dbt.date_trunc('month', 'enroll_month') }} AS DATE) as enrollment_start_date,
  CAST( {{ last_day_of_month('enroll_month') }} AS DATE) as enrollment_end_date,
  {{ format_yyyymm('enroll_month') }} as bene_member_month,
  file_name,
  period_end_date as file_date
FROM {{ ref('aalr_history_filtered') }} as ahf
WHERE ahf.enroll_flag > 0