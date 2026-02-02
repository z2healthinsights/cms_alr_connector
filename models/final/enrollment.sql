SELECT
  BENE_MBI_ID as current_bene_mbi_id,
  period_start_date as enrollment_start_date, -- @TODO - Refactor, period start date may be before start of contiguous enrollment period
  period_end_date as enrollment_end_date, -- @TODO - Refactor, period end date may be after start of contiguous enrollment period
  enroll_month as bene_member_month,
  file_name,
  period_end_date as file_date
FROM {{ ref('aalr_history_filtered') }} as ahf
WHERE ahf.enroll_flag > 0