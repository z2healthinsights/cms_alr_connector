-- Create scaffold of 12 month numbers to join against to enrollflags
WITH scaffold_months AS (
    {% for month in range(1, 13) %}
      SELECT {{ month }} AS month_number
      {% if not loop.last %}
        UNION ALL
      {% endif %}
    {% endfor %}
),

-- Cross join scaffoled months to each enrollment record, set enroll_flag if
-- enrollflag is set for the given month
-- Example: 
-- enrollflag1 and month_number = 1 then enrollflag1 value, otherwise NULL
-- enrollflag2 and month_number = 2 then enrollflag2 value, otherwise NULL
aalr_exploded_by_enrollflag AS (
  SELECT
    sm.month_number,
    CASE sm.month_number
        {% for month in range(1, 13) %}
        WHEN {{ month }} THEN b.enrollflag{{ month }}
        {% endfor %}
    END AS enroll_flag,
    b.*
  FROM {{ ref('stg_aalr1_assigned_beneficiaries')}} b
  CROSS JOIN scaffold_months sm
),

add_calculated_month AS (
  SELECT 
    CAST(date_add(mfp.period_start_date, INTERVAL (aalr.month_number - 1) MONTH) AS date) as enroll_month, -- @TODO - Refactor to macro
    aalr.*,
    mfp.*
  FROM aalr_exploded_by_enrollflag as aalr
  LEFT JOIN {{ ref('mssp_file_parameters') }} as mfp ON aalr.PERFORMANCE_YEAR = mfp.PERFORMANCE_YEAR AND aalr.file_period = mfp.file_period
),

top_tin AS (
  SELECT
    file_period,
    performance_year,
    ITERATION,
    BENE_MBI_ID,
    MASTER_ID,
    B_EM_LINE_CNT_T
  FROM {{ ref('stg_aalr2_assigned_beneficiaries_tin')}}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY BENE_MBI_ID, file_period, performance_year, ITERATION ORDER BY B_EM_LINE_CNT_T DESC) = 1
),

top_npi AS (
  SELECT
    BENE_MBI_ID,
    file_period,
    performance_year,
    ITERATION,
    MASTER_ID,
    NPI_USED,
    PCS_COUNT
  FROM {{ ref('stg_aalr4_assigned_beneficiaries_tin_npi')}}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY BENE_MBI_ID, file_period, performance_year, ITERATION, MASTER_ID ORDER BY PCS_COUNT DESC) = 1
),

assignable_or_voluntary AS (
  
)

SELECT
  row_number() OVER(PARTITION BY acm.BENE_MBI_ID, acm.enroll_month ORDER BY acm.priority, acm.performance_year DESC, acm.ITERATION DESC) AS row_number,
  acm.*,
  tt.MASTER_ID as TOP_TIN,
  tt.B_EM_LINE_CNT_T as TIN_EM_COUNT,
  tn.NPI_USED as TOP_NPI,
  tn.PCS_COUNT as NPI_EM_COUNT
FROM add_calculated_month as acm
LEFT JOIN top_tin as tt
  ON acm.BENE_MBI_ID = tt.BENE_MBI_ID
  AND acm.FILE_PERIOD = tt.FILE_PERIOD
  AND acm.PERFORMANCE_YEAR = tt.PERFORMANCE_YEAR
  -- AND acm.ITERATION = tt.ITERATION -- Normalize iterations to not include file types, then add back in
LEFT JOIN top_npi AS tn
  ON acm.BENE_MBI_ID = tn.BENE_MBI_ID
  -- Only grap NPIs from the top TIN (it's possible an individual NPI NOT assigned to the top TIN has the most visits)
  AND tt.MASTER_ID = tn.MASTER_ID 
  AND acm.FILE_PERIOD = tn.FILE_PERIOD
  AND acm.PERFORMANCE_YEAR = tn.PERFORMANCE_YEAR
  -- AND acm.ITERATION = tn.ITERATION -- Normalize iterations to not include file types, then add back in
