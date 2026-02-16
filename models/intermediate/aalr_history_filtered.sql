-- Find the latest file (by minimum priority) that was received for each enroll_month for each performance_year
WITH latest_priority AS (
  SELECT
    performance_year,
    enroll_month,
    MIN(priority) as min_priority
  FROM {{ ref('aalr_history')}}
  GROUP BY
    performance_year,
    enroll_month
)

-- Get latest priority of record based on row_number, and only keep records that were received on latest file for that report period
SELECT
  ap.*
FROM {{ ref('aalr_history')}} as ap
LEFT JOIN latest_priority as lp ON ap.enroll_month = lp.enroll_month AND ap.performance_year = lp.performance_year
WHERE row_number = 1
  AND ap.priority = lp.min_priority