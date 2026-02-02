-- Find the latest file (by priority) that was received for each enroll_month
WITH latest_priority AS (
  SELECT
    enroll_month,
    MIN(priority) as min_priority
  FROM {{ ref('aalr_history')}}
  GROUP BY
    enroll_month
)

-- Get latest priority of record based on row_number, and only keep records that were received on latest file for that report period
SELECT
  ap.*
FROM {{ ref('aalr_history')}} as ap
LEFT JOIN latest_priority as lp ON ap.enroll_month = lp.enroll_month
WHERE row_number = 1
AND ap.priority = lp.min_priority