-- Retrieves requirement start and end year pairs
SELECT DISTINCT
  TRIM(rqfyt) AS rqfyt,
  TRIM(lyt) AS lyt
FROM asu_dars_uachieve.req_main;