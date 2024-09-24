-- Retrieves a list of offered courses
WITH course_offering AS (
    SELECT *
    FROM (
        SELECT
            -- Using RANK over ROW_NUM to grab all offerings for the same max effective date as it looks
            -- like it's possible for offerings of the same course ID to have different subject
            -- and catalog numbers.
            RANK() OVER (
                PARTITION BY
                    crse_id
                ORDER BY effdt DESC
            ) AS rownum,
            *
        FROM asu_student_records.ps_crse_offer
    ) AS crse_off_effdt
    WHERE
        rownum = 1
)
SELECT
	DISTINCT TRIM(pco.subject) || ' ' || TRIM(pco.catalog_nbr) AS full_crse,
    pcc.crse_id,
	pcc.rqmnt_designtn,
    pcc.units_minimum,
    pcc.units_maximum
FROM course_offering AS pco
INNER JOIN asu_student_records.ps_crse_catalog AS pcc
  ON pco.crse_id = pcc.crse_id
  AND (
    -- To allow for differences in Maroon and Gold requirement designations, get the max effective dated
    -- course catalog row that's set to the current date or earlier and the max future effective dated row
    -- if one exists.  This will allow for matching on any requirement designation defined for the course.
  	pcc.effdt = (
  		SELECT MAX(pcc_current_ed.effdt)
  		FROM asu_student_records.ps_crse_catalog AS pcc_current_ed
  		WHERE
            pcc_current_ed.crse_id = pcc.crse_id
  		    AND pcc_current_ed.effdt <= CURRENT_DATE
  	)
  	OR pcc.effdt = (
  		SELECT MAX(pcc_future_ed.effdt)
  		FROM asu_student_records.ps_crse_catalog AS pcc_future_ed
  		WHERE
            pcc_future_ed.crse_id = pcc.crse_id
  		    AND pcc_future_ed.effdt > CURRENT_DATE
  	)
)
WHERE
    LENGTH(TRIM(pco.subject)) = 3
	AND LENGTH(TRIM(pco.catalog_nbr)) = 3
	AND pcc.eff_status  != 'I'
    AND pco.schedule_print != 'N';