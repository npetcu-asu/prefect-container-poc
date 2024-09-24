-- Joins the course offerings against the crosswalk
SELECT full_crse, crse_id, rqmnt_designtn, ac_cw, units_minimum, units_maximum
FROM ps_course 
LEFT JOIN ps_cw
    ON ps_course.rqmnt_designtn = ps_cw.PS;