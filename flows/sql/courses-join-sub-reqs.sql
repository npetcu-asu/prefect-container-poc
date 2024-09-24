-- Get a list of courses offered, along with any associated requirement designation
SELECT
    rname,          --requirement name
    subreq_id,      --same subreq id as the audits use
    rqfyt,          --first active year/term
    lyt,            --last active year/term
    rqfyt2,         --first active year/term for lists referenced
    lyt2,           --last active year/term for lists referenced
    seq1,           --UAchieve sub-requirement number
    seq2,           --UAchieve sort order
    seq4 AS seq3,   --sort order for course lists
    rtitle1,        --name of requirement
    full_crse,      --course name
    crse_id,        --course ID
    units_minimum,  --minimum number of credit hours
    units_maximum,  --maximum number of credit hours
    tflg,           --indicated if the course title is part of the requirement
    ctitle,         --match code for course requirement
    matchctl,       --includes logic about ands/ors, requirements, and other important stuff
    grp,            --Value of "H" means selections are measured by hours, "C" means measured by courses
    grpmin,         --The minimum number of groups in the sub-requirement that must be satisfied
    grpmax,         --The maximum number of groups in the sub-requirement that must be satisfied
    hcmin,          --The minimum number of Hours or Courses that must be completed in each group to satisfy it
    hcmax           --The maximum number of Hours or Courses that must be completed in each group to satisfy it
FROM (
    SELECT
        reqs.rname,
        reqs.subreq_id,
        reqs.rqfyt,
        reqs.rqfyt2,
        reqs.lyt2,
        reqs.lyt,
        reqs.seq1,
        reqs.seq2,
        reqs.seq3,
        reqs.seq4,
        reqs.rtitle1,
        courses.full_crse,
        courses.crse_id,
        courses.units_minimum,
        courses.units_maximum,
        reqs.tflg,
        reqs.ctitle,
        --get only unique values
        ROW_NUMBER() OVER(
            PARTITION BY
            reqs.rname,
            reqs.rqfyt,
            reqs.lyt,
            reqs.rqfyt2,
            reqs.lyt2,
            reqs.seq1,
            reqs.seq2,
            reqs.seq3,
            reqs.seq4,
            reqs.rtitle1,
            courses.full_crse
        ) AS row_num,
        reqs.matchctl,
        reqs.grp,
        reqs.grpmin,
        reqs.grpmax,
        reqs.hcmin,
        reqs.hcmax
		FROM reqs
		--check out courses and accept/reject criteria
    LEFT JOIN courses
        -- Addresses wildcard patterns in the requirement designation by either replacing all '*' with '_' for the
        -- explicit style patterns (MAT ***) or by appending ' ___' to the value for the implicit, subject-only style patterns (MAT).
        ON TRIM(courses.full_crse) LIKE CASE WHEN LENGTH(reqs.course) == 3 THEN reqs.course || ' ___' ELSE REPLACE(reqs.course,'*','_') END
        AND (
            CASE -- check to make sure courses match course code and upper/lower division
                WHEN reqs.upper_lower_acc LIKE '%U%'
                    THEN (
                        TRIM(courses.full_crse) LIKE '%3__'
                        OR TRIM(courses.full_crse) LIKE '%4__'
                        OR TRIM(courses.full_crse) LIKE '%5__'
                        OR TRIM(courses.full_crse) LIKE '%6__'
                        OR TRIM(courses.full_crse) LIKE '%7__')
                WHEN reqs.upper_lower_acc LIKE '%L%'
                    THEN (
                        TRIM(courses.full_crse) LIKE '%1__'
                        OR TRIM(courses.full_crse) LIKE '%2__')
                ELSE 1=1
            END
        )
        AND (
            CASE
                WHEN reqs.upper_lower_rej LIKE '%U%'
                    THEN (
                        TRIM(courses.full_crse) NOT LIKE '%3__'
                        OR TRIM(courses.full_crse) NOT LIKE '%4__'
                        OR TRIM(courses.full_crse) NOT LIKE '%5__'
                        OR TRIM(courses.full_crse) NOT LIKE '%6__'
                        OR TRIM(courses.full_crse) NOT LIKE '%7__'
                    )
                WHEN reqs.upper_lower_rej LIKE '%L%'
                    THEN (
                        TRIM(courses.full_crse) NOT LIKE '%1__'
                        OR TRIM(courses.full_crse) NOT LIKE '%2__'
                    )
                ELSE 1=1
            END
        )
		--check to make sure reject and accept criteria are met
    INNER JOIN ac_criteria_met AS acm
	    ON COALESCE(reqs.ac_all,'') = COALESCE(acm.ac_all,'')
        AND COALESCE(reqs.rc_ord,'') = COALESCE(acm.rc_ord,'')
        AND COALESCE(reqs.rc_and,'') = COALESCE(acm.rc_and,'')
        AND COALESCE(courses.ac_cw,'') = COALESCE(acm.ac_cw,'')
        AND acm.ac_match = 1
        AND acm.rc_ord_match = 1
        AND acm.rc_and_match = 1
) AS foo
--remove duplicates
WHERE row_num = 1;