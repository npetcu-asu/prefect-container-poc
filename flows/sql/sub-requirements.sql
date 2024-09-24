-- Retrieves sub-requirements and the matching courses and criteria.
--
-- Note that all LIKE conditions have doubled percent characters where they occur.
-- This is because when using a parameterized query, the psycopg2 library will treat
-- all percent signs as the start of a parameter placeholder.  A double percent escapes
-- the percent to psycopg2, allowing for the use of the percent in the LIKE condition.
--
-- Also note that psycopg2 will consider percent characters that occur in SQL comments
-- as placeholders too.
SELECT
	rname,
	subreq_id,
	rqfyt,
	lyt,
	rtitle1,
	seq1,
	seq2,
	seq3,
	seq4,
	course,
	tflg,
	ctitle,
	college,
	category,
	ar_type,
	rqtype,
	rqfyt2,
	lyt2,
	CASE
		WHEN (matchctl_coa IS NULL)
		OR (TRIM(matchctl_coa) = '!') THEN ''
		ELSE matchctl_coa
	END AS matchctl,
	grp,
	grpmin,
	grpmax,
	hcmin,
	hcmax,
	r_ac1,
	r_ac2,
	r_rc1,
	r_rc2,
	ac,
	rc,
	ac1,
	ac2,
	ac3,
	ac4,
	ac5,
	acor,
	rc1,
	rc2,
	rc3,
	rc4,
	rc5,
	rcand
FROM (
	SELECT
		TRIM(sr.rname) AS rname,
		sra.sub_int_seq_no::TEXT AS subreq_id,
		TRIM(sr.instidq) AS instidq,
		TRIM(sr.instid) AS instid,
		TRIM(sr.instcd) AS instcd,
		TRIM(sr.rqfyt) AS rqfyt,
		TRIM(rm.lyt) AS lyt,
		TRIM(lists.rqfyt) AS rqfyt2,
		TRIM(lists.lyt) AS lyt2,
		rm.rtitle1,
		sr.user_seq_no AS seq1,
		--the subreq numberin UAchieve
		sra.user_seq_no AS seq2,
		--the ORDER OF the courses in UAchieve, the RIGHT way to sort the table
    COALESCE(lists.seq1, 1) AS seq3,
		--For sorting lists after they are joined in as a course
    COALESCE(lists.seq2, 1) AS seq4,
		--For sorting lists after they are joined in as a course
		CASE
			WHEN TRIM(lists.rqtype) = 'L' THEN COALESCE(TRIM(lists.course), '')
			ELSE COALESCE(TRIM(sra.course), '')
		END AS course,
		COALESCE(TRIM(sra.tflg), lists.tflg) AS tflg,
		--This indicates if the specific course title is part of the requirement (like for specific 494 classes)
    COALESCE(TRIM(sra.ctitle), lists.ctitle) AS ctitle,
		--course title matching code
    COALESCE(TRIM(sra.crsflag), '') AS crsflag,
		dprog.college,
		dprog.category,
		sra.ar_type,
		-- "R" means filter this out, it matches the requirement but they don't want it used;
		CASE
			WHEN TRIM(lists.rqtype) = 'L' THEN COALESCE(TRIM(lists.matchctl), '')
			ELSE COALESCE(TRIM(sra.matchctl), '')
		END AS matchctl_coa,
		-- NOT IN ('$','S','P','!')   -- An '&' symbol makes two courses into one; an '/' IS OR, means you can take one FROM the GROUP but NOT multiple
		--Any pseudo course with ar_type R is out. Anything after a pseudo course that has ar_type A is also out (within the same sr.user_seq_no sorted by sra.user_seq_no)
		rm.rqtype,
		sr.grp,
		--Value of "H" means selections are measured by hours, "C" means measured by courses
		sr.grpmin,
		--The minimum number of groups in the sub-requirement that must be satisfied
		sr.grpmax,
		--The maximum number of groups in the sub-requirement that must be satisfied
		sr.hcmin,
		--The minimum number of Hours or Courses that must be completed in each group to satisfy it
		sr.hcmax,
		--The maximum number of Hours or Courses that must be completed in each group to satisfy it
		rm.ac1 AS r_ac1,
		rm.ac2 AS r_ac2,
		rm.rc1 AS r_rc1,
		rm.rc2 AS r_rc2,
		sra.ac,
		sra.rc,
		sr.ac1,
		sr.ac2,
		sr.ac3,
		sr.ac4,
		sr.ac5,
		sr.acor,
		sr.rc1,
		sr.rc2,
		sr.rc3,
		sr.rc4,
		sr.rc5,
		sr.rcand,
		SUM(
			CASE
				WHEN (
					ar_type = 'A'
					AND ( -- This case effectively re-retrieves matchctl_coa for use in the parent CASE
						CASE
							WHEN TRIM(lists.rqtype) = 'L' THEN COALESCE(TRIM(lists.matchctl), '')
							ELSE COALESCE(TRIM(sra.matchctl), '')
						END
					) IN ('$', 'S', 'P')
				) THEN 1
				ELSE 0
			END
		) OVER (
			PARTITION BY
				sr.rname,
				sr.rqfyt,
				lists.rname,
				lists.rqfyt,
				sr.user_seq_no
			ORDER BY
				sra.user_seq_no::INT,
				lists.seq1,
				lists.seq2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS cutoff,
		--flag all hidden courses
    LAG(sra.matchctl, 1) OVER (
			--this will be used to filter out hidden or's. Everything after "!" should be hidden
			PARTITION BY
				sr.rname,
				sr.rqfyt,
				lists.rname,
				lists.rqfyt,
				sr.user_seq_no
			ORDER BY
				sra.user_seq_no::INT,
				lists.seq1,
				lists.seq2
		) AS lag_m
	FROM
		asu_dars_uachieve.req_main rm
	INNER JOIN asu_dars_uachieve.sub_req sr
		ON rm.instidq = sr.instidq
		AND rm.instid = sr.instid
		AND rm.instcd = sr.instcd
		AND rm.rname = sr.rname
		AND rm.rqfyt = sr.rqfyt
	LEFT JOIN asu_dars_uachieve.sub_req_ar sra
    ON sr.int_seq_no = sra.sub_int_seq_no
	LEFT JOIN (
		--JOIN IN course lists
		SELECT
			rm.instidq,
			rm.instid,
			rm.instcd,
			rm.rname,
			rm.rqfyt,
			rm.lyt,
			sr.user_seq_no AS seq1,
			sra.user_seq_no AS seq2,
			course,
			TRIM(sra.tflg) AS tflg,
			TRIM(sra.ctitle) AS ctitle,
			rqtype,
			sra.matchctl
		FROM asu_dars_uachieve.sub_req sr
		LEFT JOIN asu_dars_uachieve.sub_req_ar sra
			ON sr.int_seq_no = sra.sub_int_seq_no
		INNER JOIN asu_dars_uachieve.req_main rm
			ON rm.instidq = sr.instidq
			AND rm.instid = sr.instid
			AND rm.instcd = sr.instcd
			AND rm.rname = sr.rname
			AND rm.rqfyt = sr.rqfyt
			AND rm.instcd = 'ASU'
			AND TRIM(rm.rqtype) = 'L'
	) AS lists
    ON TRIM(lists.instidq) = TRIM(rm.instidq)
		AND TRIM(lists.instid) = TRIM(rm.instid)
		AND TRIM(lists.instcd) = TRIM(rm.instcd)
		AND TRIM(lists.rname) = TRIM(sra.course)
		AND (((TRIM(lists.rqfyt) >= TRIM(rm.rqfyt))
			AND (TRIM(lists.rqfyt) <= TRIM(rm.lyt)))
			OR ((TRIM(lists.lyt) >= TRIM(rm.rqfyt))
				AND (TRIM(lists.lyt) <= TRIM(rm.lyt)))
				OR ((TRIM(rm.rqfyt) >= TRIM(lists.rqfyt))
					AND (TRIM(rm.rqfyt) <= TRIM(lists.lyt)))
					OR ((TRIM(rm.lyt) >= TRIM(lists.rqfyt))
						AND (TRIM(rm.lyt) <= TRIM(lists.lyt))))
		AND TRIM(sra.matchctl) = 'L'
	LEFT JOIN (
		--This will make sure TAG AND MAPP requirements are not included
    SELECT
        TRIM(dr.instidq) AS instidq,
				TRIM(dr.instid) AS instid,
				TRIM(dr.instcd) AS instcd,
				TRIM(dr.dpfyt) AS dpfyt,
				TRIM(dr.dprname) AS dprname,
				dr.category,
				d.college,
				ROW_NUMBER() OVER (
					PARTITION BY TRIM(dr.dprname), TRIM(dr.dpfyt)
					ORDER BY dr.int_seq_no DESC
				) AS row_num
    FROM asu_dars_uachieve.dprog_req dr
    INNER JOIN asu_dars_uachieve.dprog d
        ON d.instidq = dr.instidq
				AND d.instid = dr.instid
				AND d.instcd = dr.instcd
				AND d.dprog = dr.dprog
				AND d.dpfyt = dr.dpfyt
    WHERE
			TRIM(d.instcd) = 'ASU'
			AND TRIM(category) IN ('A','C','F','G','L','M','N','P','Q','R','S','X','e','f','t','y','z')
	) AS dprog
    ON TRIM(sr.instidq) = dprog.instidq
		AND TRIM(sr.instid) = dprog.instid
		AND TRIM(sr.instcd) = dprog.instcd
		AND TRIM(sr.rname) = dprog.dprname
		AND TRIM(sr.rqfyt) = TRIM(dprog.dpfyt)
		AND row_num = 1
	WHERE
		-- Named parameter rqfyt
		TRIM(rm.rqfyt) = %(rqfyt)s
		-- Named parameter lyt
		AND TRIM(rm.lyt) = %(lyt)s
		AND rm.instcd = 'ASU'
		--ALWAYS USE THIS FILTER!!!,
		AND ((UPPER(college) NOT LIKE '%%MAPP%%')
			AND (UPPER(college) NOT LIKE '%%TAG%%')
				OR college IS NULL)
) AS foo
WHERE
	TRIM(ar_type) != 'R'
	AND cutoff = 0
	AND COALESCE(matchctl_coa, '') NOT IN ('$', 'S', 'P')
	AND (TRIM(crsflag) = ''
		OR (crsflag IS NULL))
	--remove hidden or's
	AND COALESCE(lag_m, '') NOT LIKE '%%!%%'
	AND (
			-- Case where course is a subject and catalog number (MAT 117)
			-- or subject and wildcard (MAT ***)
			(
				course LIKE '___ ___'
				AND (
					course ~ '[0-9]{3}$'
					OR course LIKE '%%*%%'
				)
			)
			-- Case where course is just a subject (MAT).
			-- Implies a wildcard for the subject.
			OR course ~ '^[A-Za-z]{3}$'
			OR TRIM(matchctl_coa) IN ('L', 'N')
	);