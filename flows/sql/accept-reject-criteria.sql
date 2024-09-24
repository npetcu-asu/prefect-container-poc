-- This query splits each accept or reject criteria list to multiple rows, then checks each character if it's found in
-- the requirement designation codes, and then checks if as a group they meet the and/or logic criteria. The result is
-- a table that tells whether or not each requirement designation matches with each possible ac/rc criterion.
WITH RECURSIVE 
	cj AS(
		SELECT DISTINCT * 
		FROM acc_rej_crit
		CROSS JOIN ps_cw
),	ac_exp AS(
		SELECT 
			0 AS n,
			'' AS ac,
			'' AS ac_all,
			'' AS ac_cw
		UNION 
		SELECT
			n + 1,
			SUBSTRING(cj.ac_all,n+1,1),
			cj.ac_all,
			cj.ac_cw
		FROM 
			cj,
			ac_exp
		WHERE n < length(cj.ac_all)
),	rc_ord_exp AS(
		SELECT 
			0 AS n,
			'' AS rc,
			'' AS rc_ord,
			'' AS ac_cw
		UNION 
		SELECT
			n + 1,
			SUBSTRING(cj.rc_ord,n+1,1),
			cj.rc_ord,
			cj.ac_cw
		FROM 
			cj,
			rc_ord_exp
		WHERE n < length(cj.rc_ord)
),	cw_exp AS(
		SELECT 
			0 AS n,
			'' AS cw,
			'' AS rc_and,
			'' AS ac_cw
		UNION 
		SELECT
			n + 1,
			SUBSTRING(cj.ac_cw,n+1,1),
			cj.rc_and,
			cj.ac_cw
		FROM 
			cj,
			cw_exp
		WHERE n < length(cj.ac_cw)
),	ac_match AS (
		SELECT 
			ac_all,
			ac_cw,
			CASE WHEN (SUM(CASE WHEN ac_cw LIKE '%'||ac||'%' THEN 0 ELSE 1 END) < 1) THEN 1 ELSE 0 END AS ac_match
		FROM ac_exp
		GROUP BY ac_all, ac_cw
), 	rc_ord_match AS (
		SELECT 
			rc_ord,
			ac_cw,
			CASE WHEN (SUM(CASE WHEN ((LENGTH(rc_ord) > 0 ) AND (ac_cw LIKE '%'||rc||'%')) THEN 1 ELSE 0 END) < 1) THEN 1 ELSE 0 END AS rc_ord_match
		FROM rc_ord_exp
		GROUP BY rc_ord, ac_cw
), 	rc_and_match AS (
		SELECT 
			rc_and,
			ac_cw,
			CASE WHEN (SUM(CASE WHEN ((LENGTH(rc_and) > 0) AND (rc_and LIKE '%'||cw||'%')) THEN 0 ELSE 1 END) < 1) THEN 0 ELSE 1 END AS rc_and_match
		FROM cw_exp
		GROUP BY rc_and, ac_cw
)
SELECT 
	cj.ac_all, 
    cj.rc_ord,
    cj.rc_and,
	cj.ac_cw, 
	CASE WHEN ac_match IS NULL THEN 1 ELSE ac_match END AS ac_match, 
	CASE WHEN rc_ord_match IS NULL THEN 1 ELSE rc_ord_match END AS rc_ord_match, 
	CASE WHEN rc_and_match IS NULL THEN 1 ELSE rc_and_match END AS rc_and_match
FROM cj 
LEFT JOIN ac_match
	ON cj.ac_all = ac_match.ac_all 
		AND cj.ac_cw = ac_match.ac_cw 
LEFT JOIN rc_ord_match 
	ON cj.rc_ord = rc_ord_match.rc_ord 
		AND cj.ac_cw = rc_ord_match.ac_cw 
LEFT JOIN rc_and_match
	ON cj.rc_and = rc_and_match.rc_and
		AND cj.ac_cw = rc_and_match.ac_cw;