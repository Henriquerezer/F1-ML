SELECT DISTINCT(Season), COUNT(DISTINCT(Round))

FROM results

GROUP BY Season

ORDER BY Season ASC