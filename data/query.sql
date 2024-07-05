SELECT  DISTINCT(Round) as Round, max(Season) as Season

FROM results

WHERE Season = 2024

GROUP BY Round

ORDER BY Round desc, Season ASC;