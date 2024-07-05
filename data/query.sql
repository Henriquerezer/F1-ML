  SELECT DISTINCT Season, Round
    FROM qualifying_results
    ORDER BY Season DESC, Round DESC
    LIMIT 1