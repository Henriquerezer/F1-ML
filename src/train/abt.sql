SELECT  r.*,
        CASE WHEN r.Position = 1 AND (
            SELECT COUNT(*)
            FROM results_history AS r1
            WHERE r1.DriverID = r.DriverID AND r1.Season = r.Season AND r1.Round = r.Round AND r1.Position = 1
        ) > 0 THEN 1 ELSE 0 END AS win

FROM results_history as r

/* Position DEVE SAIR DO DF FINAL