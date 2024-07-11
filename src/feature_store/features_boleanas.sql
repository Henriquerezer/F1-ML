SELECT 
    ft.Season, 
    ft.Round,
    ft.CircuitID,
    circuit.country,
    ft.ConstructorName, 
    const.constructorId, 
    ft.ConstructorNationality,
    ft.DriverID, 
    drivers.GivenName AS DriverName, 
    drivers.nationality AS DriverNationality, 
    ft.Nationality, 
    ft.Position, 
    ft.Points, 
    ft.Grid, 
    ft.Status, 
    ft.AverageSpeed,
    -- Diferença entre posição de largada e posição de chegada
    ft.Grid - ft.Position AS DifferenceGridPosition,
    -- Piloto venceu as últimas 1/2/3/4/5/6 corridas
    CASE WHEN ft.Position = 1 AND (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) > 0 THEN 1 ELSE 0 END AS WonLastRace,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 2 THEN 1 ELSE 0 END AS WonLast2Races,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 3 THEN 1 ELSE 0 END AS WonLast3Races,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 4 THEN 1 ELSE 0 END AS WonLast4Races,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 5 THEN 1 ELSE 0 END AS WonLast5Races,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 6 THEN 1 ELSE 0 END AS WonLast6Races,
    -- Piloto tem vitória nas últimas 5/10 corridas
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 7 THEN 1 ELSE 0 END AS WonLast7Races,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position = 1
        ) >= 10 THEN 1 ELSE 0 END AS WonLast10Races,
    -- Piloto tem top 3/5 nas últimas 5/10 corridas
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position <= 3
        ) >= 5 THEN 1 ELSE 0 END AS Top3Last5Races,
    CASE WHEN (
            SELECT COUNT(*)
            FROM results AS r
            WHERE r.DriverID = ft.DriverID AND r.Season = ft.Season AND r.Round <= ft.Round AND r.Position <= 5
        ) >= 10 THEN 1 ELSE 0 END AS Top5Last10Races

FROM 
    results AS ft
LEFT JOIN 
    constructors AS const 
ON 
    ft.ConstructorName = const.Name
LEFT JOIN
    circuits AS circuit
ON
    ft.CircuitID = circuit.circuitId
LEFT JOIN
    drivers 
ON
    ft.DriverID = drivers.driverId
GROUP BY 
    ft.Season, 
    ft.Round,
    ft.CircuitID,
    circuit.country,
    ft.ConstructorName, 
    const.constructorId, 
    ft.ConstructorNationality,
    ft.DriverID, 
    drivers.GivenName, 
    drivers.nationality,
    ft.Nationality, 
    ft.Position, 
    ft.Points, 
    ft.Grid, 
    ft.Status, 
    ft.AverageSpeed
