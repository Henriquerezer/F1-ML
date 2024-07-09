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
    -- Total de vitórias por circuito
    (SELECT COUNT(*) 
     FROM results AS r1
     WHERE r1.CircuitID = ft.CircuitID AND r1.Position = 1) AS TotalWinsPerCircuit,
    -- Nacionalidade do piloto é a mesma no circuito?
    CASE 
        WHEN drivers.nationality = circuit.country THEN 1 
        ELSE 0 
    END AS IsDriverNationalitySameAsCircuit,
    -- Média de posição por circuito
    (SELECT AVG(r2.Position)
     FROM results AS r2
     WHERE r2.CircuitID = ft.CircuitID) AS AvgPositionPerCircuit,
    -- Nacionalidade do construtor é a mesma no circuito?
    CASE 
        WHEN const.nationality = circuit.country THEN 1 
        ELSE 0 
    END AS IsConstructorNationalitySameAsCircuit,
    -- Média de posições ganhas/perdidas na carreira
    (SELECT AVG(r3.Position - r3.Grid)
     FROM results AS r3
     WHERE r3.DriverID = ft.DriverID) AS AvgPositionsGainedLostCareer,
    -- Média de posições ganhas/perdidas por circuito
    (SELECT AVG(r4.Position - r4.Grid)
     FROM results AS r4
     WHERE r4.DriverID = ft.DriverID AND r4.CircuitID = ft.CircuitID) AS AvgPositionsGainedLostPerCircuit,
    -- Piloto venceu as últimas 1/2/3/4/5/6 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r5
              WHERE r5.DriverID = ft.DriverID AND r5.Position = 1
              AND r5.Round <= ft.Round AND r5.Round > ft.Round - 6) > 0 THEN 1
        ELSE 0
    END AS WinsLast6Races,
    -- Piloto tem vitória nas últimas 5 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r6
              WHERE r6.DriverID = ft.DriverID AND r6.Position = 1
              AND r6.Round <= ft.Round AND r6.Round > ft.Round - 5) > 0 THEN 1
        ELSE 0
    END AS WinsLast5Races,
    -- Piloto tem vitória nas últimas 10 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r6
              WHERE r6.DriverID = ft.DriverID AND r6.Position = 1
              AND r6.Round <= ft.Round AND r6.Round > ft.Round - 10) > 0 THEN 1
        ELSE 0
    END AS WinsLast10Races,
    -- Piloto tem top 3 nas últimas 5 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r7
              WHERE r7.DriverID = ft.DriverID AND r7.Position <= 3
              AND r7.Round <= ft.Round AND r7.Round > ft.Round - 5) > 0 THEN 1
        ELSE 0
    END AS Top3Last5Races,
    -- Piloto tem top 3 nas últimas 10 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r7
              WHERE r7.DriverID = ft.DriverID AND r7.Position <= 3
              AND r7.Round <= ft.Round AND r7.Round > ft.Round - 10) > 0 THEN 1
        ELSE 0
    END AS Top3Last10Races,
    -- Piloto tem top 5 nas últimas 5 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r8
              WHERE r8.DriverID = ft.DriverID AND r8.Position <= 5
              AND r8.Round <= ft.Round AND r8.Round > ft.Round - 5) > 0 THEN 1
        ELSE 0
    END AS Top5Last5Races,
    -- Piloto tem top 5 nas últimas 10 corridas
    CASE 
        WHEN (SELECT COUNT(*)
              FROM results AS r8
              WHERE r8.DriverID = ft.DriverID AND r8.Position <= 5
              AND r8.Round <= ft.Round AND r8.Round > ft.Round - 10) > 0 THEN 1
        ELSE 0
    END AS Top5Last10Races
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