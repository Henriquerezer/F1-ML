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
    -- Calculando a quantidade de vitórias por ano para o construtor
    (SELECT COUNT(*) 
     FROM results AS r
     WHERE r.Season = ft.Season AND r.ConstructorName = ft.ConstructorName AND r.Position = 1) AS ConstructorWinsPerYear,
    -- Calculando a média de vitórias por temporada para o construtor
    (SELECT AVG(cw.YearWins) 
     FROM (
        SELECT COUNT(*) AS YearWins
        FROM results AS r2
        WHERE r2.ConstructorName = ft.ConstructorName AND r2.Position = 1
        GROUP BY r2.Season
     ) AS cw) AS ConstructorAvgWinsPerSeason,
    -- Calculando o total de vitórias históricas para o construtor
    (SELECT COUNT(*) 
     FROM results AS r3
     WHERE r3.ConstructorName = ft.ConstructorName AND r3.Position = 1) AS ConstructorTotalWins,
    -- Calculando a quantidade de vitórias por circuito para o construtor
    (SELECT COUNT(*) 
     FROM results AS r4
     WHERE r4.CircuitID = ft.CircuitID AND r4.ConstructorName = ft.ConstructorName AND r4.Position = 1) AS ConstructorWinsPerCircuit,
    -- Calculando a quantidade de vitórias por ano para o piloto
    (SELECT COUNT(*) 
     FROM results AS r5
     WHERE r5.Season = ft.Season AND r5.DriverID = ft.DriverID AND r5.Position = 1) AS DriverWinsPerYear,
    -- Calculando a média de vitórias por temporada para o piloto
    (SELECT AVG(dw.YearWins) 
     FROM (
        SELECT COUNT(*) AS YearWins
        FROM results AS r6
        WHERE r6.DriverID = ft.DriverID AND r6.Position = 1
        GROUP BY r6.Season
     ) AS dw) AS DriverAvgWinsPerSeason,
    -- Calculando o total de vitórias na carreira para o piloto
    (SELECT COUNT(*) 
     FROM results AS r7
     WHERE r7.DriverID = ft.DriverID AND r7.Position = 1) AS DriverTotalWins
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
