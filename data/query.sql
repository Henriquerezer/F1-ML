SELECT COUNT(*) 
     FROM results AS r1
     WHERE r1.CircuitID = ft.CircuitID AND r1.Position = 1 
    CASE 
        WHEN drivers.nationality = circuit.country THEN 1 
        ELSE 0 