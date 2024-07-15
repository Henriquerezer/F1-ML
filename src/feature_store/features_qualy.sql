WITH driver_statistics AS (
  SELECT
    DriverID,
    CircuitID,
    AVG(CAST(SUBSTR(Q1, 1, INSTR(Q1, ':') - 1) * 60 + SUBSTR(Q1, INSTR(Q1, ':') + 1) AS DECIMAL(10, 3))) AS avg_Q1_time,
    AVG(CAST(SUBSTR(Q2, 1, INSTR(Q2, ':') - 1) * 60 + SUBSTR(Q2, INSTR(Q2, ':') + 1) AS DECIMAL(10, 3))) AS avg_Q2_time,
    AVG(CAST(SUBSTR(Q3, 1, INSTR(Q3, ':') - 1) * 60 + SUBSTR(Q3, INSTR(Q3, ':') + 1) AS DECIMAL(10, 3))) AS avg_Q3_time,
    AVG(Position) AS avg_position,
    COUNT(*) AS total_races
  FROM qualifying_results
  WHERE Q1 != '0' AND Q2 != '0' AND Q3 != '0'
  GROUP BY DriverID, CircuitID
),
constructor_statistics AS (
  SELECT
    ConstructorID,
    AVG(Position) AS avg_constructor_position
  FROM qualifying_results
  GROUP BY ConstructorID
),
driver_recent_positions AS (
  SELECT
    DriverID,
    CircuitID,
    Position,
    ROW_NUMBER() OVER (PARTITION BY DriverID ORDER BY Season DESC, Round DESC) AS race_rank
  FROM qualifying_results
),
driver_top_positions AS (
  SELECT
    d.DriverID,
    d.CircuitID,
    MAX(CASE WHEN d.race_rank = 1 AND d.Position = 1 THEN 1 ELSE 0 END) AS was_first_last_1,
    MAX(CASE WHEN d.race_rank <= 2 AND d.Position = 1 THEN 1 ELSE 0 END) AS was_first_last_2,
    MAX(CASE WHEN d.race_rank <= 3 AND d.Position = 1 THEN 1 ELSE 0 END) AS was_first_last_3,
    MAX(CASE WHEN d.race_rank <= 4 AND d.Position = 1 THEN 1 ELSE 0 END) AS was_first_last_4,
    MAX(CASE WHEN d.race_rank <= 5 AND d.Position = 1 THEN 1 ELSE 0 END) AS was_first_last_5,
    MAX(CASE WHEN d.race_rank <= 1 AND d.Position <= 3 THEN 1 ELSE 0 END) AS was_top3_last_1,
    MAX(CASE WHEN d.race_rank <= 2 AND d.Position <= 3 THEN 1 ELSE 0 END) AS was_top3_last_2,
    MAX(CASE WHEN d.race_rank <= 3 AND d.Position <= 3 THEN 1 ELSE 0 END) AS was_top3_last_3,
    MAX(CASE WHEN d.race_rank <= 4 AND d.Position <= 3 THEN 1 ELSE 0 END) AS was_top3_last_4,
    MAX(CASE WHEN d.race_rank <= 5 AND d.Position <= 3 THEN 1 ELSE 0 END) AS was_top3_last_5,
    MAX(CASE WHEN d.race_rank <= 1 AND d.Position <= 5 THEN 1 ELSE 0 END) AS was_top5_last_1,
    MAX(CASE WHEN d.race_rank <= 2 AND d.Position <= 5 THEN 1 ELSE 0 END) AS was_top5_last_2,
    MAX(CASE WHEN d.race_rank <= 3 AND d.Position <= 5 THEN 1 ELSE 0 END) AS was_top5_last_3,
    MAX(CASE WHEN d.race_rank <= 4 AND d.Position <= 5 THEN 1 ELSE 0 END) AS was_top5_last_4,
    MAX(CASE WHEN d.race_rank <= 5 AND d.Position <= 5 THEN 1 ELSE 0 END) AS was_top5_last_5,
    MAX(CASE WHEN d.race_rank <= 1 AND d.Position <= 8 THEN 1 ELSE 0 END) AS was_top8_last_1,
    MAX(CASE WHEN d.race_rank <= 2 AND d.Position <= 8 THEN 1 ELSE 0 END) AS was_top8_last_2,
    MAX(CASE WHEN d.race_rank <= 3 AND d.Position <= 8 THEN 1 ELSE 0 END) AS was_top8_last_3,
    MAX(CASE WHEN d.race_rank <= 4 AND d.Position <= 8 THEN 1 ELSE 0 END) AS was_top8_last_4,
    MAX(CASE WHEN d.race_rank <= 5 AND d.Position <= 8 THEN 1 ELSE 0 END) AS was_top8_last_5,
    MAX(CASE WHEN d.race_rank <= 1 AND d.Position <= 10 THEN 1 ELSE 0 END) AS was_top10_last_1,
    MAX(CASE WHEN d.race_rank <= 2 AND d.Position <= 10 THEN 1 ELSE 0 END) AS was_top10_last_2,
    MAX(CASE WHEN d.race_rank <= 3 AND d.Position <= 10 THEN 1 ELSE 0 END) AS was_top10_last_3,
    MAX(CASE WHEN d.race_rank <= 4 AND d.Position <= 10 THEN 1 ELSE 0 END) AS was_top10_last_4,
    MAX(CASE WHEN d.race_rank <= 5 AND d.Position <= 10 THEN 1 ELSE 0 END) AS was_top10_last_5,
    MAX(CASE WHEN d.race_rank <= 1 AND d.Position <= 15 THEN 1 ELSE 0 END) AS was_top15_last_1,
    MAX(CASE WHEN d.race_rank <= 2 AND d.Position <= 15 THEN 1 ELSE 0 END) AS was_top15_last_2,
    MAX(CASE WHEN d.race_rank <= 3 AND d.Position <= 15 THEN 1 ELSE 0 END) AS was_top15_last_3,
    MAX(CASE WHEN d.race_rank <= 4 AND d.Position <= 15 THEN 1 ELSE 0 END) AS was_top15_last_4,
    MAX(CASE WHEN d.race_rank <= 5 AND d.Position <= 15 THEN 1 ELSE 0 END) AS was_top15_last_5
  FROM driver_recent_positions d
  GROUP BY d.DriverID, d.CircuitID
)
SELECT
  q.Season,
  q.Round,
  q.CircuitID,
  q.DriverID,
  q.Code,
  q.PermanentNumber,
  q.GivenName,
  q.FamilyName,
  q.DateOfBirth,
  q.Nationality AS driver_nationality,
  q.ConstructorID,
  q.ConstructorName,
  q.ConstructorNationality AS constructor_nationality,
  q.Q1,
  q.Q2,
  q.Q3,
  ds.avg_Q1_time,
  ds.avg_Q2_time,
  ds.avg_Q3_time,
  ds.avg_position AS driver_avg_position,
  ds.total_races AS driver_total_races,
  cs.avg_constructor_position,
  tp.was_first_last_1,
  tp.was_first_last_2,
  tp.was_first_last_3,
  tp.was_first_last_4,
  tp.was_first_last_5,
  tp.was_top3_last_1,
  tp.was_top3_last_2,
  tp.was_top3_last_3,
  tp.was_top3_last_4,
  tp.was_top3_last_5,
  tp.was_top5_last_1,
  tp.was_top5_last_2,
  tp.was_top5_last_3,
  tp.was_top5_last_4,
  tp.was_top5_last_5,
  tp.was_top8_last_1,
  tp.was_top8_last_2,
  tp.was_top8_last_3,
  tp.was_top8_last_4,
  tp.was_top8_last_5,
  tp.was_top10_last_1,
  tp.was_top10_last_2,
  tp.was_top10_last_3,
  tp.was_top10_last_4,
  tp.was_top10_last_5,
  tp.was_top15_last_1,
  tp.was_top15_last_2,
  tp.was_top15_last_3,
  tp.was_top15_last_4,
  tp.was_top15_last_5,
  q.Position
FROM
  qualifying_results q
LEFT JOIN driver_statistics ds ON q.DriverID = ds.DriverID AND q.CircuitID = ds.CircuitID
LEFT JOIN constructor_statistics cs ON q.ConstructorID = cs.ConstructorID
LEFT JOIN driver_top_positions tp ON q.DriverID = tp.DriverID AND q.CircuitID = tp.CircuitID;
