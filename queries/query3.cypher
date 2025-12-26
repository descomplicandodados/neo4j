MATCH (d:staged_interventions)-[:STUDIED_IN]->(t:staged_trials)
RETURN d.name AS drug, COUNT(t) AS num_trials
ORDER BY num_trials DESC
LIMIT 10;