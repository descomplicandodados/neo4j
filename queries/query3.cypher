MATCH (d:Silver_interventions)-[:TESTED_IN]->(t:Silver_trials)
RETURN d.name AS drug, COUNT(t) AS num_trials
ORDER BY num_trials DESC
LIMIT 10;