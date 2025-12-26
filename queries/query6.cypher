MATCH (d:staged_interventions)-[r:STUDIED_IN]->(t:staged_trials)
RETURN COUNT(DISTINCT t) AS trials_total,
       COUNT(DISTINCT CASE WHEN r.route IS NOT NULL AND r.route <> 'unknown' THEN t END) AS trials_with_route,
       COUNT(DISTINCT CASE WHEN r.dosage_form IS NOT NULL AND r.dosage_form <> 'unknown' THEN t END) AS trials_with_dosage_form;
