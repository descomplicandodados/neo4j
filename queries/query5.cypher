
MATCH (cond:staged_conditions {name: 'diabetes'})<-[:STUDIES_CONDITION]-(t:staged_trials)
OPTIONAL MATCH (d:staged_interventions)-[:STUDIED_IN]->(t)
OPTIONAL MATCH (t)-[:IN_PHASE]->(p:Phase)
RETURN cond.name AS condition, 
       COLLECT(DISTINCT d.name) AS drugs, 
       COLLECT(DISTINCT p.name) AS phases;
