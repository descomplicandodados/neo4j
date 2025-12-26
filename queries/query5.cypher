
MATCH (cond:Silver_conditions {name: 'diabetes'})<-[:STUDIES_CONDITION]-(t:Silver_trials)
OPTIONAL MATCH (d:Silver_interventions)-[:TESTED_IN]->(t)
OPTIONAL MATCH (t)-[:IN_PHASE]->(p:Phase)
RETURN cond.name AS condition, 
       COLLECT(DISTINCT d.name) AS drugs, 
       COLLECT(DISTINCT p.name) AS phases;
