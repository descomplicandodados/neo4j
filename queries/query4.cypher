
MATCH (c:staged_sponsors {name: 'astrazeneca'})<-[:SPONSORED_BY]-(t:staged_trials)
OPTIONAL MATCH (t)<-[:STUDIED_IN]-(d:staged_interventions)
OPTIONAL MATCH (t)-[:STUDIES_CONDITION]->(cond:staged_conditions)
RETURN c.name AS company, 
       COLLECT(DISTINCT d.name) AS drugs, 
       COLLECT(DISTINCT cond.name) AS conditions;
