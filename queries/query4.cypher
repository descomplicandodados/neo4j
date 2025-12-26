
MATCH (c:Silver_sponsors {name: 'astrazeneca'})<-[:SPONSORED_BY]-(t:Silver_trials)
OPTIONAL MATCH (t)<-[:TESTED_IN]-(d:Silver_interventions)
OPTIONAL MATCH (t)-[:STUDIES_CONDITION]->(cond:Silver_conditions)
RETURN c.name AS company, 
       COLLECT(DISTINCT d.name) AS drugs, 
       COLLECT(DISTINCT cond.name) AS conditions;
