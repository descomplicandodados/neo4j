MATCH (p:Phase)
CALL {
  WITH p
  MATCH (d:staged_interventions)-[:STUDIED_IN_PHASE]->(p)
  RETURN d
  LIMIT 6
}
RETURN d, p;
