MATCH (p:Phase)
CALL {
  WITH p
  MATCH (d:Silver_interventions)-[:TESTED_IN_PHASE]->(p)
  RETURN d
  LIMIT 6
}
RETURN d, p;
