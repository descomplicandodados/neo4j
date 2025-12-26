MATCH (p:Phase)
CALL {
  WITH p
  MATCH (o:Silver_sponsors)-[:SPONSORS_PHASE]->(p)
  RETURN o
  LIMIT 6
}
RETURN o, p;
