import os
from neo4j import GraphDatabase

# ==========================================================
# Função chamada pelo Airflow
# ==========================================================
def load_gold():

    # ------------------------------------------------------
    # Variáveis de ambiente
    # ------------------------------------------------------
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        raise RuntimeError("❌ Variáveis de ambiente do Neo4j não configuradas")

    # ------------------------------------------------------
    # Conexão Neo4j
    # ------------------------------------------------------
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    try:
        with driver.session() as session:

            print("🚀 Iniciando transformação SILVER → GOLD")

            # --------------------------------------------------
            # Study metrics
            # --------------------------------------------------
            print("📊 Calculando métricas por Study")

            session.run("""
                MATCH (s:Study)
                OPTIONAL MATCH (s)-[:HAS_CONDITION]->(c)
                OPTIONAL MATCH (s)-[:STUDIED_IN]->(d)
                WITH s,
                     count(DISTINCT c) AS conditions,
                     count(DISTINCT d) AS drugs
                SET
                    s.num_conditions = conditions,
                    s.num_drugs = drugs
            """)

            # --------------------------------------------------
            # Drug popularity
            # --------------------------------------------------
            print("💊 Calculando popularidade de Drug")

            session.run("""
                MATCH (d:Drug)<-[:STUDIED_IN]-(s:Study)
                WITH d, count(DISTINCT s) AS trials
                SET d.trial_count = trials
            """)

            # --------------------------------------------------
            # Condition coverage
            # --------------------------------------------------
            print("🧬 Calculando cobertura de Condition")

            session.run("""
                MATCH (c:Condition)<-[:HAS_CONDITION]-(s:Study)
                WITH c,
                     count(DISTINCT s) AS trials,
                     collect(DISTINCT s.phase) AS phases
                SET
                    c.trial_count = trials,
                    c.phases = phases
            """)

            # --------------------------------------------------
            # Organization involvement
            # --------------------------------------------------
            print("🏢 Calculando envolvimento de Organization")

            session.run("""
                MATCH (o:Organization)<-[:SPONSORED_BY]-(s:Study)
                WITH o, count(DISTINCT s) AS studies
                SET o.study_count = studies
            """)

            print("✅ Transformação GOLD concluída com sucesso")

    finally:
        driver.close()
