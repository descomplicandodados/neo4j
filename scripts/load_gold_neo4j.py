import os
from neo4j import GraphDatabase

# ==========================================================
# Função chamada pelo Airflow / pipeline
# ==========================================================
def load_gold():

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        raise RuntimeError("❌ Variáveis de ambiente do Neo4j não configuradas")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    try:
        with driver.session() as session:

            print("🚀 INICIANDO TRANSFORMAÇÃO SILVER → GOLD")

            # ==================================================
            # Trial metrics
            # ==================================================
            print("📊 Calculando métricas por Trial")

            session.run("""
                MATCH (t:Silver_trials)
                OPTIONAL MATCH (t)-[:HAS_CONDITION]->(c:Silver_conditions)
                OPTIONAL MATCH (t)<-[:TESTED_IN]-(i:Silver_interventions)
                WITH t,
                     count(DISTINCT c) AS num_conditions,
                     count(DISTINCT i) AS num_interventions
                SET
                    t.num_conditions = num_conditions,
                    t.num_interventions = num_interventions
            """)

            # ==================================================
            # Drug popularity
            # ==================================================
            print("💊 Calculando popularidade de Interventions")

            session.run("""
                MATCH (i:Silver_interventions)-[:TESTED_IN]->(t:Silver_trials)
                WITH i, count(DISTINCT t) AS trial_count
                SET i.trial_count = trial_count
            """)

            # ==================================================
            # Condition coverage
            # ==================================================
            print("🧬 Calculando cobertura de Conditions")

            session.run("""
                MATCH (c:Silver_conditions)<-[:HAS_CONDITION]-(t:Silver_trials)
                WITH c,
                     count(DISTINCT t) AS trial_count,
                     collect(DISTINCT t.phase) AS phases
                SET
                    c.trial_count = trial_count,
                    c.phases = phases
            """)

            # ==================================================
            # Organization involvement
            # ==================================================
            print("🏢 Calculando envolvimento de Organizations")

            session.run("""
                MATCH (o:Silver_sponsors)<-[:SPONSORED_BY]-(t:Silver_trials)
                WITH o, count(DISTINCT t) AS trial_count
                SET o.trial_count = trial_count
            """)

            print("✅ TRANSFORMAÇÃO GOLD FINALIZADA COM SUCESSO")

    finally:
        driver.close()
