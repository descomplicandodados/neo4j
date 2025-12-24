import os
from neo4j import GraphDatabase

# ==========================================================
# Função chamada pelo Airflow
# ==========================================================
def load_silver():

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

            print("🚀 Iniciando transformação BRONZE → SILVER")

            # --------------------------------------------------
            # 1) STUDY (COM FILTRO CLINICAL-STAGE)
            # --------------------------------------------------
            print("🔬 Criando nós :Study (clinical-stage only)")

            session.run("""
                MATCH (b:Bronze_studies)
                WHERE b.phase =~ 'Phase (1|2|3|4).*'
                MERGE (s:Study {nct_id: b.nct_id})
                SET
                    s.brief_title     = b.brief_title,
                    s.official_title  = b.official_title,
                    s.phase           = b.phase,
                    s.study_type      = b.study_type,
                    s.overall_status  = b.overall_status,
                    s.start_date      = b.start_date,
                    s.completion_date = b.completion_date,
                    s.__layer         = 'silver',
                    s.__loaded_at     = datetime()
            """)

            # --------------------------------------------------
            # 2) CONDITIONS
            # --------------------------------------------------
            print("🧬 Criando :Condition e relacionamentos")

            session.run("""
                MATCH (b:Bronze_conditions)
                MATCH (s:Study {nct_id: b.nct_id})
                MERGE (c:Condition {name: toLower(trim(b.downcase_name))})
                SET c.display_name = b.name
                MERGE (s)-[:HAS_CONDITION]->(c)
            """)

            # --------------------------------------------------
            # 3) DRUGS / INTERVENTIONS
            # --------------------------------------------------
            print("💊 Criando :Drug e relacionamentos")

            session.run("""
                MATCH (b:Bronze_interventions)
                MATCH (s:Study {nct_id: b.nct_id})
                WHERE b.intervention_type IS NOT NULL
                MERGE (d:Drug {
                    name: toLower(trim(b.name)),
                    type: b.intervention_type
                })
                MERGE (s)-[:STUDIED_IN]->(d)
            """)

            # --------------------------------------------------
            # 4) ORGANIZATIONS (SPONSORS / COLLABORATORS)
            # --------------------------------------------------
            print("🏢 Criando :Organization e relacionamentos")

            session.run("""
                MATCH (b:Bronze_sponsors)
                MATCH (s:Study {nct_id: b.nct_id})
                MERGE (o:Organization {name: toLower(trim(b.name))})
                SET o.agency_class = b.agency_class
                MERGE (s)-[:SPONSORED_BY {
                    role: b.lead_or_collaborator
                }]->(o)
            """)

            # --------------------------------------------------
            # 5) COUNTRIES
            # --------------------------------------------------
            print("🌍 Criando :Country e relacionamentos")

            session.run("""
                MATCH (b:Bronze_countries)
                MATCH (s:Study {nct_id: b.nct_id})
                MERGE (c:Country {name: b.name})
                MERGE (s)-[:CONDUCTED_IN]->(c)
            """)

            print("✅ Transformação SILVER concluída com sucesso")

    finally:
        driver.close()
