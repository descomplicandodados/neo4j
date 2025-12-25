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

            # ==================================================
            # 1) STUDY (clinical-stage only)
            # ==================================================
            print("🔬 Criando nós :Study")

            session.run("""
                MATCH (b:Bronze_studies)
                WHERE b.phase IS NOT NULL
                  AND trim(b.phase) <> ''
                  AND toUpper(trim(b.phase)) <> 'NA'
                  AND toUpper(trim(b.phase)) STARTS WITH 'PHASE'

                MERGE (s:Study {nct_id: b.nct_id})
                SET
                    s.brief_title     = b.brief_title,
                    s.official_title  = b.official_title,
                    s.phase           = toUpper(trim(b.phase)),
                    s.study_type      = b.study_type,
                    s.overall_status  = b.overall_status,
                    s.start_date      = b.start_date,
                    s.completion_date = b.completion_date,
                    s.__layer         = 'silver',
                    s.__loaded_at     = datetime()
                RETURN count(s)
            """).single()

            # ==================================================
            # 2) CONDITIONS
            # ==================================================
            print("🧬 Criando :Condition e relacionamentos")

            session.run("""
                MATCH (b:Bronze_conditions)
                WHERE b.nct_id IS NOT NULL
                  AND b.downcase_name IS NOT NULL

                MATCH (s:Study {nct_id: b.nct_id})

                MERGE (c:Condition {
                    name: toLower(trim(b.downcase_name))
                })
                SET c.display_name = b.name

                MERGE (s)-[:HAS_CONDITION]->(c)
                RETURN count(*)
            """).single()

            # ==================================================
            # 3) DRUGS / INTERVENTIONS
            # ==================================================
            print("💊 Criando :Drug e relacionamentos")

            session.run("""
                MATCH (b:Bronze_interventions)
                WHERE b.nct_id IS NOT NULL
                  AND b.name IS NOT NULL
                  AND b.intervention_type IS NOT NULL

                MATCH (s:Study {nct_id: b.nct_id})

                MERGE (d:Drug {
                    name: toLower(trim(b.name)),
                    type: trim(b.intervention_type)
                })

                MERGE (s)-[:STUDIED_IN]->(d)
                RETURN count(*)
            """).single()

            # ==================================================
            # 4) ORGANIZATIONS (SPONSORS / COLLABORATORS)
            # ==================================================
            print("🏢 Criando :Organization e relacionamentos")

            session.run("""
                MATCH (b:Bronze_sponsors)
                WHERE b.nct_id IS NOT NULL
                  AND b.name IS NOT NULL

                MATCH (s:Study {nct_id: b.nct_id})

                MERGE (o:Organization {
                    name: toLower(trim(b.name))
                })
                SET o.agency_class = b.agency_class

                MERGE (s)-[:SPONSORED_BY {
                    role: b.lead_or_collaborator
                }]->(o)

                RETURN count(*)
            """).single()

            # ==================================================
            # 5) COUNTRIES
            # ==================================================
            print("🌍 Criando :Country e relacionamentos")

            session.run("""
                MATCH (b:Bronze_countries)
                WHERE b.nct_id IS NOT NULL
                  AND b.name IS NOT NULL

                MATCH (s:Study {nct_id: b.nct_id})

                MERGE (c:Country {
                    name: trim(b.name)
                })

                MERGE (s)-[:CONDUCTED_IN]->(c)
                RETURN count(*)
            """).single()

            print("✅ Transformação SILVER concluída com sucesso")

    finally:
        driver.close()
