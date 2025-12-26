import os
from neo4j import GraphDatabase

# ==========================================================
# Função Airflow
# ==========================================================
def load_silver():

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        raise RuntimeError("❌ Neo4j env vars ausentes")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    with driver.session() as session:

        print("=" * 80)
        print("🚀 INICIANDO TRANSFORMAÇÃO SILVER (GRAPH MODEL CANÔNICO)")
        print("=" * 80)

        # ==================================================
        # CONSTRAINTS
        # ==================================================
        print("🔐 Criando constraints")

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (t:Silver_trials)
        REQUIRE t.nct_id IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (d:Silver_interventions)
        REQUIRE d.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (c:Silver_conditions)
        REQUIRE c.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (o:Silver_sponsors)
        REQUIRE o.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (p:Phase)
        REQUIRE p.name IS UNIQUE
        """)

        print("✅ Constraints criadas")

        # ==================================================
        # PHASE NODES (CANÔNICOS)
        # ==================================================
        print("\n🧩 Criando nós Phase canônicos")

        session.run("""
        UNWIND ['PHASE 1','PHASE 2','PHASE 3','PHASE 4'] AS phase
        MERGE (:Phase {name: phase});
        """)

        # ==================================================
        # TRIALS (clinical-stage only)
        # ==================================================
        print("\n🧪 Criando nós Silver_trials")

        result = session.run("""
        MATCH (b:Bronze_studies)
        WHERE b.nct_id IS NOT NULL
          AND b.phase IS NOT NULL
          AND trim(b.phase) <> ''
          AND toUpper(b.phase) CONTAINS 'PHASE'

        MERGE (t:Silver_trials {nct_id: b.nct_id})
        SET
          t.brief_title     = b.brief_title,
          t.official_title  = b.official_title,
          t.raw_phase       = toUpper(trim(b.phase)),
          t.study_type      = b.study_type,
          t.status          = b.overall_status,
          t.start_date      = b.start_date,
          t.completion_date = b.completion_date,
          t.__layer         = 'silver',
          t.__loaded_at     = datetime()

        RETURN count(t) AS total
        """).single()

        print(f"   🧪 Trials criados/atualizados: {result['total']}")

        # ==================================================
        # TRIAL → PHASE (CORREÇÃO DEFINITIVA)
        # ==================================================
        print("\n🧩 Criando relação Silver_trials → Phase")

        session.run("""
        MATCH (t:Silver_trials)
        WHERE t.raw_phase IS NOT NULL

        WITH t, replace(t.raw_phase, '/', ',') AS phases
        UNWIND split(phases, ',') AS phase_raw
        WITH t, trim(phase_raw) AS phase_clean

        WITH t,
             CASE
               WHEN phase_clean = 'PHASE1' THEN 'PHASE 1'
               WHEN phase_clean = 'PHASE2' THEN 'PHASE 2'
               WHEN phase_clean = 'PHASE3' THEN 'PHASE 3'
               WHEN phase_clean = 'PHASE4' THEN 'PHASE 4'
               ELSE null
             END AS phase

        WHERE phase IS NOT NULL
        MATCH (p:Phase {name: phase})
        MERGE (t)-[:IN_PHASE]->(p)
        """)

        # ==================================================
        # CONDITIONS
        # ==================================================
        print("\n🧬 Criando Conditions e relacionamentos")

        result = session.run("""
        MATCH (b:Bronze_conditions)
        MATCH (t:Silver_trials {nct_id: b.nct_id})
        WHERE b.nct_id IS NOT NULL
          AND b.downcase_name IS NOT NULL
          AND trim(b.downcase_name) <> ''

        MERGE (c:Silver_conditions {
          name: toLower(trim(b.downcase_name))
        })
        SET c.display_name = b.name

        MERGE (t)-[:HAS_CONDITION]->(c)

        RETURN count(*) AS total
        """).single()

        print(f"   🧬 Conditions vinculadas: {result['total']}")

        # ==================================================
        # INTERVENTIONS → TRIAL
        # ==================================================
        print("\n💊 Criando Interventions e relação TESTED_IN")

        result = session.run("""
        MATCH (b:Bronze_interventions)
        MATCH (t:Silver_trials {nct_id: b.nct_id})
        WHERE b.nct_id IS NOT NULL
          AND b.name IS NOT NULL
          AND trim(b.name) <> ''

        MERGE (d:Silver_interventions {
          name: toLower(trim(b.name))
        })

        MERGE (d)-[r:TESTED_IN]->(t)
        SET
          r.route        = coalesce(toLower(trim(b.route)), 'unknown'),
          r.dosage_form  = coalesce(toLower(trim(b.dosage_form)), 'unknown'),
          r.__loaded_at  = datetime()

        RETURN count(r) AS total
        """).single()

        print(f"   💊 Interventions vinculadas a Trials: {result['total']}")

        # ==================================================
        # DERIVED: INTERVENTION → PHASE
        # ==================================================
        print("\n⭐ Criando relação DERIVADA TESTED_IN_PHASE")

        result = session.run("""
        MATCH (d:Silver_interventions)-[:TESTED_IN]->(t:Silver_trials)-[:IN_PHASE]->(p:Phase)
        MERGE (d)-[:TESTED_IN_PHASE]->(p)
        RETURN count(*) AS total
        """).single()

        print(f"   ⭐ Relações Drug → Phase criadas: {result['total']}")

        # ==================================================
        # SPONSORS / COLLABORATORS
        # ==================================================
        print("\n🏢 Criando Sponsors e relacionamentos")

        result = session.run("""
        MATCH (b:Bronze_sponsors)
        MATCH (t:Silver_trials {nct_id: b.nct_id})
        WHERE b.nct_id IS NOT NULL
          AND b.name IS NOT NULL
          AND trim(b.name) <> ''

        MERGE (o:Silver_sponsors {
          name: toLower(trim(b.name))
        })
        SET o.agency_class = b.agency_class

        FOREACH (_ IN CASE WHEN b.lead_or_collaborator = 'lead' THEN [1] ELSE [] END |
          MERGE (t)-[:SPONSORED_BY]->(o)
        )

        FOREACH (_ IN CASE WHEN b.lead_or_collaborator = 'collaborator' THEN [1] ELSE [] END |
          MERGE (t)-[:COLLABORATES_WITH]->(o)
        )

        RETURN count(o) AS total
        """).single()

        print(f"   🏢 Organizations processadas: {result['total']}")

        print("\n" + "=" * 80)
        print("✅ TRANSFORMAÇÃO SILVER FINALIZADA COM SUCESSO")
        print("=" * 80)

    driver.close()
