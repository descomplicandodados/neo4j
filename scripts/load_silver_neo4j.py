import os
from neo4j import GraphDatabase

# ==========================================================
# Função Airflow
# ==========================================================
def load_silver():

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    REPROCESS_EXISTING = os.getenv("REPROCESS_EXISTING", "false").lower() == "true"

    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        raise RuntimeError("❌ Neo4j env vars ausentes")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    with driver.session() as session:

        print("=" * 80)
        print("🚀 INICIANDO TRANSFORMAÇÃO staged (GRAPH MODEL CANÔNICO)")
        print("=" * 80)

        # ==================================================
        # CONSTRAINTS
        # ==================================================
        print("🔐 Criando constraints")

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (t:staged_trials)
        REQUIRE t.nct_id IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (d:staged_interventions)
        REQUIRE d.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (c:staged_conditions)
        REQUIRE c.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (o:staged_sponsors)
        REQUIRE o.name IS UNIQUE
        """)

        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (p:Phase)
        REQUIRE p.name IS UNIQUE
        """)

        # NOVOS NÓS: Route e DosageForm
        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (r:Route)
        REQUIRE r.name IS UNIQUE
        """)
        session.run("""
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (df:DosageForm)
        REQUIRE df.name IS UNIQUE
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
        print("\n🧪 Criando nós staged_trials")

        merge_clause = "MERGE (t:staged_trials {nct_id: b.nct_id})" if not REPROCESS_EXISTING else "MERGE (t:staged_trials {nct_id: b.nct_id}) SET t = {}"

        result = session.run(f"""
        MATCH (b:Bronze_studies)
        WHERE b.nct_id IS NOT NULL
          AND b.phase IS NOT NULL
          AND trim(b.phase) <> ''
          AND toUpper(b.phase) CONTAINS 'PHASE'

        {merge_clause}
        SET
          t.brief_title     = b.brief_title,
          t.official_title  = b.official_title,
          t.raw_phase       = toUpper(trim(b.phase)),
          t.study_type      = b.study_type,
          t.status          = b.overall_status,
          t.start_date      = b.start_date,
          t.completion_date = b.completion_date,
          t.__layer         = 'staged',
          t.__loaded_at     = datetime()

        RETURN count(t) AS total
        """).single()

        print(f"   🧪 Trials criados/atualizados: {result['total']}")

        # ==================================================
        # TRIAL → PHASE (CORREÇÃO DEFINITIVA)
        # ==================================================
        print("\n🧩 Criando relação staged_trials → Phase")

        session.run("""
        MATCH (t:staged_trials)
        WHERE t.raw_phase IS NOT NULL

        WITH t, replace(t.raw_phase, '/', ',') AS phases
        UNWIND split(phases, ',') AS phase_raw
        WITH t, trim(phase_raw) AS phase_clean

        WITH t,
             CASE
               WHEN phase_clean = 'EARLY_PHASE1' THEN 'PHASE 1'     
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
        MATCH (t:staged_trials {nct_id: b.nct_id})
        WHERE b.nct_id IS NOT NULL
          AND b.downcase_name IS NOT NULL
          AND trim(b.downcase_name) <> ''

        MERGE (c:staged_conditions {
          name: toLower(trim(b.downcase_name))
        })
        SET c.display_name = b.name

        MERGE (t)-[:STUDIES_CONDITION]->(c)

        RETURN count(*) AS total
        """).single()

        print(f"   🧬 Conditions vinculadas: {result['total']}")

        # ==================================================
        # INTERVENTIONS → TRIAL + Route/DosageForm
        # ==================================================
        print("\n💊 Criando Interventions, STUDIED_IN e Route/DosageForm")

        result = session.run("""
        MATCH (b:Bronze_interventions)
        MATCH (t:staged_trials {nct_id: b.nct_id})
        WHERE b.nct_id IS NOT NULL
          AND b.name IS NOT NULL
          AND trim(b.name) <> ''

        MERGE (d:staged_interventions { name: toLower(trim(b.name)) })

        // Relacionamento STUDIED_IN com propriedades
        MERGE (d)-[r:STUDIED_IN]->(t)
        SET
          r.route        = coalesce(toLower(trim(b.route)), 'unknown'),
          r.dosage_form  = coalesce(toLower(trim(b.dosage_form)), 'unknown'),
          r.__loaded_at  = datetime()

        // Criar nós Route e DosageForm
        MERGE (route:Route { name: coalesce(toLower(trim(b.route)), 'unknown') })
        MERGE (df:DosageForm { name: coalesce(toLower(trim(b.dosage_form)), 'unknown') })

        // Relacionar Drug com Route e DosageForm
        MERGE (d)-[:USED_ROUTE]->(route)
        MERGE (d)-[:HAS_DOSAGE_FORM]->(df)

        RETURN count(r) AS total
        """).single()

        print(f"   💊 Interventions vinculadas a Trials e Route/DosageForm: {result['total']}")

        # ==================================================
        # DERIVED: INTERVENTION → PHASE
        # ==================================================
        print("\n⭐ Criando relação DERIVADA STUDIED_IN_PHASE")

        result = session.run("""
        MATCH (d:staged_interventions)-[:STUDIED_IN]->(t:staged_trials)-[:IN_PHASE]->(p:Phase)
        MERGE (d)-[:STUDIED_IN_PHASE]->(p)
        RETURN count(*) AS total
        """).single()

        print(f"   ⭐ Relações Drug → Phase criadas: {result['total']}")

        # ==================================================
        # SPONSORS / COLLABORATORS
        # ==================================================
        print("\n🏢 Criando Sponsors e relacionamentos")

        result = session.run("""
        MATCH (b:Bronze_sponsors)
        MATCH (t:staged_trials {nct_id: b.nct_id})
        WHERE b.nct_id IS NOT NULL
          AND b.name IS NOT NULL
          AND trim(b.name) <> ''

        MERGE (o:staged_sponsors {
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

        # ==================================================
        # DERIVED: SPONSORS → PHASE
        # ==================================================
        print("\n🏢 Criando relação derivada SPONSORS_PHASE")

        result = session.run("""
        MATCH (o:staged_sponsors)<-[:SPONSORED_BY]-(t:staged_trials)-[:IN_PHASE]->(p:Phase)
        MERGE (o)-[r:SPONSORS_PHASE]->(p)
        ON CREATE SET
          r.__derived_from = 'staged_trials',
          r.__created_at  = datetime()
        RETURN count(r) AS total
        """).single()

        print(f"   🏢 Relações Sponsors → Phase criadas: {result['total']}")

        print("\n" + "=" * 80)
        print("✅ TRANSFORMAÇÃO staged FINALIZADA COM SUCESSO")
        print("=" * 80)

    driver.close()
