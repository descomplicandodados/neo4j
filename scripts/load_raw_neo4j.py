import os
import shutil
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ==========================================================
# Variáveis de ambiente
# ==========================================================
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

SOURCE_DIR = os.getenv("SOURCE_DIR")
NEO4J_IMPORT_DIR = os.getenv("NEO4J_IMPORT_DIR")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))

REPROCESS_EXISTING = os.getenv("REPROCESS_EXISTING", "false").lower() == "true"

# ==========================================================
# Validação obrigatória
# ==========================================================
required = {
    "NEO4J_URI": NEO4J_URI,
    "NEO4J_USER": NEO4J_USER,
    "NEO4J_PASSWORD": NEO4J_PASSWORD,
    "SOURCE_DIR": SOURCE_DIR,
    "NEO4J_IMPORT_DIR": NEO4J_IMPORT_DIR,
}

missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"❌ Variáveis de ambiente ausentes: {', '.join(missing)}")

# ==========================================================
# Lock para logs thread-safe
# ==========================================================
print_lock = Lock()

def safe_print(*args):
    with print_lock:
        print(*args)

# ==========================================================
# Processamento de um único arquivo
# ==========================================================
def process_file(driver, filename, idx, total_files):

    source_path = os.path.join(SOURCE_DIR, filename)
    target_path = os.path.join(NEO4J_IMPORT_DIR, filename)

    id_field = "nct_id" if filename == "studies.txt" else "id"
    label = f"Bronze_{filename.replace('.txt', '')}"

    if not os.path.exists(source_path):
        safe_print(f"⚠️ Arquivo não encontrado: {source_path}")
        return None

    safe_print(f"\n🔄 [{idx}/{total_files}] Processando arquivo: {filename}")
    safe_print(f"   🔑 Campo de chave: {id_field}")
    safe_print(f"   🏷️ Label Neo4j: {label}")

    try:
        with driver.session() as session:

            # --------------------------------------------------
            # Constraint
            # --------------------------------------------------
            safe_print(f"   🔐 Criando constraint UNIQUE ({label}.{id_field})")
            session.run(f"""
                CREATE CONSTRAINT IF NOT EXISTS
                FOR (n:{label})
                REQUIRE n.{id_field} IS UNIQUE
            """)

            # --------------------------------------------------
            # Contagem ANTES
            # --------------------------------------------------
            before = session.run(
                f"MATCH (n:{label}) RETURN count(n) AS c"
            ).single()["c"]

            shutil.copy(source_path, target_path)

            with open(source_path, "r", encoding="utf-8") as f:
                total_lines = max(sum(1 for _ in f) - 1, 0)

            safe_print(f"   📄 Linhas detectadas: {total_lines:,}")
            safe_print(f"   📥 Copiado para Neo4j import dir")

            is_large_file = total_lines > 100_000
            safe_print(f"   ▶️ Iniciando carga APOC ({filename})")

            # --------------------------------------------------
            # MERGE
            # --------------------------------------------------
            if REPROCESS_EXISTING:
                merge_clause = f"""
                MERGE (n:{label} {{{id_field}: row.{id_field}}})
                ON CREATE SET
                    n += row,
                    n.nct_id = row.nct_id,
                    n.__file = '{filename}',
                    n.__label = '{label}',
                    n.__created_at = datetime()
                ON MATCH SET
                    n += row
                """
            else:
                merge_clause = f"""
                MERGE (n:{label} {{{id_field}: row.{id_field}}})
                ON CREATE SET
                    n += row,
                    n.nct_id = row.nct_id,
                    n.__file = '{filename}',
                    n.__label = '{label}',
                    n.__created_at = datetime()
                """

            cypher = f"""
            CALL apoc.periodic.iterate(
              "
              LOAD CSV WITH HEADERS
              FROM 'file:///{filename}' AS row
              FIELDTERMINATOR '|'
              WITH row
              WHERE row.{id_field} IS NOT NULL
              RETURN row
              ",
              "
              {merge_clause}
              SET n.__loaded_at = datetime()
              ",
              {{
                batchSize: {BATCH_SIZE},
                parallel: {'true' if is_large_file else 'false'}
              }}
            )
            YIELD total, committedOperations, failedOperations, timeTaken
            RETURN *
            """

            record = session.run(cypher).single()

            total = record["total"] or 0
            committed = record["committedOperations"] or 0
            failed = record["failedOperations"] or 0
            time_ms = record["timeTaken"] or 0

            # --------------------------------------------------
            # Contagem DEPOIS
            # --------------------------------------------------
            after = session.run(
                f"MATCH (n:{label}) RETURN count(n) AS c"
            ).single()["c"]

            created = max(after - before, 0)
            updated = total - created

            safe_print(f"   ✅ [{filename}] Carga concluída")
            safe_print(f"   📊 Total processado: {total:,}")
            safe_print(f"   📊 Commitados: {committed:,}")
            safe_print(f"   ❌ Falhas: {failed:,}")
            safe_print(f"   🆕 Criados: {created:,}")
            safe_print(f"   🔄 Atualizados: {updated:,}")
            safe_print(f"   ⏱️ Tempo: {time_ms/1000:.2f}s")

            return {
                "success": True,
                "created": created,
                "updated": updated,
                "total": total
            }

    except Exception:
        import traceback
        safe_print(f"   ❌ [{filename}] Erro durante carga")
        safe_print(traceback.format_exc())
        return {"success": False}

# ==========================================================
# Função principal (Airflow)
# ==========================================================
def load_raw_files():

    safe_print("=" * 70)
    safe_print("🚀 INICIANDO CARGA BRONZE (RAW) NO NEO4J")
    safe_print("=" * 70)

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_connection_pool_size=50,
        max_connection_lifetime=3600,
        connection_acquisition_timeout=120,
    )

    driver.verify_connectivity()

    files = sorted(f for f in os.listdir(SOURCE_DIR) if f.endswith(".txt"))
    safe_print(f"📁 Arquivos encontrados: {len(files)}")

    total_created = total_updated = success = failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_file, driver, f, i, len(files)): f
            for i, f in enumerate(files, 1)
        }

        for future in as_completed(futures):
            result = future.result()
            if result and result.get("success"):
                success += 1
                total_created += result["created"]
                total_updated += result["updated"]
            else:
                failed += 1

    driver.close()

    safe_print("=" * 70)
    safe_print("✅ CARGA BRONZE FINALIZADA")
    safe_print(f"✔️ Arquivos com sucesso: {success}")
    safe_print(f"❌ Arquivos com falha: {failed}")
    safe_print(f"🆕 Nós criados: {total_created:,}")
    safe_print(f"🔄 Nós atualizados: {total_updated:,}")
    safe_print("=" * 70)
