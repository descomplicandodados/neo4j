import os
import shutil
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ==========================================================
# Variáveis de ambiente (.env)
# ==========================================================
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

SOURCE_DIR = os.getenv("SOURCE_DIR")
NEO4J_IMPORT_DIR = os.getenv("NEO4J_IMPORT_DIR")

MAX_LINES_PER_FILE = int(os.getenv("MAX_LINES_PER_FILE", "10000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))

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

    if not os.path.exists(source_path):
        safe_print(f"⚠️  Arquivo não encontrado: {source_path}")
        return None

    safe_print(f"\n🔄 [{idx}/{total_files}] Processando: {filename}")

    try:
        with driver.session() as session:
            # --------------------------------------------------
            # Copiar arquivo
            # --------------------------------------------------
            safe_print(f"   📋 [{filename}] Copiando arquivo...")
            shutil.copy(source_path, target_path)

            with open(source_path, "r", encoding="utf-8") as f:
                line_count = max(sum(1 for _ in f) - 1, 0)

            safe_print(f"   ✅ [{filename}] Arquivo copiado ({line_count:,} linhas)")

            is_large_file = line_count > 100_000

            # --------------------------------------------------
            # Cypher CORRETO (ORDER BY + LIMIT)
            # --------------------------------------------------
            cypher = f"""
            CALL apoc.periodic.iterate(
              "
              LOAD CSV WITH HEADERS
              FROM 'file:///{filename}' AS row
              FIELDTERMINATOR '|'
              WITH row
              WHERE row.id IS NOT NULL
              WITH row
              ORDER BY row.id ASC
              LIMIT {MAX_LINES_PER_FILE}
              RETURN row
              ",
              "
              MERGE (n:Raw {{id: row.id}})
              ON CREATE SET
                  n += row,
                  n.__file = '{filename}',
                  n.__loaded_at = datetime(),
                  n.__created_at = datetime()
              ON MATCH SET
                  n += row,
                  n.__file = '{filename}',
                  n.__loaded_at = datetime(),
                  n.__updated_at = datetime()
              ",
              {{
                batchSize: {BATCH_SIZE},
                parallel: {'true' if is_large_file else 'false'}
              }}
            )
            YIELD total, committedOperations, failedOperations, timeTaken, operations
            RETURN *
            """

            safe_print(
                f"   ⚙️  [{filename}] "
                f"Neo4j (batch={BATCH_SIZE}, limit={MAX_LINES_PER_FILE}, parallel={is_large_file})"
            )

            record = session.run(cypher).single()

            if not record:
                safe_print(f"   ⚠️  [{filename}] Nenhum resultado retornado")
                return None

            total = record.get("total", 0) or 0
            committed = record.get("committedOperations", 0) or 0
            failed = record.get("failedOperations", 0) or 0
            time_ms = record.get("timeTaken", 0) or 0

            safe_print(f"   ✅ [{filename}] Processado")
            safe_print(f"   📊 Total: {total:,}")
            safe_print(f"   📊 Commitados: {committed:,}")
            safe_print(f"   📊 Falhas: {failed:,}")

            if time_ms > 0:
                time_s = time_ms / 1000
                rate = total / time_s if total else 0
                safe_print(f"   ⏱️  {time_s:.2f}s ({rate:,.0f} reg/s)")
            else:
                safe_print(f"   ⏱️  <1ms")

            ops = record.get("operations") or {}
            created = ops.get("created", 0)
            updated = total - created

            safe_print(f"   🆕 Criados: {created:,}")
            safe_print(f"   🔄 Atualizados: {updated:,}")

            return {
                "success": True,
                "created": created,
                "updated": updated,
                "total": total,
            }

    except Exception as e:
        safe_print(f"   ❌ [{filename}] Erro: {e}")
        import traceback
        safe_print(traceback.format_exc())
        return {"success": False}


# ==========================================================
# Função principal (Airflow)
# ==========================================================
def load_raw_files():
    safe_print("=" * 60)
    safe_print("🚀 Iniciando carga RAW no Neo4j")
    safe_print("=" * 60)

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_connection_pool_size=50,
        max_connection_lifetime=3600,
        connection_acquisition_timeout=120,
    )

    driver.verify_connectivity()

    # Constraint única
    with driver.session() as session:
        session.run("""
            CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
            FOR (n:Raw)
            REQUIRE n.id IS UNIQUE
        """)

    files = sorted(f for f in os.listdir(SOURCE_DIR) if f.endswith(".txt"))
    safe_print(f"📁 Arquivos encontrados: {len(files)}")

    if not files:
        driver.close()
        return

    total_created = total_updated = success = failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_file, driver, f, i, len(files)): f
            for i, f in enumerate(files, 1)
        }

        for future in as_completed(futures):
            r = future.result()
            if r and r.get("success"):
                success += 1
                total_created += r.get("created", 0)
                total_updated += r.get("updated", 0)
            else:
                failed += 1

    driver.close()

    safe_print("=" * 60)
    safe_print("✅ Carga finalizada")
    safe_print(f"✔️ Sucesso: {success}")
    safe_print(f"❌ Falhas: {failed}")
    safe_print(f"🆕 Criados: {total_created:,}")
    safe_print(f"🔄 Atualizados: {total_updated:,}")
    safe_print("=" * 60)
