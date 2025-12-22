import os
import shutil
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ==========================================================
# Neo4j
# ==========================================================
NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "teste123"

# ==========================================================
# Diretórios
# ==========================================================
SOURCE_DIR = "/opt/bases_neo4j"
NEO4J_IMPORT_DIR = "/opt/airflow/import_raw"

# ==========================================================
# Configurações
# ==========================================================
BATCH_SIZE = 5000
MAX_WORKERS = 3  # Número de arquivos processados simultaneamente

# Lock para prints sincronizados (Airflow-safe)
print_lock = Lock()


def safe_print(*args, **kwargs):
    """Print thread-safe (evita logs embaralhados no Airflow)"""
    with print_lock:
        print(*args, **kwargs)


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
            # Copia do arquivo
            # --------------------------------------------------
            safe_print(f"   📋 [{filename}] Copiando arquivo...")
            shutil.copy(source_path, target_path)

            with open(source_path, "r", encoding="utf-8") as f:
                line_count = max(sum(1 for _ in f) - 1, 0)

            safe_print(f"   ✅ [{filename}] Arquivo copiado ({line_count:,} linhas)")

            is_large_file = line_count > 100_000

            # --------------------------------------------------
            # Cypher
            # --------------------------------------------------
            cypher = f"""
            CALL apoc.periodic.iterate(
              "
              LOAD CSV WITH HEADERS
              FROM 'file:///{filename}' AS row
              FIELDTERMINATOR '|'
              WITH row
              WHERE row.id IS NOT NULL
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
            YIELD batches, total, timeTaken, committedOperations,
                  failedOperations, failedBatches, retries, errorMessages, operations
            RETURN *
            """

            safe_print(
                f"   ⚙️  [{filename}] "
                f"Processando no Neo4j (batch={BATCH_SIZE}, parallel={is_large_file})..."
            )

            result = session.run(cypher)
            record = result.single()

            if not record:
                safe_print(f"   ⚠️  [{filename}] Nenhum resultado retornado")
                return None

            # --------------------------------------------------
            # Métricas seguras
            # --------------------------------------------------
            total = record["total"] or 0
            committed = record["committedOperations"] or 0
            failed = record["failedOperations"] or 0
            time_ms = record["timeTaken"] or 0

            safe_print(f"   ✅ [{filename}] Dados processados")
            safe_print(f"   📊 [{filename}] Total: {total:,}")
            safe_print(f"   📊 [{filename}] Commitados: {committed:,}")
            safe_print(f"   📊 [{filename}] Falhas: {failed:,}")

            if time_ms > 0:
                time_seconds = time_ms / 1000
                rate = total / time_seconds if total else 0
                safe_print(
                    f"   ⏱️  [{filename}] Tempo: {time_seconds:.2f}s "
                    f"({rate:,.0f} registros/s)"
                )
            else:
                safe_print(
                    f"   ⏱️  [{filename}] Tempo: < 1ms "
                    f"({total:,} registros)"
                )

            ops = record.get("operations") or {}
            created = ops.get("created", 0)
            updated = total - created

            safe_print(f"   🆕 [{filename}] Novos: {created:,}")
            safe_print(f"   🔄 [{filename}] Atualizados: {updated:,}")

            if failed > 0:
                safe_print(f"   ⚠️  [{filename}] Erros: {record['errorMessages']}")

            return {
                "filename": filename,
                "success": True,
                "created": created,
                "updated": updated,
                "total": total,
            }

    except Exception as e:
        safe_print(f"   ❌ [{filename}] Erro: {e}")
        import traceback
        safe_print(traceback.format_exc())
        return {
            "filename": filename,
            "success": False,
            "error": str(e),
        }


# ==========================================================
# Função principal (chamada pelo Airflow)
# ==========================================================
def load_raw_files():
    safe_print("=" * 60)
    safe_print("🚀 Iniciando carregamento COMPLETO de dados RAW para Neo4j")
    safe_print(f"⚡ Processamento paralelo com {MAX_WORKERS} workers")
    safe_print("=" * 60)

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_connection_lifetime=3600,
        max_connection_pool_size=50,
        connection_acquisition_timeout=120,
    )

    try:
        driver.verify_connectivity()
        safe_print("✅ Conectado ao Neo4j com sucesso!")
    except Exception as e:
        safe_print(f"❌ Erro ao conectar no Neo4j: {e}")
        driver.close()
        raise

    # ------------------------------------------------------
    # Pré-validações
    # ------------------------------------------------------
    with driver.session() as session:
        try:
            version = session.run("RETURN apoc.version() AS v").single()
            safe_print(f"✅ APOC instalado: {version['v']}")
        except Exception as e:
            safe_print(f"⚠️  APOC pode não estar disponível: {e}")

        safe_print("\n🧹 Limpando duplicatas...")
        session.run("""
            MATCH (n:Raw)
            WITH n.id AS id, collect(n) AS nodes
            WHERE size(nodes) > 1
            FOREACH (node IN tail(nodes) | DELETE node)
        """)

        safe_print("🔧 Criando constraint única...")
        session.run("""
            CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
            FOR (n:Raw)
            REQUIRE n.id IS UNIQUE
        """)

    # ------------------------------------------------------
    # Lista de arquivos
    # ------------------------------------------------------
    files = sorted(f for f in os.listdir(SOURCE_DIR) if f.endswith(".txt"))

    safe_print(f"\n📁 Arquivos encontrados: {len(files)}")

    if not files:
        safe_print("⚠️  Nenhum arquivo .txt encontrado!")
        driver.close()
        return

    # ------------------------------------------------------
    # Execução paralela
    # ------------------------------------------------------
    total_created = 0
    total_updated = 0
    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_file, driver, f, i, len(files)): f
            for i, f in enumerate(files, 1)
        }

        for future in as_completed(futures):
            result = future.result()
            if not result:
                failed += 1
                continue

            if result.get("success"):
                success += 1
                total_created += result.get("created", 0)
                total_updated += result.get("updated", 0)
            else:
                failed += 1

    driver.close()

    safe_print("\n" + "=" * 60)
    safe_print("✅ Processo finalizado!")
    safe_print(f"📁 Arquivos processados com sucesso: {success}/{len(files)}")
    safe_print(f"❌ Arquivos com falha: {failed}")
    safe_print(f"🆕 Total de novos registros: {total_created:,}")
    safe_print(f"🔄 Total de registros atualizados: {total_updated:,}")
    safe_print("=" * 60)
