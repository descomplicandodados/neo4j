import pytest
import os
import shutil
from pathlib import Path
from neo4j import GraphDatabase
import time

# Configurações de teste (usar variáveis de ambiente ou defaults)
TEST_NEO4J_URI = os.getenv("TEST_NEO4J_URI", "bolt://neo4j:7687")
TEST_NEO4J_USER = os.getenv("TEST_NEO4J_USER", "neo4j")
TEST_NEO4J_PASSWORD = os.getenv("TEST_NEO4J_PASSWORD", "password")

# Diretório de import compartilhado com Neo4j (ajustar conforme seu docker-compose)
NEO4J_IMPORT_DIR = os.getenv("NEO4J_IMPORT_DIR", "/opt/airflow/import_raw")


@pytest.fixture(scope="session")
def neo4j_driver():
    """Fixture para conexão com Neo4j"""
    driver = GraphDatabase.driver(
        TEST_NEO4J_URI,
        auth=(TEST_NEO4J_USER, TEST_NEO4J_PASSWORD)
    )
    
    # Verificar conectividade
    try:
        driver.verify_connectivity()
        yield driver
    except Exception as e:
        pytest.skip(f"Neo4j não disponível: {e}")
    finally:
        driver.close()


@pytest.fixture(scope="function")
def clean_database(neo4j_driver):
    """Limpa o banco antes e depois de cada teste"""
    with neo4j_driver.session() as session:
        # Limpar dados de teste
        session.run("MATCH (n:Raw) WHERE n.__test = true DELETE n")
        session.run("DROP CONSTRAINT raw_unique_id IF EXISTS")
    
    yield
    
    with neo4j_driver.session() as session:
        # Limpar após teste
        session.run("MATCH (n:Raw) WHERE n.__test = true DELETE n")


@pytest.fixture
def import_dir():
    """Retorna o diretório de import compartilhado"""
    # Criar diretório se não existir
    os.makedirs(NEO4J_IMPORT_DIR, exist_ok=True)
    return NEO4J_IMPORT_DIR


@pytest.fixture
def sample_csv_file(import_dir):
    """Cria arquivo CSV de exemplo direto no import_dir"""
    csv_content = """id|nome|idade|cidade
1|João Silva|30|São Paulo
2|Maria Santos|25|Rio de Janeiro
3|Pedro Oliveira|35|Belo Horizonte
4|Ana Costa|28|Curitiba
5|Carlos Souza|40|Porto Alegre
"""
    
    filename = "clientes_test.txt"
    filepath = os.path.join(import_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    
    yield filename, filepath
    
    # Cleanup após o teste
    try:
        os.remove(filepath)
    except:
        pass


class TestIntegrationSmallDataset:
    """Testes de integração com dataset pequeno"""
    
    def test_load_small_dataset_into_neo4j(self, neo4j_driver, clean_database, sample_csv_file):
        """
        TESTE DE INTEGRAÇÃO PRINCIPAL
        Carrega um dataset pequeno no Neo4j e verifica os resultados
        """
        filename, filepath = sample_csv_file
        
        # Criar constraint
        with neo4j_driver.session() as session:
            session.run("""
                CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                FOR (n:Raw)
                REQUIRE n.id IS UNIQUE
            """)
        
        # Executar carga usando LOAD CSV com APOC
        with neo4j_driver.session() as session:
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
                  n.__test = true,
                  n.__created_at = datetime()
              ON MATCH SET
                  n += row,
                  n.__file = '{filename}',
                  n.__test = true,
                  n.__updated_at = datetime()
              ",
              {{
                batchSize: 1000,
                parallel: false
              }}
            )
            YIELD total, committedOperations, failedOperations
            RETURN total, committedOperations, failedOperations
            """
            
            result = session.run(cypher).single()
            
            # Verificações
            assert result is not None, "Query não retornou resultados"
            assert result["total"] == 5, f"Esperado 5 registros, obtido {result['total']}"
            assert result["committedOperations"] == 5, "Nem todas operações foram commitadas"
            assert result["failedOperations"] == 0, "Houve operações com falha"
        
        # Verificar dados carregados
        with neo4j_driver.session() as session:
            # Contar nós
            count_result = session.run(
                "MATCH (n:Raw {__test: true}) RETURN count(n) as count"
            ).single()
            assert count_result["count"] == 5, "Número incorreto de nós criados"
            
            # Verificar nó específico
            node_result = session.run(
                "MATCH (n:Raw {id: '1', __test: true}) RETURN n"
            ).single()
            assert node_result is not None, "Nó com id=1 não encontrado"
            
            node = node_result["n"]
            assert node["nome"] == "João Silva"
            assert node["idade"] == "30"
            assert node["cidade"] == "São Paulo"
            assert node["__file"] == filename
            assert "__created_at" in node
    
    def test_deduplication_on_reload(self, neo4j_driver, clean_database, sample_csv_file):
        """Testa se registros duplicados são atualizados, não duplicados"""
        filename, filepath = sample_csv_file
        
        # Criar constraint
        with neo4j_driver.session() as session:
            session.run("""
                CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                FOR (n:Raw) REQUIRE n.id IS UNIQUE
            """)
        
        # Primeira carga
        with neo4j_driver.session() as session:
            cypher = f"""
            LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row
            FIELDTERMINATOR '|'
            WITH row WHERE row.id IS NOT NULL
            MERGE (n:Raw {{id: row.id}})
            ON CREATE SET n += row, n.__test = true, n.__load_count = 1
            ON MATCH SET n += row, n.__test = true, n.__load_count = coalesce(n.__load_count, 0) + 1
            RETURN count(n) as count
            """
            result1 = session.run(cypher).single()
            initial_count = result1["count"]
        
        # Segunda carga (mesmos dados)
        with neo4j_driver.session() as session:
            result2 = session.run(cypher).single()
            second_count = result2["count"]
        
        # Verificar que não houve duplicação
        with neo4j_driver.session() as session:
            total = session.run(
                "MATCH (n:Raw {__test: true}) RETURN count(n) as count"
            ).single()["count"]
            
            assert total == 5, f"Esperado 5 nós únicos, obtido {total}"
            
            # Verificar que __load_count foi incrementado
            node = session.run(
                "MATCH (n:Raw {id: '1', __test: true}) RETURN n.__load_count as count"
            ).single()
            
            assert node["count"] == 2, "Contador de cargas não foi incrementado"
    
    def test_null_id_filtering(self, neo4j_driver, clean_database, import_dir):
        """Testa se registros com ID nulo são filtrados"""
        # Criar arquivo com IDs nulos direto no import_dir
        csv_content = """id|nome|idade
1|João|30
|Maria|25
3|Pedro|35
"""
        filename = "test_null_test.txt"
        filepath = os.path.join(import_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        try:
            with neo4j_driver.session() as session:
                session.run("""
                    CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                    FOR (n:Raw) REQUIRE n.id IS UNIQUE
                """)
                
                cypher = f"""
                LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row
                FIELDTERMINATOR '|'
                WITH row WHERE row.id IS NOT NULL AND row.id <> ''
                MERGE (n:Raw {{id: row.id}})
                ON CREATE SET n += row, n.__test = true
                RETURN count(n) as count
                """
                
                result = session.run(cypher).single()
                
                # Deve ter carregado apenas 2 registros (id=1 e id=3)
                assert result["count"] == 2, f"Esperado 2 registros, obtido {result['count']}"
                
                # Verificar que Maria (sem ID) não foi carregada
                maria = session.run(
                    "MATCH (n:Raw {nome: 'Maria', __test: true}) RETURN n"
                ).single()
                assert maria is None, "Registro com ID nulo não deveria ter sido carregado"
        finally:
            # Cleanup
            try:
                os.remove(filepath)
            except:
                pass
    
    def test_order_by_desc_limit(self, neo4j_driver, clean_database, import_dir):
        """Testa ORDER BY DESC e LIMIT"""
        # Criar arquivo com 10 registros usando IDs únicos para este teste
        csv_lines = ["id|valor"]
        # Usar IDs com prefixo para evitar conflito com outros testes
        csv_lines.extend([f"ORDER_TEST_{i}|valor{i}" for i in range(1, 11)])
        csv_content = "\n".join(csv_lines) + "\n"
        
        filename = "test_order_unique.txt"
        filepath = os.path.join(import_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        max_lines = 5
        
        try:
            with neo4j_driver.session() as session:
                session.run("""
                    CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                    FOR (n:Raw) REQUIRE n.id IS UNIQUE
                """)
                
                # Limpar registros deste teste específico antes
                session.run("MATCH (n:Raw) WHERE n.id STARTS WITH 'ORDER_TEST_' DELETE n")
                
                cypher = f"""
                LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row
                FIELDTERMINATOR '|'
                WITH row
                WHERE row.id IS NOT NULL
                WITH row
                ORDER BY row.id DESC
                LIMIT {max_lines}
                MERGE (n:Raw {{id: row.id}})
                ON CREATE SET n += row, n.__test = true, n.__test_type = 'order_test'
                RETURN count(n) as count
                """
                
                result = session.run(cypher).single()
                
                # Verificar que processou 5 registros
                assert result["count"] == max_lines, f"Esperado {max_lines} registros processados"
                
                # Contar registros deste teste que foram criados
                created_count = session.run(
                    "MATCH (n:Raw {__test_type: 'order_test'}) RETURN count(n) as count"
                ).single()["count"]
                
                # Deve ter criado exatamente 5 registros
                assert created_count == 5, f"Esperado 5 registros criados, obtido {created_count}"
                
                # Verificar que o LIMIT funcionou (arquivo tem 10 linhas mas só 5 foram processadas)
                all_test_nodes = session.run(
                    "MATCH (n:Raw) WHERE n.id STARTS WITH 'ORDER_TEST_' RETURN count(n) as count"
                ).single()["count"]
                
                assert all_test_nodes == 5, f"LIMIT não funcionou corretamente: {all_test_nodes} registros ao invés de 5"
                
        finally:
            # Cleanup - remover registros de teste
            with neo4j_driver.session() as session:
                session.run("MATCH (n:Raw) WHERE n.id STARTS WITH 'ORDER_TEST_' DELETE n")
            try:
                os.remove(filepath)
            except:
                pass
    
    def test_metadata_fields(self, neo4j_driver, clean_database, sample_csv_file):
        """Testa se campos de metadados são adicionados corretamente"""
        filename, filepath = sample_csv_file
        
        with neo4j_driver.session() as session:
            session.run("""
                CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                FOR (n:Raw) REQUIRE n.id IS UNIQUE
            """)
            
            cypher = f"""
            LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row
            FIELDTERMINATOR '|'
            WITH row WHERE row.id IS NOT NULL
            MERGE (n:Raw {{id: row.id}})
            ON CREATE SET
                n += row,
                n.__file = '{filename}',
                n.__test = true,
                n.__created_at = datetime()
            RETURN count(n) as count
            """
            
            session.run(cypher)
            
            # Verificar metadados
            node = session.run(
                "MATCH (n:Raw {id: '1', __test: true}) RETURN n"
            ).single()["n"]
            
            assert node["__file"] == filename
            assert node["__test"] is True
            assert "__created_at" in node
            
            # Verificar dados originais preservados
            assert node["nome"] == "João Silva"
            assert node["idade"] == "30"


class TestIntegrationErrorHandling:
    """Testes de tratamento de erros"""
    
    def test_missing_file_error(self, neo4j_driver, clean_database):
        """Testa erro ao tentar carregar arquivo inexistente"""
        with neo4j_driver.session() as session:
            with pytest.raises(Exception):
                session.run("""
                    LOAD CSV WITH HEADERS FROM 'file:///nao_existe.txt' AS row
                    RETURN row
                """).consume()
    
    def test_constraint_violation_handling(self, neo4j_driver, clean_database):
        """Testa que MERGE trata violação de constraint corretamente"""
        with neo4j_driver.session() as session:
            session.run("""
                CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                FOR (n:Raw) REQUIRE n.id IS UNIQUE
            """)
            
            # Criar nó
            session.run("""
                CREATE (n:Raw {id: '999', nome: 'Teste', __test: true})
            """)
            
            # Tentar criar novamente com MERGE (não deve dar erro)
            result = session.run("""
                MERGE (n:Raw {id: '999'})
                ON MATCH SET n.nome = 'Teste Atualizado', n.__test = true
                RETURN n.nome as nome
            """).single()
            
            assert result["nome"] == "Teste Atualizado"


@pytest.mark.skipif(
    not all([TEST_NEO4J_URI, TEST_NEO4J_USER, TEST_NEO4J_PASSWORD]),
    reason="Credenciais Neo4j não configuradas"
)
class TestIntegrationPerformance:
    """Testes de performance (opcional)"""
    
    def test_batch_processing_performance(self, neo4j_driver, clean_database, import_dir):
        """Testa performance com batch processing"""
        # Criar arquivo com 1000 registros direto no import_dir
        csv_lines = ["id|nome|valor"]
        csv_lines.extend([f"{i}|Nome{i}|{i*100}" for i in range(1, 1001)])
        csv_content = "\n".join(csv_lines) + "\n"
        
        filename = "performance_test_test.txt"
        filepath = os.path.join(import_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        try:
            with neo4j_driver.session() as session:
                session.run("""
                    CREATE CONSTRAINT raw_unique_id IF NOT EXISTS
                    FOR (n:Raw) REQUIRE n.id IS UNIQUE
                """)
                
                start_time = time.time()
                
                cypher = f"""
                CALL apoc.periodic.iterate(
                  "
                  LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row
                  FIELDTERMINATOR '|'
                  WITH row WHERE row.id IS NOT NULL
                  RETURN row
                  ",
                  "
                  MERGE (n:Raw {{id: row.id}})
                  ON CREATE SET n += row, n.__test = true
                  ",
                  {{batchSize: 100, parallel: false}}
                )
                YIELD total, committedOperations, timeTaken
                RETURN total, committedOperations, timeTaken
                """
                
                result = session.run(cypher).single()
                elapsed_time = time.time() - start_time
                
                assert result["total"] == 1000
                assert result["committedOperations"] == 1000
                print(f"\n⏱️  Carregados 1000 registros em {elapsed_time:.2f}s")
                print(f"   Neo4j timeTaken: {result['timeTaken']}ms")
        finally:
            # Cleanup
            try:
                os.remove(filepath)
            except:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])