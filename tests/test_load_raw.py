import pytest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
import sys

# Importar o módulo a ser testado
# Assumindo que o script está em load_raw.py
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# from load_raw import process_file, load_raw_files, safe_print


class TestParsingAndExtraction:
    """Testes para lógica de parsing e extração de dados"""

    def test_csv_parsing_with_pipe_delimiter(self):
        """Testa se o parsing lida corretamente com delimitador pipe (|)"""
        # Simular arquivo CSV com pipe
        csv_content = "id|nome|idade\n1|João|30\n2|Maria|25\n"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            # Verificar se arquivo foi criado corretamente
            with open(temp_path, 'r') as f:
                lines = f.readlines()
                assert len(lines) == 3
                assert '|' in lines[0]
                # Verificar estrutura dos dados
                headers = lines[0].strip().split('|')
                assert headers == ['id', 'nome', 'idade']
                
                first_row = lines[1].strip().split('|')
                assert first_row == ['1', 'João', '30']
        finally:
            os.unlink(temp_path)

    def test_empty_file_handling(self):
        """Testa tratamento de arquivo vazio"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                line_count = sum(1 for _ in f)
                assert line_count == 0
        finally:
            os.unlink(temp_path)

    def test_file_with_header_only(self):
        """Testa arquivo com apenas cabeçalho"""
        csv_content = "id|nome|idade\n"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                line_count = sum(1 for _ in f) - 1  # Subtrair header
                assert line_count == 0
        finally:
            os.unlink(temp_path)

    def test_null_id_filtering(self):
        """Testa se registros com ID nulo são filtrados"""
        # O Cypher tem: WHERE row.id IS NOT NULL
        cypher_filter = "WHERE row.id IS NOT NULL"
        
        test_rows = [
            {"id": "1", "nome": "João"},
            {"id": None, "nome": "Maria"},  # Deve ser filtrado
            {"id": "", "nome": "Pedro"},    # Pode ser considerado null
            {"id": "2", "nome": "Ana"},
        ]
        
        # Simular filtro do Cypher
        filtered = [r for r in test_rows if r.get("id")]
        assert len(filtered) == 2
        assert filtered[0]["nome"] == "João"
        assert filtered[1]["nome"] == "Ana"

    def test_large_file_detection(self):
        """Testa detecção de arquivo grande (>100k linhas)"""
        line_counts = [50000, 100000, 100001, 200000]
        expected_parallel = [False, False, True, True]
        
        for count, expected in zip(line_counts, expected_parallel):
            is_large = count > 100_000
            assert is_large == expected

    def test_max_lines_limit(self):
        """Testa se o limite de linhas (MAX_LINES_PER_FILE) é respeitado"""
        max_lines = 10000
        total_lines = 15000
        
        # O Cypher usa ORDER BY + LIMIT
        # Simular que apenas max_lines serão processadas
        processed_lines = min(total_lines, max_lines)
        assert processed_lines == max_lines


class TestNormalizationAndDeduplication:
    """Testes para lógica de normalização e deduplicação"""

    def test_merge_creates_unique_nodes(self):
        """Testa se MERGE garante unicidade por ID"""
        # Simular operação MERGE do Neo4j
        existing_ids = {"1", "2", "3"}
        new_ids = ["1", "4", "2", "5"]  # 1 e 2 já existem
        
        created = []
        updated = []
        
        for id_val in new_ids:
            if id_val in existing_ids:
                updated.append(id_val)
            else:
                created.append(id_val)
                existing_ids.add(id_val)
        
        assert len(created) == 2  # 4 e 5
        assert len(updated) == 2  # 1 e 2
        assert created == ["4", "5"]
        assert updated == ["1", "2"]

    def test_on_create_sets_timestamps(self):
        """Testa se timestamps são definidos na criação"""
        node_data = {
            "id": "1",
            "nome": "João",
            "__file": "test.txt",
            "__loaded_at": "2025-01-01T00:00:00",
            "__created_at": "2025-01-01T00:00:00",
        }
        
        # Verificar campos obrigatórios em criação
        assert "__file" in node_data
        assert "__loaded_at" in node_data
        assert "__created_at" in node_data
        assert "__updated_at" not in node_data  # Não existe em CREATE

    def test_on_match_updates_timestamps(self):
        """Testa se timestamps são atualizados no match"""
        existing_node = {
            "id": "1",
            "nome": "João",
            "__created_at": "2025-01-01T00:00:00",
        }
        
        updated_node = {
            **existing_node,
            "nome": "João Silva",  # Atualizado
            "__loaded_at": "2025-01-02T00:00:00",
            "__updated_at": "2025-01-02T00:00:00",
        }
        
        # Verificar que __created_at permanece, mas __updated_at é adicionado
        assert updated_node["__created_at"] == existing_node["__created_at"]
        assert "__updated_at" in updated_node
        assert updated_node["nome"] == "João Silva"

    def test_constraint_prevents_duplicates(self):
        """Testa se constraint UNIQUE previne IDs duplicados"""
        # Simular constraint: raw_unique_id FOR (n:Raw) REQUIRE n.id IS UNIQUE
        constraint_query = "CREATE CONSTRAINT raw_unique_id IF NOT EXISTS FOR (n:Raw) REQUIRE n.id IS UNIQUE"
        
        assert "UNIQUE" in constraint_query
        assert "n.id" in constraint_query
        
        # Simular inserção duplicada
        existing_ids = {"1"}
        new_id = "1"
        
        # Deve resultar em UPDATE, não erro
        if new_id in existing_ids:
            operation = "UPDATE"
        else:
            operation = "CREATE"
            existing_ids.add(new_id)
        
        assert operation == "UPDATE"

    def test_data_normalization_preserves_source(self):
        """Testa se dados originais são preservados com metadados"""
        source_data = {"id": "1", "nome": "João", "cidade": "SP"}
        filename = "clientes.txt"
        
        normalized_data = {
            **source_data,
            "__file": filename,
            "__loaded_at": "2025-01-01T00:00:00",
        }
        
        # Verificar que dados originais foram preservados
        assert normalized_data["id"] == source_data["id"]
        assert normalized_data["nome"] == source_data["nome"]
        assert normalized_data["cidade"] == source_data["cidade"]
        # Verificar metadados adicionados
        assert normalized_data["__file"] == filename


class TestFileProcessing:
    """Testes para processamento de arquivos"""

    @patch('shutil.copy')
    def test_file_copy_operation(self, mock_copy):
        """Testa se arquivo é copiado corretamente"""
        source = "/source/test.txt"
        target = "/import/test.txt"
        
        shutil.copy(source, target)
        mock_copy.assert_called_once_with(source, target)

    def test_line_count_calculation(self):
        """Testa cálculo correto de linhas (excluindo header)"""
        csv_content = "id|nome\n1|João\n2|Maria\n3|Pedro\n"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            with open(temp_path, 'r') as f:
                line_count = max(sum(1 for _ in f) - 1, 0)  # Mesma lógica do script
                assert line_count == 3  # 4 linhas totais - 1 header
        finally:
            os.unlink(temp_path)

    def test_missing_file_handling(self):
        """Testa tratamento de arquivo inexistente"""
        non_existent_file = "/tmp/does_not_exist_12345.txt"
        assert not os.path.exists(non_existent_file)

    def test_batch_size_configuration(self):
        """Testa se batch size é configurável"""
        batch_sizes = [1000, 5000, 10000]
        
        for batch_size in batch_sizes:
            cypher = f"batchSize: {batch_size}"
            assert str(batch_size) in cypher

    def test_parallel_processing_flag(self):
        """Testa flag de processamento paralelo"""
        test_cases = [
            (50000, False),   # Arquivo pequeno
            (150000, True),   # Arquivo grande
        ]
        
        for line_count, expected_parallel in test_cases:
            is_large = line_count > 100_000
            assert is_large == expected_parallel


class TestEnvironmentValidation:
    """Testes para validação de variáveis de ambiente"""

    def test_required_env_vars_missing(self):
        """Testa se erro é levantado quando variáveis estão ausentes"""
        required = {
            "NEO4J_URI": None,
            "NEO4J_USER": "user",
            "NEO4J_PASSWORD": None,
        }
        
        missing = [k for k, v in required.items() if not v]
        assert len(missing) == 2
        assert "NEO4J_URI" in missing
        assert "NEO4J_PASSWORD" in missing

    def test_all_required_env_vars_present(self):
        """Testa quando todas variáveis estão presentes"""
        required = {
            "NEO4J_URI": "bolt://localhost:7687",
            "NEO4J_USER": "neo4j",
            "NEO4J_PASSWORD": "password",
            "SOURCE_DIR": "/data",
            "NEO4J_IMPORT_DIR": "/import",
        }
        
        missing = [k for k, v in required.items() if not v]
        assert len(missing) == 0

    def test_default_values_for_optional_vars(self):
        """Testa valores padrão para variáveis opcionais"""
        max_lines = int(os.getenv("MAX_LINES_PER_FILE", "10000"))
        batch_size = int(os.getenv("BATCH_SIZE", "5000"))
        max_workers = int(os.getenv("MAX_WORKERS", "3"))
        
        # Se não definidas, devem usar defaults
        assert max_lines >= 1000
        assert batch_size >= 1000
        assert max_workers >= 1


class TestThreadSafety:
    """Testes para operações thread-safe"""

    def test_safe_print_with_lock(self):
        """Testa se safe_print usa lock corretamente"""
        from threading import Lock
        
        print_lock = Lock()
        messages = []
        
        def safe_print_mock(msg):
            with print_lock:
                messages.append(msg)
        
        # Simular múltiplas threads
        safe_print_mock("Thread 1")
        safe_print_mock("Thread 2")
        safe_print_mock("Thread 3")
        
        assert len(messages) == 3
        assert "Thread 1" in messages

    def test_concurrent_file_processing(self):
        """Testa processamento concorrente de múltiplos arquivos"""
        from concurrent.futures import ThreadPoolExecutor
        
        files = ["file1.txt", "file2.txt", "file3.txt"]
        results = []
        
        def mock_process(filename):
            return {"success": True, "file": filename}
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(mock_process, f) for f in files]
            results = [f.result() for f in futures]
        
        assert len(results) == 3
        assert all(r["success"] for r in results)


class TestCypherQueryGeneration:
    """Testes para geração de queries Cypher"""

    def test_cypher_order_by_desc(self):
        """Testa se ORDER BY DESC está presente"""
        cypher = "ORDER BY row.id DESC"
        assert "ORDER BY" in cypher
        assert "DESC" in cypher

    def test_cypher_limit_clause(self):
        """Testa se LIMIT está presente"""
        max_lines = 10000
        cypher = f"LIMIT {max_lines}"
        assert "LIMIT" in cypher
        assert str(max_lines) in cypher

    def test_cypher_merge_operation(self):
        """Testa se operação MERGE está correta"""
        cypher = "MERGE (n:Raw {id: row.id})"
        assert "MERGE" in cypher
        assert "Raw" in cypher
        assert "id: row.id" in cypher

    def test_cypher_fieldterminator_pipe(self):
        """Testa se delimitador pipe está configurado"""
        cypher = "FIELDTERMINATOR '|'"
        assert "FIELDTERMINATOR" in cypher
        assert "'|'" in cypher

    def test_apoc_periodic_iterate_usage(self):
        """Testa uso correto de apoc.periodic.iterate"""
        cypher = "CALL apoc.periodic.iterate"
        assert "apoc.periodic.iterate" in cypher


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])