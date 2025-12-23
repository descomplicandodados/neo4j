# 🧪 Guia de Testes - Neo4j Data Loader

Suíte completa de testes unitários e de integração para o script de carga de dados no Neo4j.

## 📋 Índice

- [Instalação](#instalação)
- [Estrutura dos Testes](#estrutura-dos-testes)
- [Executando os Testes](#executando-os-testes)
- [Cobertura de Testes](#cobertura-de-testes)
- [Testes Implementados](#testes-implementados)

---

## 🔧 Instalação

### Dependências

```bash
pip install pytest pytest-cov pytest-mock neo4j python-dotenv
```

### Estrutura de Arquivos

```
projeto/
├── load_raw.py              # Script principal
├── test_load_raw.py         # Testes unitários
├── test_integration.py      # Testes de integração
├── conftest.py              # Configurações pytest
├── .env                     # Variáveis de ambiente
└── README_TESTS.md          # Este arquivo
```

---

## 🏗️ Estrutura dos Testes

### 1. **Testes Unitários** (`test_load_raw.py`)

Testes que **não requerem Neo4j** rodando:

- ✅ **Parsing/Extraction Logic**: Testa parsing de CSV com delimitador pipe (`|`)
- ✅ **Normalization/Deduping Logic**: Testa lógica de MERGE e deduplicação
- ✅ **File Processing**: Testa operações de arquivo e contagem de linhas
- ✅ **Environment Validation**: Valida variáveis de ambiente
- ✅ **Thread Safety**: Testa operações concorrentes
- ✅ **Cypher Query Generation**: Valida queries geradas

### 2. **Testes de Integração** (`test_integration.py`)

Testes que **requerem Neo4j** rodando:

- ✅ **Load Small Dataset**: Carrega dataset de exemplo no Neo4j (TESTE PRINCIPAL)
- ✅ **Deduplication**: Verifica que dados duplicados são atualizados
- ✅ **Null ID Filtering**: Confirma filtro de IDs nulos
- ✅ **Order By + Limit**: Testa ordenação e limite de registros
- ✅ **Metadata Fields**: Verifica campos de metadados (`__file`, `__created_at`, etc.)
- ✅ **Error Handling**: Testa tratamento de erros
- ✅ **Performance**: Testes de carga com 1000+ registros

---

## 🚀 Executando os Testes

### Executar TODOS os testes

```bash
pytest -v
```

### Executar APENAS testes unitários (sem Neo4j)

```bash
pytest test_load_raw.py -v
```

### Executar APENAS testes de integração (requer Neo4j)

```bash
pytest test_integration.py -v
```

### Executar com relatório de cobertura

```bash
pytest --cov=load_raw --cov-report=html --cov-report=term
```

Abra `htmlcov/index.html` para visualizar o relatório detalhado.

### Executar testes específicos

```bash
# Por classe
pytest test_load_raw.py::TestParsingAndExtraction -v

# Por método
pytest test_integration.py::TestIntegrationSmallDataset::test_load_small_dataset_into_neo4j -v

# Por palavra-chave
pytest -k "dedup" -v
```

### Executar excluindo testes lentos

```bash
pytest -m "not slow" -v
```

---

## ⚙️ Configuração para Testes de Integração

### Opção 1: Usar Neo4j existente

Configure as variáveis no arquivo `.env.test`:

```bash
TEST_NEO4J_URI=bolt://localhost:7687
TEST_NEO4J_USER=neo4j
TEST_NEO4J_PASSWORD=your_password
```

Carregue antes de rodar:

```bash
export $(cat .env.test | xargs)
pytest test_integration.py -v
```

### Opção 2: Docker Compose para testes

```yaml
# docker-compose.test.yml
version: '3.8'
services:
  neo4j-test:
    image: neo4j:5.17-enterprise
    environment:
      - NEO4J_AUTH=neo4j/testpassword
      - NEO4J_ACCEPT_LICENSE_AGREEMENT=yes
      - NEO4J_PLUGINS=["apoc"]
    ports:
      - "7687:7687"
    volumes:
      - ./test_import:/var/lib/neo4j/import
```

Execute:

```bash
docker compose -f docker-compose.test.yml up -d
export TEST_NEO4J_PASSWORD=testpassword
pytest test_integration.py -v
docker compose -f docker-compose.test.yml down
```

---

## 📊 Cobertura de Testes

### Áreas Cobertas

| Categoria | Testes | Status |
|-----------|--------|--------|
| **Parsing CSV** | 5 testes | ✅ |
| **Normalização** | 5 testes | ✅ |
| **Deduplicação** | 3 testes | ✅ |
| **File Processing** | 6 testes | ✅ |
| **Thread Safety** | 2 testes | ✅ |
| **Validação Env** | 3 testes | ✅ |
| **Cypher Queries** | 5 testes | ✅ |
| **Integração Neo4j** | 7 testes | ✅ |
| **Error Handling** | 2 testes | ✅ |

**Total: 38+ testes implementados**

---

## 🧪 Testes Implementados

### 1. Parsing e Extração (`TestParsingAndExtraction`)

```python
✅ test_csv_parsing_with_pipe_delimiter()
   - Valida parsing de CSV com delimitador |

✅ test_empty_file_handling()
   - Testa arquivo vazio

✅ test_file_with_header_only()
   - Testa arquivo só com header

✅ test_null_id_filtering()
   - Verifica filtro WHERE row.id IS NOT NULL

✅ test_large_file_detection()
   - Testa detecção de arquivo >100k linhas

✅ test_max_lines_limit()
   - Verifica aplicação de MAX_LINES_PER_FILE
```

### 2. Normalização e Deduplicação (`TestNormalizationAndDeduplication`)

```python
✅ test_merge_creates_unique_nodes()
   - Valida MERGE para unicidade

✅ test_on_create_sets_timestamps()
   - Verifica campos __created_at em ON CREATE

✅ test_on_match_updates_timestamps()
   - Verifica campos __updated_at em ON MATCH

✅ test_constraint_prevents_duplicates()
   - Testa constraint UNIQUE

✅ test_data_normalization_preserves_source()
   - Confirma preservação de dados originais
```

### 3. Testes de Integração (`TestIntegrationSmallDataset`)

```python
✅ test_load_small_dataset_into_neo4j()
   🎯 TESTE PRINCIPAL - Carrega 5 registros e valida:
   - Total de registros carregados
   - Dados corretos nos nós
   - Metadados (__file, __created_at)
   - Constraint aplicada

✅ test_deduplication_on_reload()
   - Carrega mesmos dados 2x
   - Verifica que não há duplicação
   - Valida contador de cargas

✅ test_null_id_filtering()
   - Carrega arquivo com IDs nulos
   - Confirma que foram filtrados

✅ test_order_by_desc_limit()
   - Carrega 10 registros com LIMIT 5
   - Verifica ORDER BY DESC

✅ test_metadata_fields()
   - Valida todos campos de metadados
```

---

## 📈 Exemplo de Saída

```bash
$ pytest test_integration.py::TestIntegrationSmallDataset::test_load_small_dataset_into_neo4j -v

test_integration.py::TestIntegrationSmallDataset::test_load_small_dataset_into_neo4j PASSED

======================== 1 passed in 2.34s =========================
```

### Com `-s` para ver prints:

```bash
$ pytest test_integration.py::TestIntegrationSmallDataset::test_load_small_dataset_into_neo4j -v -s

...
⏱️  Carregados 1000 registros em 3.45s
   Neo4j timeTaken: 2893ms
======================== 1 passed in 3.45s =========================
```

---

## 🎯 Checklist para Teste Técnico

- [x] **Parsing/extraction logic** - `TestParsingAndExtraction` (6 testes)
- [x] **Normalization/deduping logic** - `TestNormalizationAndDeduplication` (5 testes)
- [x] **Bonus: Integration test** - `test_load_small_dataset_into_neo4j` ⭐

---

## 🐛 Troubleshooting

### Problema: "Neo4j não disponível"

```bash
# Verificar se Neo4j está rodando
docker compose ps

# Ver logs
docker compose logs neo4j

# Testar conexão
echo "MATCH (n) RETURN count(n);" | cypher-shell -u neo4j -p password
```

### Problema: "APOC procedure not found"

Certifique-se que APOC está instalado:

```yaml
# docker-compose.yml
environment:
  - NEO4J_PLUGINS=["apoc"]
```

### Problema: "Permission denied" ao copiar arquivos

```bash
# Dar permissões ao diretório import
chmod 777 ./neo4j/import
```

---

## 📚 Recursos Adicionais

- [Pytest Documentation](https://docs.pytest.org/)
- [Neo4j Python Driver](https://neo4j.com/docs/python-manual/current/)
- [APOC Documentation](https://neo4j.com/labs/apoc/)

---

## 🤝 Contribuindo

Para adicionar novos testes:

1. Testes unitários → `test_load_raw.py`
2. Testes de integração → `test_integration.py`
3. Fixtures compartilhadas → `conftest.py`
4. Execute `pytest --cov` para verificar cobertura

---

**✨ Pronto! Todos os requisitos do teste técnico estão cobertos.**