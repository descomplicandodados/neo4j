"""
Configurações compartilhadas para testes pytest
"""
import pytest
import os
import sys
from pathlib import Path

# Adicionar diretório raiz ao path para imports
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))


def pytest_configure(config):
    """Configuração global do pytest"""
    config.addinivalue_line(
        "markers", "integration: marca testes de integração que requerem Neo4j"
    )
    config.addinivalue_line(
        "markers", "slow: marca testes que demoram mais tempo"
    )


@pytest.fixture(scope="session")
def test_data_dir():
    """Diretório para dados de teste"""
    data_dir = ROOT_DIR / "test_data"
    data_dir.mkdir(exist_ok=True)
    return data_dir


@pytest.fixture
def sample_data():
    """Dados de exemplo para testes"""
    return {
        "clients": [
            {"id": "1", "nome": "João Silva", "idade": "30", "cidade": "São Paulo"},
            {"id": "2", "nome": "Maria Santos", "idade": "25", "cidade": "Rio de Janeiro"},
            {"id": "3", "nome": "Pedro Oliveira", "idade": "35", "cidade": "Belo Horizonte"},
        ],
        "products": [
            {"id": "101", "produto": "Notebook", "preco": "3500", "estoque": "10"},
            {"id": "102", "produto": "Mouse", "preco": "50", "estoque": "100"},
            {"id": "103", "produto": "Teclado", "preco": "150", "estoque": "50"},
        ],
    }


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock de variáveis de ambiente para testes"""
    env_vars = {
        "NEO4J_URI": "bolt://localhost:7687",
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": "password",
        "SOURCE_DIR": "/tmp/test_source",
        "NEO4J_IMPORT_DIR": "/tmp/test_import",
        "BATCH_SIZE": "5000",
        "MAX_WORKERS": "3",
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars


def pytest_collection_modifyitems(config, items):
    """Modifica items coletados para adicionar markers"""
    for item in items:
        # Marcar testes de integração
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        
        # Marcar testes lentos
        if "performance" in item.nodeid.lower():
            item.add_marker(pytest.mark.slow)