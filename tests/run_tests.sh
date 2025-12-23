#!/bin/bash

# Script para executar testes do projeto Neo4j Data Loader no Docker/Airflow
# Uso: ./run_tests.sh [opção]

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para imprimir cabeçalho
print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
}

# Função para imprimir sucesso
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

# Função para imprimir erro
print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Função para imprimir aviso
print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Verificar se está rodando dentro do container ou fora
is_inside_docker() {
    [ -f /.dockerenv ] || grep -q docker /proc/1/cgroup 2>/dev/null
}

# Verificar se container está rodando (apenas quando fora do Docker)
check_container() {
    if ! is_inside_docker; then
        if ! docker ps | grep -q "airflow"; then
            print_error "Container 'airflow' não está rodando"
            echo "Execute: docker compose up -d"
            exit 1
        fi
    fi
}

# Verificar se pytest está instalado
check_dependencies() {
    if is_inside_docker; then
        if ! command -v pytest &> /dev/null; then
            print_warning "pytest não encontrado, instalando..."
            pip install -q pytest pytest-cov neo4j
        fi
        print_success "Dependências verificadas"
    fi
}

# Executar testes unitários
run_unit_tests() {
    print_header "Executando Testes Unitários"
    
    if is_inside_docker; then
        # Rodando dentro do container
        cd /opt/airflow/tests
        pytest test_load_raw.py -v --tb=short
    else
        # Rodando fora do container - usar usuário airflow
        docker exec -u airflow airflow bash -c "
            cd /opt/airflow/tests && \
            pytest test_load_raw.py -v --tb=short
        "
    fi
    
    print_success "Testes unitários concluídos"
}

# Executar testes de integração
run_integration_tests() {
    print_header "Executando Testes de Integração"
    
    # Configurar variáveis de ambiente
    export TEST_NEO4J_URI=${TEST_NEO4J_URI:-bolt://neo4j:7687}
    export TEST_NEO4J_USER=${TEST_NEO4J_USER:-neo4j}
    export TEST_NEO4J_PASSWORD=${TEST_NEO4J_PASSWORD:-password}
    
    if [ -z "$TEST_NEO4J_PASSWORD" ] || [ "$TEST_NEO4J_PASSWORD" = "password" ]; then
        print_warning "Usando senha padrão. Configure TEST_NEO4J_PASSWORD se necessário"
    fi
    
    if is_inside_docker; then
        # Rodando dentro do container
        cd /opt/airflow/tests
        pytest teste_integration.py -v --tb=short -s
    else
        # Rodando fora do container - usar usuário airflow
        docker exec -u airflow \
                    -e TEST_NEO4J_URI="$TEST_NEO4J_URI" \
                    -e TEST_NEO4J_USER="$TEST_NEO4J_USER" \
                    -e TEST_NEO4J_PASSWORD="$TEST_NEO4J_PASSWORD" \
                    airflow bash -c "
            cd /opt/airflow/tests && \
            pytest teste_integration.py -v --tb=short -s
        "
    fi
    
    print_success "Testes de integração concluídos"
}

# Executar todos os testes
run_all_tests() {
    print_header "Executando TODOS os Testes"
    
    export TEST_NEO4J_URI=${TEST_NEO4J_URI:-bolt://neo4j:7687}
    export TEST_NEO4J_USER=${TEST_NEO4J_USER:-neo4j}
    export TEST_NEO4J_PASSWORD=${TEST_NEO4J_PASSWORD:-password}
    
    if is_inside_docker; then
        # Rodando dentro do container
        cd /opt/airflow/tests
        pytest -v --tb=short
    else
        # Rodando fora do container - usar usuário airflow
        docker exec -u airflow \
                    -e TEST_NEO4J_URI="$TEST_NEO4J_URI" \
                    -e TEST_NEO4J_USER="$TEST_NEO4J_USER" \
                    -e TEST_NEO4J_PASSWORD="$TEST_NEO4J_PASSWORD" \
                    airflow bash -c "
            cd /opt/airflow/tests && \
            pytest -v --tb=short
        "
    fi
    
    print_success "Todos os testes concluídos"
}

# Executar com cobertura
run_coverage() {
    print_header "Executando Testes com Cobertura"
    
    export TEST_NEO4J_URI=${TEST_NEO4J_URI:-bolt://neo4j:7687}
    export TEST_NEO4J_USER=${TEST_NEO4J_USER:-neo4j}
    export TEST_NEO4J_PASSWORD=${TEST_NEO4J_PASSWORD:-password}
    
    if is_inside_docker; then
        # Rodando dentro do container
        cd /opt/airflow/tests
        pytest --cov=/opt/airflow/scripts \
               --cov-report=html \
               --cov-report=term \
               --cov-report=term-missing \
               -v
    else
        # Rodando fora do container - usar usuário airflow
        docker exec -u airflow \
                    -e TEST_NEO4J_URI="$TEST_NEO4J_URI" \
                    -e TEST_NEO4J_USER="$TEST_NEO4J_USER" \
                    -e TEST_NEO4J_PASSWORD="$TEST_NEO4J_PASSWORD" \
                    airflow bash -c "
            cd /opt/airflow/tests && \
            pytest --cov=/opt/airflow/scripts \
                   --cov-report=html:/opt/airflow/tests/htmlcov \
                   --cov-report=term \
                   --cov-report=term-missing \
                   -v
        "
    fi
    
    print_success "Relatório de cobertura gerado em tests/htmlcov/index.html"
}

# Executar testes rápidos (sem integração)
run_fast_tests() {
    print_header "Executando Testes Rápidos (Unitários)"
    
    if is_inside_docker; then
        cd /opt/airflow/tests
        pytest test_load_raw.py -v -x --tb=short
    else
        docker exec -u airflow airflow bash -c "
            cd /opt/airflow/tests && \
            pytest test_load_raw.py -v -x --tb=short
        "
    fi
    
    print_success "Testes rápidos concluídos"
}

# Executar teste principal de integração
run_main_integration() {
    print_header "Executando Teste Principal de Integração"
    
    export TEST_NEO4J_URI=${TEST_NEO4J_URI:-bolt://neo4j:7687}
    export TEST_NEO4J_USER=${TEST_NEO4J_USER:-neo4j}
    export TEST_NEO4J_PASSWORD=${TEST_NEO4J_PASSWORD:-password}
    
    if is_inside_docker; then
        cd /opt/airflow/tests
        pytest teste_integration.py::TestIntegrationSmallDataset::test_load_small_dataset_into_neo4j -v -s
    else
        docker exec -u airflow \
                    -e TEST_NEO4J_URI="$TEST_NEO4J_URI" \
                    -e TEST_NEO4J_USER="$TEST_NEO4J_USER" \
                    -e TEST_NEO4J_PASSWORD="$TEST_NEO4J_PASSWORD" \
                    airflow bash -c "
            cd /opt/airflow/tests && \
            pytest teste_integration.py::TestIntegrationSmallDataset::test_load_small_dataset_into_neo4j -v -s
        "
    fi
    
    print_success "Teste principal concluído"
}

# Limpar cache e arquivos temporários
clean() {
    print_header "Limpando Cache e Arquivos Temporários"
    
    if is_inside_docker; then
        cd /opt/airflow/tests
        rm -rf .pytest_cache __pycache__ htmlcov .coverage
        find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
        find . -type f -name "*.pyc" -delete
    else
        docker exec -u airflow airflow bash -c "
            cd /opt/airflow/tests && \
            rm -rf .pytest_cache __pycache__ htmlcov .coverage && \
            find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true && \
            find . -type f -name '*.pyc' -delete
        "
    fi
    
    print_success "Limpeza concluída"
}

# Abrir shell no container (apenas quando fora do Docker)
open_shell() {
    if is_inside_docker; then
        print_error "Você já está dentro do container!"
        exit 1
    fi
    
    print_header "Abrindo Shell no Container Airflow"
    echo ""
    print_warning "Dica: Execute 'cd /opt/airflow/tests && pytest test_load_raw.py -v'"
    echo ""
    docker exec -it -u airflow airflow bash
}

# Menu de ajuda
show_help() {
    echo "Uso: ./run_tests.sh [opção]"
    echo ""
    echo "Opções:"
    echo "  all              Executar todos os testes (padrão)"
    echo "  unit             Executar apenas testes unitários"
    echo "  integration      Executar apenas testes de integração"
    echo "  fast             Executar testes rápidos (sem integração)"
    echo "  main             Executar apenas o teste principal de integração"
    echo "  coverage         Executar testes com relatório de cobertura"
    echo "  clean            Limpar cache e arquivos temporários"
    echo "  shell            Abrir shell no container (apenas fora do Docker)"
    echo "  help             Mostrar esta mensagem"
    echo ""
    echo "Exemplos:"
    echo "  ./run_tests.sh unit                # Da sua máquina ou dentro do container"
    echo "  ./run_tests.sh coverage            # Gerar relatório de cobertura"
    echo "  ./run_tests.sh main                # Teste principal de integração"
    echo ""
    echo "Variáveis de ambiente:"
    echo "  TEST_NEO4J_URI       (default: bolt://neo4j:7687)"
    echo "  TEST_NEO4J_USER      (default: neo4j)"
    echo "  TEST_NEO4J_PASSWORD  (default: password)"
}

# Main
main() {
    if is_inside_docker; then
        # Rodando dentro do container
        print_success "Executando testes dentro do container"
        check_dependencies
    else
        # Rodando fora do container
        print_success "Executando testes no container airflow"
        check_container
    fi
    
    case "${1:-all}" in
        all)
            run_all_tests
            ;;
        unit)
            run_unit_tests
            ;;
        integration|int)
            run_integration_tests
            ;;
        fast)
            run_fast_tests
            ;;
        main)
            run_main_integration
            ;;
        coverage|cov)
            run_coverage
            ;;
        clean)
            clean
            ;;
        shell|bash)
            open_shell
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            print_error "Opção inválida: $1"
            show_help
            exit 1
            ;;
    esac
}

main "$@"