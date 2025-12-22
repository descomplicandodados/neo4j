#!/bin/bash
set -e

# ========================================
# AJUSTA PERMISSÕES COMO ROOT
# ========================================
echo "🔧 Ajustando permissões..."

# Executa comandos de permissão como root
if [ "$(id -u)" != "0" ]; then
    echo "⚠️  Aviso: entrypoint não está rodando como root"
else
    chmod -R 777 /opt/airflow/scripts 2>/dev/null || true
    chmod -R 777 /opt/airflow/dags 2>/dev/null || true
    chmod -R 777 /opt/airflow/import_raw 2>/dev/null || true
    chmod -R 777 /opt/bases_neo4j 2>/dev/null || true
    chown -R airflow:root /opt/airflow/scripts 2>/dev/null || true
    chown -R airflow:root /opt/airflow/dags 2>/dev/null || true
    chown -R airflow:root /opt/airflow/import_raw 2>/dev/null || true
    chown -R airflow:root /opt/bases_neo4j 2>/dev/null || true
    echo "✅ Permissões ajustadas"
fi

# ========================================
# MUDA PARA USUÁRIO AIRFLOW
# ========================================
if [ "$(id -u)" = "0" ]; then
    echo "🔄 Mudando para usuário airflow..."
    exec gosu airflow "$0" "$@"
fi

# ========================================
# INICIALIZAÇÃO DO AIRFLOW (como airflow)
# ========================================
echo "🚀 Iniciando Airflow como usuário $(whoami)..."

# Inicializa o banco de dados do Airflow se necessário
if [ ! -f "/opt/airflow/airflow.db" ]; then
    echo "📊 Inicializando banco de dados do Airflow..."
    airflow db init
    
    # Cria o usuário admin
    echo "👤 Criando usuário admin..."
    airflow users create \
        --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
        --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}" \
        --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-User}" \
        --role Admin \
        --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" \
        --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}"
fi

# Atualiza o banco se necessário
echo "🔄 Atualizando banco de dados..."
airflow db upgrade

# Inicia o scheduler em background
echo "📅 Iniciando Scheduler..."
airflow scheduler &

# Inicia o webserver
echo "🌐 Iniciando Webserver..."
echo "✅ Airflow disponível em http://localhost:8080"
exec airflow webserver