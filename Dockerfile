FROM apache/airflow:2.8.4-python3.10

USER root

# Instala dependências do sistema incluindo gosu
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cria os diretórios
RUN mkdir -p /opt/airflow/scripts /opt/airflow/dags /opt/bases_neo4j /opt/airflow/import_raw

# Copia o entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copia e instala requirements como airflow
USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Volta para root para o entrypoint executar
USER root

WORKDIR /opt/airflow
ENTRYPOINT ["/entrypoint.sh"]