FROM apache/airflow:2.8.1

USER root

# Instalar dependências do sistema (se necessário)
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

USER airflow

# Copiar requirements
COPY requirements/airflow.txt /requirements.txt

# Instalar dependências
RUN pip install --no-cache-dir -r /requirements.txt
