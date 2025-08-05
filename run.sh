#!/bin/bash

# ==============================================================================
# Script para executar o job PySpark de ingestão de XML.
#
# Este script automatiza a execução do spark-submit, permitindo a configuração
# de recursos do Spark e parâmetros da aplicação através de variáveis de
# ambiente.
#
# Uso:
#   ./run.sh
#
# Ou com variáveis customizadas:
#   XML_FILE="/path/to/another.xml" DB_USER="myuser" ./run.sh
# ==============================================================================

# --- Configurações do Spark (podem ser sobrescritas por variáveis de ambiente) ---

# Define a memória para o processo Driver (o "cérebro" da aplicação Spark).
# O Driver planeja e coordena a execução das tarefas.
# Aumente este valor se o seu script manipular muitos metadados ou se você
# precisar coletar um grande volume de dados para o driver (ação .collect()).
DRIVER_MEMORY=${DRIVER_MEMORY:-"1g"}

# Define a memória para cada processo Executor (os "trabalhadores" que processam os dados).
# Este é um dos parâmetros mais críticos para o desempenho. Aumente-o se ocorrerem
# erros de OutOfMemoryError nos logs dos workers.
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-"2g"}

# Define o número de processos Executor a serem alocados para o job.
# Aumentar este valor aumenta o paralelismo e pode acelerar significativamente o processamento,
# desde que o cluster tenha recursos (CPU/memória) disponíveis.
NUM_EXECUTORS=${NUM_EXECUTORS:-"2"}

SPARK_PACKAGES="com.databricks:spark-xml_2.12:0.17.0,org.postgresql:postgresql:42.5.0"

# --- Parâmetros da Aplicação (podem ser sobrescritas por variáveis de ambiente) ---
XML_FILE=${XML_FILE:-"/app/exemploDocPadraoInfosBasicas.xml"}
DB_URL=${DB_URL:-"jdbc:postgresql://postgres:5432/xml"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"postgres"}
DB_TABLE=${DB_TABLE:-"operacoes_cliente"}
APP_SCRIPT="/app/importXml.py"

# --- Validação ---
if [ ! -f "$XML_FILE" ]; then
    echo "Erro: O arquivo XML '$XML_FILE' não foi encontrado."
    echo "Verifique o caminho ou defina a variável de ambiente XML_FILE."
    exit 1
fi

echo "================================================="
echo "  Iniciando Job Spark de Ingestão de XML"
echo "================================================="
echo "  Driver Memory:    ${DRIVER_MEMORY}"
echo "  Executor Memory:  ${EXECUTOR_MEMORY}"
echo "  Num Executors:    ${NUM_EXECUTORS}"
echo "-------------------------------------------------"
echo "  Arquivo XML:      ${XML_FILE}"
echo "  Tabela Destino:   ${DB_TABLE}"
echo "================================================="

# --- Execução do spark-submit ---
spark-submit \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --num-executors "${NUM_EXECUTORS}" \
  --packages "${SPARK_PACKAGES}" \
  "${APP_SCRIPT}" \
    --xml-file "${XML_FILE}" \
    --db-url "${DB_URL}" \
    --db-user "${DB_USER}" \
    --db-password "${DB_PASSWORD}" \
    --db-table "${DB_TABLE}"

EXIT_CODE=$?

if [ ${EXIT_CODE} -eq 0 ]; then
  echo -e "\nJob Spark concluído com sucesso!"
else
  echo -e "\nErro ao executar o Job Spark (Código de saída: ${EXIT_CODE})"
fi

exit ${EXIT_CODE}