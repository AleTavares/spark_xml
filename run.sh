#!/bin/bash

# ==============================================================================
# Script para executar o job PySpark de ingestão de XML via Docker Compose.
#
# Agora este script executa o spark-submit DENTRO do contêiner Spark usando
# docker-compose exec, permitindo rodar o script a partir do host (fora do container).
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

echo "================================================="
echo "  Iniciando Job Spark de Ingestão de XML (via Docker Compose)"
echo "================================================="
echo "  Driver Memory:    ${DRIVER_MEMORY}"
echo "  Executor Memory:  ${EXECUTOR_MEMORY}"
echo "  Num Executors:    ${NUM_EXECUTORS}"
echo "-------------------------------------------------"
echo "  Arquivo XML:      ${XML_FILE}"
echo "  Tabela Destino:   ${DB_TABLE}"
echo "================================================="

# --- Executa o comando spark-submit diretamente dentro do container spark-dev ---
# Esta abordagem é mais robusta do que construir uma string de comando e passá-la
# para 'bash -c', pois evita problemas de quoting e word splitting.
# A flag -T desabilita a alocação de um pseudo-TTY, o que é recomendado para
# execuções não interativas e resolve problemas de parsing de argumentos.
docker-compose exec -T spark-dev \
  spark-submit \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --num-executors "${NUM_EXECUTORS}" \
  --packages "${SPARK_PACKAGES}" \
  "${APP_SCRIPT}" \
  --input-path "${XML_FILE}" \
  --db-host "postgres" \
  --db-port "5432" \
  --db-name "xml" \
  --db-user "${DB_USER}" \
  --db-password "${DB_PASSWORD}" \
  --table-name "${DB_TABLE}"
EXIT_CODE=$?

if [ ${EXIT_CODE} -eq 0 ]; then
  echo -e "\nJob Spark concluído com sucesso!"
else
  echo -e "\nErro ao executar o Job Spark (Código de saída: ${EXIT_CODE})"
fi