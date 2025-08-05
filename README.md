# Processamento de XML com PySpark e PostgreSQL

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white" alt="PySpark"/>
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
</p>

Este projeto demonstra um pipeline de dados para ler, processar e armazenar informações de arquivos XML complexos. Ele utiliza um ambiente conteinerizado com Docker para garantir a reprodutibilidade e facilitar a configuração.

## Objetivo

O objetivo deste repositório é servir como um caso de uso para o processamento de arquivos XML que seguem o padrão **Documento 3040** do Sistema de Informações de Crédito (SCR), definido pelo Banco Central do Brasil.

Toda a documentação técnica, layouts e arquivos de exemplo podem ser encontrados no site oficial:

*   **Fonte:** [SCR - Documento 3040](https://www.bcb.gov.br/estabilidadefinanceira/scrdoc3040)

O fluxo de trabalho principal é:
1.  Um script **PySpark** (`importXml.py`) é executado em um contêiner Spark.
2.  O script lê um arquivo XML (`exemploDocPadraoInfosBasicas.xml`) com uma estrutura aninhada.
3.  Os dados são transformados e "achatados" (flattened) em um formato tabular.
4.  O DataFrame resultante é salvo em uma tabela em um banco de dados **PostgreSQL**, que também está rodando em um contêiner.

## Pré-requisitos

Antes de começar, certifique-se de ter os seguintes softwares instalados em sua máquina:
*   Docker
*   Docker Compose

## Estrutura do Projeto

```
.
├── db/
│   └── Dockerfile             # Dockerfile para o serviço PostgreSQL
├── infra/
│   ├── Dockerfile             # Dockerfile para o ambiente Spark com Python
│   └── README.md
├── docker-compose.yml         # Orquestra os contêineres de Spark e PostgreSQL
├── exemploDocPadraoInfosBasicas.xml # Arquivo XML de exemplo para ingestão
├── run.sh                     # Script para automatizar a execução do job
├── importXml.py               # Script PySpark principal para o processamento
└── README.md                  # Este arquivo de instruções
```

## Como Executar

Siga os passos abaixo para configurar o ambiente e executar o processo de ingestão de dados.

### 1. Clone o Repositório
```bash
git clone https://github.com/AleTavares/spark_xml.git
```
### 2. Inicie os Serviços em Segundo Plano

Use o Docker Compose para construir as imagens e iniciar os contêineres do Spark e do PostgreSQL em modo "detached" (segundo plano).

```bash
docker-compose up --build -d
```
*   `--build`: Garante que as imagens Docker sejam construídas a partir dos Dockerfiles na primeira vez.
*   `-d`: Executa os contêineres em modo "detached" (em segundo plano).

### 3. Execute o Script de Ingestão

Para executar o job de ingestão, utilize o script `run.sh` na raiz do projeto. Ele automatiza a chamada ao `spark-submit` dentro do contêiner Docker.

**a. Dê permissão de execução ao script (apenas na primeira vez):**
   ```bash
   chmod +x run.sh
   ```

**b. Execute o script:**

Para uma execução padrão, que processa o arquivo de exemplo e usa as configurações padrão de memória, basta rodar:
   ```bash
   ./run.sh
   ```

#### Execução Avançada (Customizando Parâmetros)

Para processar arquivos maiores ou para se conectar a um banco de dados diferente, você pode customizar a execução passando variáveis de ambiente para o script `run.sh`.

**Exemplo:**
```bash
DRIVER_MEMORY="2g" \
EXECUTOR_MEMORY="4g" \
NUM_EXECUTORS="4" \
XML_FILE="/app/outro_arquivo.xml" \
DB_TABLE="minha_tabela" \
./run.sh
```

**Parâmetros de Otimização do Spark:**
*   `DRIVER_MEMORY`: Memória para o processo *driver* (Ex: `1g`).
*   `EXECUTOR_MEMORY`: Memória para cada *worker* (executor) (Ex: `2g`).
*   `NUM_EXECUTORS`: Número de *workers* (executors) a serem alocados.

**Parâmetros da Aplicação:**
*   `XML_FILE`: Caminho do arquivo XML de entrada dentro do contêiner.
*   `DB_URL`: URL de conexão JDBC do PostgreSQL.
*   `DB_USER`: Usuário do banco de dados.
*   `DB_PASSWORD`: Senha do banco de dados.
*   `DB_TABLE`: Nome da tabela de destino.

## Verificando o Resultado

Após a execução bem-sucedida do script, os dados do XML estarão na tabela `operacoes_cliente` no banco de dados PostgreSQL.

Você pode se conectar ao banco de dados usando seu cliente SQL preferido (DBeaver, DataGrip, pgAdmin, etc.) com as seguintes credenciais:

*   **Host**: `localhost`
*   **Porta**: `3000` (mapeada no `docker-compose.yml`)
*   **Banco de Dados**: `xml`
*   **Usuário**: `postgres`
*   **Senha**: `postgres`

Execute uma consulta para visualizar os dados importados:

```sql
SELECT * FROM operacoes_cliente LIMIT 10;
```

## Encerrando o Ambiente

Quando terminar de usar o ambiente, você pode parar e remover os contêineres com o seguinte comando, executado na raiz do projeto:

```bash
docker-compose down
```
Este comando irá parar os contêineres `spark-dev` e `postgres`. Para remover também os volumes (e apagar os dados do banco), adicione a flag `-v`: `docker-compose down -v`.