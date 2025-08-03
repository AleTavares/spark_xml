# Processamento de XML com PySpark e PostgreSQL

Este projeto demonstra um pipeline de dados para ler, processar e armazenar informações de arquivos XML complexos. Ele utiliza um ambiente conteinerizado com Docker para garantir a reprodutibilidade e facilitar a configuração.

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
├── importXml.py               # Script PySpark principal para o processamento
└── README.md                  # Este arquivo de instruções
```

## Como Executar

Siga os passos abaixo para configurar o ambiente e executar o processo de ingestão de dados.

### 1. Clone o Repositório

### 2. Inicie os Serviços em Segundo Plano

Use o Docker Compose para construir as imagens e iniciar os contêineres do Spark e do PostgreSQL em modo "detached" (segundo plano).

```bash
docker-compose up --build -d
```
*   `--build`: Garante que as imagens Docker sejam construídas a partir dos Dockerfiles na primeira vez.
*   `-d`: Executa os contêineres em modo "detached" (em segundo plano).

### 3. Execute o Script de Ingestão
 
Com os contêineres rodando, vamos executar o script PySpark manualmente.

**a. Acesse o terminal do contêiner Spark:**

```bash
docker-compose exec spark-dev bash
```

**b. Execute o script com `spark-submit`:**

Dentro do terminal do contêiner, execute o seguinte comando. Ele instrui o Spark a baixar os pacotes necessários para ler XML e para se conectar ao PostgreSQL antes de executar o script.

```bash
spark-submit \
  --packages com.databricks:spark-xml_2.12:0.17.0,org.postgresql:postgresql:42.5.0 \
  /app/importXml.py
```

O script irá iniciar uma sessão Spark, baixar as dependências necessárias (driver do PostgreSQL e biblioteca `spark-xml`), processar o arquivo e salvar os dados. Você verá no terminal o schema do DataFrame final e uma amostra dos dados processados.

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