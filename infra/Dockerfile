# Usamos a imagem oficial do Bitnami para Spark, que já inclui Java, Python e o próprio Spark.
FROM bitnami/spark:3.3.0-debian-11-r21

# Mudar para o usuário root para instalar pacotes
USER root

# Define o diretório de trabalho padrão dentro do contêiner
WORKDIR /app

# Copia o arquivo de dependências Python para o container.
# O contexto de build é a raiz do projeto, então o caminho está correto.
COPY requirements.txt .

# Instala as dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Retorna para o usuário não-root padrão do Spark para maior segurança
USER 1001