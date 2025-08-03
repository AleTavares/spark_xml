FROM postgres:16

# Defina variáveis de ambiente padrão (pode ser sobrescrito no docker-compose ou na linha de comando)
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=meubanco

# (Opcional) Copie scripts de inicialização, se necessário
# COPY ./init.sql /docker-entrypoint-initdb.d/

# Exponha a porta padrão do Postgres
EXPOSE 5432