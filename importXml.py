import argparse
import logging
import sys
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, lit

# Configuração do logging para melhor visualização no console do Spark
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(app_name: str) -> SparkSession:
    """Cria e retorna uma sessão Spark."""
    logging.info(f"Criando sessão Spark com o nome: {app_name}")
    return SparkSession.builder.appName(app_name).getOrCreate()

def process_xml_data(spark: SparkSession, xml_path: str) -> DataFrame:
    """
    Lê um arquivo XML complexo, extrai metadados do cabeçalho,
    achata a estrutura de operações e clientes, e retorna um DataFrame final.

    Args:
        spark: A sessão Spark ativa.
        xml_path: O caminho para o arquivo XML de entrada.

    Returns:
        Um DataFrame do PySpark com os dados processados e achatados.
    """
    logging.info(f"Iniciando processamento do arquivo XML: {xml_path}")

    # 1. Lê os metadados do documento a partir da tag raiz <Doc3040>
    logging.info("Extraindo metadados do cabeçalho do documento...")
    doc_df = spark.read.format("xml").option("rowTag", "Doc3040").load(xml_path)
    doc_attributes = doc_df.select("_DtBase", "_CNPJ", "_Remessa", "_TotalCli").first()
    if not doc_attributes:
        raise ValueError("Não foi possível encontrar a tag <Doc3040> ou seus atributos no XML.")

    # 2. Lê os dados principais, tratando cada tag <Cli> como uma linha
    logging.info("Lendo dados dos clientes e operações...")
    cli_df = spark.read.format("xml").option("rowTag", "Cli").load(xml_path)

    # 3. "Explode" o array de operações para criar uma linha por operação
    df_exploded = cli_df.withColumn("Op", explode(col("Op")))

    # 4. Achata a estrutura, selecionando e renomeando colunas aninhadas
    logging.info("Achatando a estrutura do DataFrame...")
    df_flat = df_exploded.select(
        # Atributos de <Cli>
        col("_Cd").alias("cliente_cd"),
        col("_Tp").alias("cliente_tp"),
        col("_Autorzc").alias("cliente_autorzc"),
        col("_PorteCli").alias("cliente_porte"),
        col("_TpCtrl").alias("cliente_tp_ctrl"),
        col("_IniRelactCli").alias("cliente_ini_relact"),
        col("_CongEcon").alias("cliente_cong_econ"),
        col("_ClassCli").alias("cliente_class_cli"),
        # Atributos de <Op>
        col("Op._Contrt").alias("op_contrt"),
        col("Op._DetCli").alias("op_det_cli"),
        col("Op._NatuOp").alias("op_natu"),
        col("Op._Mod").alias("op_mod"),
        col("Op._OrigemRec").alias("op_origem_rec"),
        col("Op._Indx").alias("op_indx"),
        col("Op._VarCamb").alias("op_var_camb"),
        col("Op._DtVencOp").alias("op_dt_venc"),
        col("Op._ClassOp").alias("op_class"),
        col("Op._CEP").alias("op_cep"),
        col("Op._TaxEft").alias("op_tax_eft"),
        col("Op._DtContr").alias("op_dt_contr"),
        col("Op._ProvConsttd").alias("op_prov_consttd"),
        col("Op._CaracEspecial").alias("op_carac_especial"),
        col("Op._Cosif").alias("op_cosif"),
        col("Op._IPOC").alias("op_ipoc"),
        # Atributos de <Venc>
        col("Op.Venc._v110").alias("venc_v110"),
        col("Op.Venc._v120").alias("venc_v120"),
        col("Op.Venc._v130").alias("venc_v130"),
        col("Op.Venc._v140").alias("venc_v140"),
        col("Op.Venc._v150").alias("venc_v150"),
        # Atributos de <Gar>
        col("Op.Gar._Tp").alias("gar_tp"),
        col("Op.Gar._Ident").alias("gar_ident"),
        col("Op.Gar._PercGar").alias("gar_perc"),
        col("Op.Gar._VlrOrig").alias("gar_vlr_orig"),
        col("Op.Gar._VlrData").alias("gar_vlr_data"),
        col("Op.Gar._DtReav").alias("gar_dt_reav")
    )

    # 5. Adiciona os metadados do documento a cada linha
    df_final = df_flat.withColumn("doc_dt_base", lit(doc_attributes["_DtBase"])) \
                      .withColumn("doc_cnpj", lit(doc_attributes["_CNPJ"])) \
                      .withColumn("doc_remessa", lit(doc_attributes["_Remessa"])) \
                      .withColumn("doc_total_cli", lit(doc_attributes["_TotalCli"]))

    logging.info("Processamento do XML concluído.")
    return df_final

def write_to_postgres(df: DataFrame, url: str, table: str, properties: Dict[str, str]):
    """
    Escreve um DataFrame em uma tabela do PostgreSQL.

    Args:
        df: O DataFrame a ser salvo.
        url: A URL de conexão JDBC do PostgreSQL.
        table: O nome da tabela de destino.
        properties: Um dicionário com propriedades de conexão (usuário, senha, driver).
    """
    logging.info(f"Iniciando escrita na tabela '{table}' do PostgreSQL...")
    df.write.jdbc(url=url, table=table, mode="overwrite", properties=properties)
    logging.info("Escrita no PostgreSQL concluída com sucesso.")

def main():
    """
    Ponto de entrada principal do script.
    Orquestra a leitura dos parâmetros, o processamento do XML e a escrita no banco.
    """
    parser = argparse.ArgumentParser(description="Processa um arquivo XML e o ingere no PostgreSQL.")
    parser.add_argument("--xml-file", required=True, help="Caminho do arquivo XML de entrada.")
    parser.add_argument("--db-url", default="jdbc:postgresql://postgres:5432/xml", help="URL JDBC do PostgreSQL.")
    parser.add_argument("--db-user", default="postgres", help="Usuário do banco de dados.")
    parser.add_argument("--db-password", default="postgres", help="Senha do banco de dados.")
    parser.add_argument("--db-table", default="operacoes_cliente", help="Nome da tabela de destino.")
    args = parser.parse_args()

    spark = None
    try:
        spark = create_spark_session("XML to PostgreSQL Ingestion")

        final_df = process_xml_data(spark, args.xml_file)

        logging.info("Schema final do DataFrame a ser salvo:")
        final_df.printSchema()

        logging.info("Amostra dos dados processados (5 linhas):")
        final_df.show(5, truncate=False)

                      .withColumn("doc_cnpj", lit(doc_attributes["_CNPJ"])) \
                      .withColumn("doc_remessa", lit(doc_attributes["_Remessa"])) \
                      .withColumn("doc_total_cli", lit(doc_attributes["_TotalCli"]))

    print("Schema final do DataFrame a ser salvo:")
    df_final.printSchema()

    print("\nAmostra dos dados processados:")
    df_final.show(5, truncate=False)

    # Define as propriedades de conexão com o PostgreSQL.
    # O hostname 'postgres' é o nome do serviço definido no docker-compose.yml.
    pg_url = "jdbc:postgresql://postgres:5432/xml"
    pg_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    table_name = "operacoes_cliente"

    print(f"\nEscrevendo dados na tabela '{table_name}' do PostgreSQL...")
    # Escreve o DataFrame na tabela. O modo 'overwrite' garante que a tabela
    # seja criada ou substituída a cada execução.
    df_final.write.jdbc(url=pg_url, table=table_name, mode="overwrite", properties=pg_properties)

    print("Dados importados com sucesso!")

    spark.stop()

if __name__ == "__main__":
    main()