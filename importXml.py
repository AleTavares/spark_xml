from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit

def main():
    """
    Script PySpark para ler um arquivo XML complexo, achatar sua estrutura
    e salvar os dados em uma tabela PostgreSQL.
    """
    spark = SparkSession.builder \
        .appName("XML to PostgreSQL Ingestion") \
        .getOrCreate()

    xml_file_path = "/app/exemploDocPadraoInfosBasicas.xml"

    # Lê o arquivo uma vez focando na tag raiz para obter os metadados do documento.
    doc_df = spark.read.format("xml") \
        .option("rowTag", "Doc3040") \
        .load(xml_file_path)

    # Capturamos os atributos da primeira (e única) linha.
    doc_attributes = doc_df.select("_DtBase", "_CNPJ", "_Remessa", "_TotalCli").first()

    # Lê o XML novamente, agora tratando cada tag <Cli> como uma linha.
    # As tags <Op> dentro de cada <Cli> serão carregadas como um array de structs.
    df = spark.read.format("xml") \
        .option("rowTag", "Cli") \
        .load(xml_file_path)

    # Usamos a função `explode` para criar uma nova linha para cada operação no array 'Op'.
    # Isso transforma a relação um-para-muitos (cliente-operações) em um formato tabular.
    df_exploded = df.withColumn("Op", explode(col("Op")))

    # Selecionamos e achatamos todos os atributos aninhados.
    # O prefixo '_' é adicionado por padrão pela biblioteca spark-xml.
    # Renomeia as colunas para nomes mais claros e sem o prefixo.
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

        # Atributos de <Venc> (aninhado em <Op>)
        col("Op.Venc._v110").alias("venc_v110"),
        col("Op.Venc._v120").alias("venc_v120"),
        col("Op.Venc._v130").alias("venc_v130"),
        col("Op.Venc._v140").alias("venc_v140"),
        col("Op.Venc._v150").alias("venc_v150"),

        # Atributos de <Gar> (aninhado em <Op>, pode não existir para todas as operações)
        col("Op.Gar._Tp").alias("gar_tp"),
        col("Op.Gar._Ident").alias("gar_ident"),
        col("Op.Gar._PercGar").alias("gar_perc"),
        col("Op.Gar._VlrOrig").alias("gar_vlr_orig"),
        col("Op.Gar._VlrData").alias("gar_vlr_data"),
        col("Op.Gar._DtReav").alias("gar_dt_reav")
    )

    # Adicionamos os metadados do <Doc3040> como colunas em todas as linhas.
    df_final = df_flat.withColumn("doc_dt_base", lit(doc_attributes["_DtBase"])) \
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