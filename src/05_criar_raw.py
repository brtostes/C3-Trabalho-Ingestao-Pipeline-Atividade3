from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Camada_RAW")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

entrada = "/workspace/data/input"
saida = "/workspace/data/raw"

fontes = [
    {
        "arquivo": "2021_tri_01.csv",
        "grupo": "reclamacoes",
        "destino": "2021_tri_01",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "2021_tri_02.csv",
        "grupo": "reclamacoes",
        "destino": "2021_tri_02",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "2021_tri_03.csv",
        "grupo": "reclamacoes",
        "destino": "2021_tri_03",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "2021_tri_04.csv",
        "grupo": "reclamacoes",
        "destino": "2021_tri_04",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "2022_tri_01.csv",
        "grupo": "reclamacoes",
        "destino": "2022_tri_01",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "2022_tri_03.csv",
        "grupo": "reclamacoes",
        "destino": "2022_tri_03",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "2022_tri_04.csv",
        "grupo": "reclamacoes",
        "destino": "2022_tri_04",
        "sep": ";",
        "encoding": "ISO-8859-1",
    },
    {
        "arquivo": "EnquadramentoInicia_v2.tsv",
        "grupo": "enquadramento",
        "destino": "enquadramento_inicial",
        "sep": "\t",
        "encoding": "UTF-8",
    },
    {
        "arquivo": "glassdoor_consolidado_join_match_less_v2.csv",
        "grupo": "glassdoor",
        "destino": "match_less",
        "sep": "|",
        "encoding": "UTF-8",
    },
    {
        "arquivo": "glassdoor_consolidado_join_match_v2.csv",
        "grupo": "glassdoor",
        "destino": "match",
        "sep": "|",
        "encoding": "UTF-8",
    },
]

print("=" * 100)
print("ATIVIDADE 3 - CRIACAO DA CAMADA RAW COM APACHE SPARK")
print("=" * 100)

total_registros = 0

for fonte in fontes:

    caminho_entrada = f"{entrada}/{fonte['arquivo']}"

    caminho_saida = (
        f"{saida}/{fonte['grupo']}/{fonte['destino']}"
    )

    print("\n" + "=" * 100)
    print(f"ARQUIVO: {fonte['arquivo']}")
    print("=" * 100)

    df = (
        spark.read
        .option("header", "true")
        .option("sep", fonte["sep"])
        .option("encoding", fonte["encoding"])
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(caminho_entrada)
    )

    quantidade = df.count()

    total_registros += quantidade

    print(f"Encoding: {fonte['encoding']}")
    print(f"Registros lidos: {quantidade}")
    print(f"Colunas: {len(df.columns)}")

    (
        df.write
        .mode("overwrite")
        .parquet(caminho_saida)
    )

    df_validacao = spark.read.parquet(caminho_saida)

    quantidade_validacao = df_validacao.count()

    print(f"Registros gravados em RAW: {quantidade_validacao}")
    print(f"Destino: {caminho_saida}")

    if quantidade != quantidade_validacao:
        raise RuntimeError(
            f"Falha de integridade em {fonte['arquivo']}: "
            f"entrada={quantidade}, raw={quantidade_validacao}"
        )

    print("VALIDACAO: OK")

print("\n" + "=" * 100)
print(f"TOTAL DE REGISTROS PROCESSADOS: {total_registros}")
print("CAMADA_RAW_CRIADA_COM_SUCESSO")
print("=" * 100)

spark.stop()
