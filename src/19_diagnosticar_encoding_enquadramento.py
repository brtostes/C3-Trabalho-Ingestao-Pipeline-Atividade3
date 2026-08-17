from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Atividade3_Diagnostico_Encoding_Enquadramento")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

arquivo = "/workspace/data/input/EnquadramentoInicia_v2.tsv"

print("=" * 100)
print("ATIVIDADE 3 - DIAGNOSTICO DO ENCODING DO ENQUADRAMENTO")
print("=" * 100)

encodings = [
    "UTF-8",
    "ISO-8859-1",
]

cnpjs_teste = [
    "16927221",
    "62931522",
    "517645",
    "33416108",
    "62169875",
    "58506221",
]

for encoding in encodings:

    print("\n" + "=" * 100)
    print(f"ENCODING: {encoding}")
    print("=" * 100)

    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("encoding", encoding)
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(arquivo)
    )

    print(f"Registros: {df.count()}")
    print(f"Colunas  : {len(df.columns)}")

    # Conta linhas contendo o caractere Unicode de substituição.
    qtd_fffd = (
        df
        .filter(
            F.instr(
                F.col("Nome"),
                "\ufffd"
            ) > 0
        )
        .count()
    )

    print(
        f"Linhas com U+FFFD na coluna Nome: "
        f"{qtd_fffd}"
    )

    print("\nExemplos selecionados:")

    linhas = (
        df
        .filter(
            F.col("CNPJ").isin(cnpjs_teste)
        )
        .select(
            "CNPJ",
            "Nome"
        )
        .orderBy("CNPJ")
        .collect()
    )

    for linha in linhas:

        print(
            f"CNPJ={linha['CNPJ']} | "
            f"Nome={ascii(linha['Nome'])}"
        )

print("\n" + "=" * 100)
print("DIAGNOSTICO_ENCODING_ENQUADRAMENTO_OK")
print("=" * 100)

spark.stop()
