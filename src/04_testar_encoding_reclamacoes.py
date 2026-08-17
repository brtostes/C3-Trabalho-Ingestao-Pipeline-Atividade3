from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Teste_Encoding_Reclamacoes")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

caminho = "/workspace/data/input/2021_tri_01.csv"

encodings = [
    "ISO-8859-1",
    "windows-1252"
]

print("=" * 100)
print("ATIVIDADE 3 - TESTE DE ENCODING DOS ARQUIVOS DE RECLAMACOES")
print("=" * 100)

for encoding in encodings:

    print("\n" + "=" * 100)
    print(f"ENCODING TESTADO: {encoding}")
    print("=" * 100)

    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", encoding)
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(caminho)
    )

    print(f"Registros: {df.count()}")
    print(f"Colunas: {len(df.columns)}")

    print("\nNomes das colunas - Unicode seguro:")

    for numero, coluna in enumerate(df.columns, start=1):

        seguro = (
            coluna
            .encode("unicode_escape")
            .decode("ascii")
        )

        print(f"{numero:02d}: {seguro}")

    primeira = df.first()

    print("\nPrimeira linha - Unicode seguro:")

    for indice in range(min(7, len(df.columns))):

        coluna = (
            df.columns[indice]
            .encode("unicode_escape")
            .decode("ascii")
        )

        valor = primeira[indice]

        if valor is None:
            valor_seguro = "NULL"
        else:
            valor_seguro = (
                str(valor)
                .encode("unicode_escape")
                .decode("ascii")
            )

        print(f"{coluna} = {valor_seguro}")

print("\n" + "=" * 100)
print("TESTE_ENCODING_FINALIZADO")
print("=" * 100)

spark.stop()
