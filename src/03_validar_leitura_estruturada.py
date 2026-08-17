from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Validacao_Leitura_Estruturada")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

fontes = [
    ("2021_tri_01.csv", ";"),
    ("2021_tri_02.csv", ";"),
    ("2021_tri_03.csv", ";"),
    ("2021_tri_04.csv", ";"),
    ("2022_tri_01.csv", ";"),
    ("2022_tri_03.csv", ";"),
    ("2022_tri_04.csv", ";"),
    ("EnquadramentoInicia_v2.tsv", "\t"),
    ("glassdoor_consolidado_join_match_less_v2.csv", "|"),
    ("glassdoor_consolidado_join_match_v2.csv", "|"),
]

diretorio = "/workspace/data/input"

print("=" * 100)
print("ATIVIDADE 3 - VALIDACAO DA LEITURA ESTRUTURADA")
print("=" * 100)

for nome_arquivo, separador in fontes:

    caminho = f"{diretorio}/{nome_arquivo}"

    df = (
        spark.read
        .option("header", "true")
        .option("sep", separador)
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(caminho)
    )

    quantidade = df.count()

    print("\n" + "=" * 100)
    print(f"ARQUIVO: {nome_arquivo}")
    print("=" * 100)

    print(f"Quantidade de registros: {quantidade}")
    print(f"Quantidade de colunas: {len(df.columns)}")

    print("\nNomes das colunas - representacao Unicode segura:")

    for numero, coluna in enumerate(df.columns, start=1):
        coluna_segura = coluna.encode(
            "unicode_escape"
        ).decode("ascii")

        print(f"{numero:02d}: {coluna_segura}")

    if quantidade > 0 and len(df.columns) > 0:

        primeira_linha = df.first()

        print("\nExemplo da primeira linha - Unicode seguro:")

        limite = min(5, len(df.columns))

        for indice in range(limite):

            nome_coluna = (
                df.columns[indice]
                .encode("unicode_escape")
                .decode("ascii")
            )

            valor = primeira_linha[indice]

            if valor is None:
                valor_seguro = "NULL"
            else:
                valor_seguro = (
                    str(valor)
                    .encode("unicode_escape")
                    .decode("ascii")
                )

            print(
                f"{nome_coluna} = {valor_seguro}"
            )

print("\n" + "=" * 100)
print("VALIDACAO_LEITURA_ESTRUTURADA_OK")
print("=" * 100)

spark.stop()
