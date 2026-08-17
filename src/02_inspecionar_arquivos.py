from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Atividade3_Inspecao_Arquivos")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

arquivos = [
    "2021_tri_01.csv",
    "2021_tri_02.csv",
    "2021_tri_03.csv",
    "2021_tri_04.csv",
    "2022_tri_01.csv",
    "2022_tri_03.csv",
    "2022_tri_04.csv",
    "EnquadramentoInicia_v2.tsv",
    "glassdoor_consolidado_join_match_less_v2.csv",
    "glassdoor_consolidado_join_match_v2.csv",
]

diretorio = "/workspace/data/input"

print("=" * 100)
print("ATIVIDADE 3 - INSPECAO DOS ARQUIVOS DE ENTRADA COM APACHE SPARK")
print("=" * 100)

for nome_arquivo in arquivos:

    caminho = f"{diretorio}/{nome_arquivo}"

    print("\n" + "=" * 100)
    print(f"ARQUIVO: {nome_arquivo}")
    print("=" * 100)

    df_texto = spark.read.text(caminho)

    quantidade_linhas = df_texto.count()

    print(f"Quantidade de linhas fisicas: {quantidade_linhas}")

    print("\nPrimeiras 3 linhas:")
    df_texto.select(col("value")).show(
        3,
        truncate=False
    )

print("\n" + "=" * 100)
print("INSPECAO_SPARK_OK")
print("=" * 100)

spark.stop()
