from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    when,
    regexp_replace
)
from functools import reduce

spark = (
    SparkSession.builder
    .appName("Atividade3_Diagnostico_Indice_Reclamacoes")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_base = "/workspace/data/raw/reclamacoes"

fontes = [
    ("2021_tri_01", "2021_tri_01.csv"),
    ("2021_tri_02", "2021_tri_02.csv"),
    ("2021_tri_03", "2021_tri_03.csv"),
    ("2021_tri_04", "2021_tri_04.csv"),
    ("2022_tri_01", "2022_tri_01.csv"),
    ("2022_tri_03", "2022_tri_03.csv"),
    ("2022_tri_04", "2022_tri_04.csv"),
]

colunas = [
    "ano",
    "trimestre",
    "categoria",
    "tipo",
    "cnpj_if",
    "instituicao_financeira",
    "indice",
    "qtd_reclamacoes_reguladas_procedentes",
    "qtd_reclamacoes_reguladas_outras",
    "qtd_reclamacoes_nao_reguladas",
    "qtd_total_reclamacoes",
    "qtd_total_clientes_ccs_scr",
    "qtd_clientes_ccs",
    "qtd_clientes_scr",
    "_c14",
]

dataframes = []

for pasta, arquivo in fontes:

    df = spark.read.parquet(
        f"{raw_base}/{pasta}"
    )

    df = df.toDF(*colunas)

    df = (
        df
        .select(
            trim(col("indice")).alias("indice_original"),
            col("instituicao_financeira"),
            lit(arquivo).alias("arquivo_origem")
        )
    )

    dataframes.append(df)

df = reduce(
    lambda a, b: a.unionByName(b),
    dataframes
)

# Converte string vazia para NULL apenas para diagnóstico.
df = df.withColumn(
    "indice_original",
    when(
        col("indice_original") == "",
        None
    ).otherwise(col("indice_original"))
)

# Classificação do formato observado.
df = df.withColumn(
    "classe_formato",
    when(
        col("indice_original").isNull(),
        "NULO"
    )
    .when(
        col("indice_original").rlike(
            r"^-?\d{1,3}(\.\d{3})+,\d+$"
        ),
        "MILHAR_PONTO_DECIMAL_VIRGULA"
    )
    .when(
        col("indice_original").rlike(
            r"^-?\d+,\d+$"
        ),
        "DECIMAL_VIRGULA"
    )
    .when(
        col("indice_original").rlike(
            r"^-?\d+\.\d+$"
        ),
        "DECIMAL_PONTO"
    )
    .when(
        col("indice_original").rlike(
            r"^-?\d+$"
        ),
        "INTEIRO"
    )
    .otherwise(
        "OUTRO_FORMATO"
    )
)

print("=" * 100)
print("ATIVIDADE 3 - DIAGNOSTICO DO CAMPO INDICE")
print("=" * 100)

print("\nDistribuicao dos formatos:")

df.groupBy(
    "classe_formato"
).count().orderBy(
    "classe_formato"
).show(
    truncate=False
)

print("\nValores que utilizam ponto de milhar e virgula decimal:")

(
    df
    .filter(
        col("classe_formato")
        == "MILHAR_PONTO_DECIMAL_VIRGULA"
    )
    .select(
        "indice_original",
        "instituicao_financeira",
        "arquivo_origem"
    )
    .distinct()
    .orderBy(
        "indice_original"
    )
    .show(
        100,
        truncate=False
    )
)

print("\nValores classificados como OUTRO_FORMATO:")

(
    df
    .filter(
        col("classe_formato")
        == "OUTRO_FORMATO"
    )
    .select(
        "indice_original",
        "instituicao_financeira",
        "arquivo_origem"
    )
    .distinct()
    .show(
        100,
        truncate=False
    )
)

# Teste da regra de normalizacao, sem converter para double.
df = df.withColumn(
    "indice_normalizado",
    when(
        col("indice_original").isNull(),
        None
    )
    .when(
        col("indice_original").contains(","),
        regexp_replace(
            regexp_replace(
                col("indice_original"),
                r"\.",
                ""
            ),
            ",",
            "."
        )
    )
    .otherwise(
        col("indice_original")
    )
)

invalidos_apos_normalizacao = (
    df
    .filter(
        col("indice_normalizado").isNotNull()
        & ~col("indice_normalizado").rlike(
            r"^-?\d+(\.\d+)?$"
        )
    )
    .count()
)

print("\nValidacao da regra proposta:")
print(
    "Valores invalidos apos normalizacao:",
    invalidos_apos_normalizacao
)

print("\nExemplos da normalizacao:")

(
    df
    .filter(
        col("indice_original").isNotNull()
    )
    .select(
        "indice_original",
        "indice_normalizado"
    )
    .distinct()
    .orderBy(
        "indice_original"
    )
    .show(
        50,
        truncate=False
    )
)

print("=" * 100)
print("DIAGNOSTICO_INDICE_OK")
print("=" * 100)

spark.stop()
