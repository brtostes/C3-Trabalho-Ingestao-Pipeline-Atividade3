from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    when,
    regexp_replace
)

spark = (
    SparkSession.builder
    .appName("Atividade3_Trusted_Glassdoor")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_match = "/workspace/data/raw/glassdoor/match"
raw_match_less = "/workspace/data/raw/glassdoor/match_less"

trusted_match = "/workspace/data/trusted/glassdoor/match"
trusted_match_less = "/workspace/data/trusted/glassdoor/match_less"

colunas_comuns = [
    "employer_name",
    "reviews_count",
    "culture_count",
    "salaries_count",
    "benefits_count",
    "employer_website",
    "employer_headquarters",
    "employer_founded",
    "employer_industry",
    "employer_revenue",
    "url",
    "geral",
    "cultura_valores",
    "diversidade_inclusao",
    "qualidade_vida",
    "alta_lideranca",
    "remuneracao_beneficios",
    "oportunidades_carreira",
    "recomendam_percentual",
    "perspectiva_positiva_percentual",
]

colunas_inteiras = [
    "reviews_count",
    "culture_count",
    "salaries_count",
    "benefits_count",
]

colunas_decimais = [
    "employer_founded",
    "geral",
    "cultura_valores",
    "diversidade_inclusao",
    "qualidade_vida",
    "alta_lideranca",
    "remuneracao_beneficios",
    "oportunidades_carreira",
    "recomendam_percentual",
    "perspectiva_positiva_percentual",
    "match_percent",
]

def limpar_textos(df):

    for campo in df.columns:

        df = df.withColumn(
            campo,
            trim(col(campo))
        )

        df = df.withColumn(
            campo,
            when(
                col(campo) == "",
                None
            ).otherwise(col(campo))
        )

    return df


def tipar_metricas(df):

    for campo in colunas_inteiras:

        df = df.withColumn(
            campo,
            col(campo).cast("long")
        )

    for campo in colunas_decimais:

        if campo in df.columns:

            df = df.withColumn(
                campo,
                col(campo).cast("double")
            )

    return df


print("=" * 100)
print("ATIVIDADE 3 - CRIACAO DA CAMADA TRUSTED - GLASSDOOR")
print("=" * 100)

# =============================================================================
# GLASSDOOR MATCH
# =============================================================================

print("\n" + "=" * 100)
print("DATASET: GLASSDOOR MATCH")
print("=" * 100)

df_match = spark.read.parquet(raw_match)

print(f"Registros RAW: {df_match.count()}")
print(f"Colunas RAW  : {len(df_match.columns)}")

if df_match.count() != 34:
    raise RuntimeError(
        f"Quantidade inesperada em match: {df_match.count()}"
    )

if len(df_match.columns) != 23:
    raise RuntimeError(
        f"Quantidade inesperada de colunas em match: "
        f"{len(df_match.columns)}"
    )

df_match = df_match.toDF(
    *colunas_comuns,
    "segmento",
    "nome",
    "match_percent"
)

df_match = limpar_textos(df_match)
df_match = tipar_metricas(df_match)

df_match = df_match.withColumn(
    "arquivo_origem",
    lit("glassdoor_consolidado_join_match_v2.csv")
)

print(f"Registros preparados: {df_match.count()}")

print("\nSchema MATCH:")
df_match.printSchema()

print("\nAmostra MATCH:")

df_match.select(
    "employer_name",
    "geral",
    "segmento",
    "nome",
    "match_percent",
    "arquivo_origem"
).show(
    10,
    truncate=False
)

(
    df_match.write
    .mode("overwrite")
    .parquet(trusted_match)
)

validacao_match = spark.read.parquet(
    trusted_match
).count()

print(
    f"Registros gravados MATCH: {validacao_match}"
)

if validacao_match != 34:
    raise RuntimeError(
        "Falha na persistencia do Glassdoor Match."
    )

# =============================================================================
# GLASSDOOR MATCH LESS
# =============================================================================

print("\n" + "=" * 100)
print("DATASET: GLASSDOOR MATCH LESS")
print("=" * 100)

df_less = spark.read.parquet(raw_match_less)

print(f"Registros RAW: {df_less.count()}")
print(f"Colunas RAW  : {len(df_less.columns)}")

if df_less.count() != 5:
    raise RuntimeError(
        f"Quantidade inesperada em match_less: {df_less.count()}"
    )

if len(df_less.columns) != 23:
    raise RuntimeError(
        f"Quantidade inesperada de colunas em match_less: "
        f"{len(df_less.columns)}"
    )

df_less = df_less.toDF(
    *colunas_comuns,
    "cnpj",
    "nome",
    "match_percent"
)

df_less = limpar_textos(df_less)

# CNPJ permanece identificador STRING.
df_less = df_less.withColumn(
    "cnpj",
    regexp_replace(
        col("cnpj"),
        r"[^0-9]",
        ""
    )
)

df_less = df_less.withColumn(
    "cnpj",
    when(
        col("cnpj") == "",
        None
    ).otherwise(col("cnpj"))
)

df_less = tipar_metricas(df_less)

df_less = df_less.withColumn(
    "arquivo_origem",
    lit(
        "glassdoor_consolidado_join_match_less_v2.csv"
    )
)

print(f"Registros preparados: {df_less.count()}")

print("\nSchema MATCH LESS:")
df_less.printSchema()

print("\nAmostra MATCH LESS:")

df_less.select(
    "employer_name",
    "geral",
    "cnpj",
    "nome",
    "match_percent",
    "arquivo_origem"
).show(
    10,
    truncate=False
)

(
    df_less.write
    .mode("overwrite")
    .parquet(trusted_match_less)
)

validacao_less = spark.read.parquet(
    trusted_match_less
).count()

print(
    f"Registros gravados MATCH LESS: {validacao_less}"
)

if validacao_less != 5:
    raise RuntimeError(
        "Falha na persistencia do Glassdoor Match Less."
    )

print("\n" + "=" * 100)
print("RESUMO")
print("=" * 100)

print(f"Glassdoor Match     : {validacao_match}")
print(f"Glassdoor Match Less: {validacao_less}")
print(
    f"Total Glassdoor     : "
    f"{validacao_match + validacao_less}"
)

print("TRUSTED_GLASSDOOR_OK")
print("=" * 100)

spark.stop()
