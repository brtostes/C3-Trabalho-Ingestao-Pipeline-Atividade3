from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    regexp_replace
)

spark = (
    SparkSession.builder
    .appName("Atividade3_Trusted_Enquadramento")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw = "/workspace/data/raw/enquadramento/enquadramento_inicial"
saida = "/workspace/data/trusted/enquadramento"

print("=" * 100)
print("ATIVIDADE 3 - CRIACAO DA CAMADA TRUSTED - ENQUADRAMENTO")
print("=" * 100)

df = spark.read.parquet(raw)

print(f"Registros RAW: {df.count()}")
print(f"Colunas RAW  : {len(df.columns)}")

print("\nColunas originais:")
for nome in df.columns:
    print(f"- {nome}")

if df.count() != 1474:
    raise RuntimeError(
        f"Quantidade inesperada na RAW: {df.count()}"
    )

if len(df.columns) != 3:
    raise RuntimeError(
        f"Quantidade inesperada de colunas: {len(df.columns)}"
    )

# Padronização dos nomes.
df = df.toDF(
    "segmento",
    "cnpj",
    "nome"
)

# Limpeza de espaços.
for nome_coluna in [
    "segmento",
    "cnpj",
    "nome"
]:

    df = df.withColumn(
        nome_coluna,
        trim(col(nome_coluna))
    )

    df = df.withColumn(
        nome_coluna,
        when(
            col(nome_coluna) == "",
            None
        ).otherwise(col(nome_coluna))
    )

# O CNPJ é um identificador e permanece como STRING.
# Mantemos somente dígitos.
df = df.withColumn(
    "cnpj",
    regexp_replace(
        col("cnpj"),
        r"[^0-9]",
        ""
    )
)

df = df.withColumn(
    "cnpj",
    when(
        col("cnpj") == "",
        None
    ).otherwise(col("cnpj"))
)

total = df.count()

print("\n" + "=" * 100)
print("VALIDACOES")
print("=" * 100)

print(f"Total de registros: {total}")
print("Total esperado     : 1474")

segmentos_nulos = (
    df
    .filter(col("segmento").isNull())
    .count()
)

cnpjs_nulos = (
    df
    .filter(col("cnpj").isNull())
    .count()
)

nomes_nulos = (
    df
    .filter(col("nome").isNull())
    .count()
)

segmentos_distintos = (
    df
    .select("segmento")
    .distinct()
    .count()
)

cnpjs_distintos = (
    df
    .select("cnpj")
    .where(col("cnpj").isNotNull())
    .distinct()
    .count()
)

print(f"Segmentos nulos    : {segmentos_nulos}")
print(f"CNPJs nulos        : {cnpjs_nulos}")
print(f"Nomes nulos        : {nomes_nulos}")
print(f"Segmentos distintos: {segmentos_distintos}")
print(f"CNPJs distintos    : {cnpjs_distintos}")

print("\nSchema:")
df.printSchema()

print("\nAmostra:")
df.show(
    10,
    truncate=False
)

print("\nDistribuicao por segmento:")

(
    df
    .groupBy("segmento")
    .count()
    .orderBy("segmento")
    .show(
        50,
        truncate=False
    )
)

# Persistência em Parquet.
(
    df.write
    .mode("overwrite")
    .parquet(saida)
)

# Validação da gravação.
df_validacao = spark.read.parquet(saida)

gravados = df_validacao.count()

print("\n" + "=" * 100)
print("VALIDACAO DA GRAVACAO")
print("=" * 100)

print(f"Registros antes da gravacao: {total}")
print(f"Registros apos a gravacao : {gravados}")

if gravados != total:
    raise RuntimeError(
        "Divergencia na gravacao da Trusted de Enquadramento."
    )

if gravados != 1474:
    raise RuntimeError(
        f"Total final inesperado: {gravados}"
    )

print("TRUSTED_ENQUADRAMENTO_OK")
print("=" * 100)

spark.stop()
