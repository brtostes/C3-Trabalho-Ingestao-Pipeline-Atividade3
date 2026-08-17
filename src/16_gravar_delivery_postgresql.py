from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Delivery_PostgreSQL")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# CONFIGURACOES
# =============================================================================

delivery_path = (
    "/workspace/data/delivery/"
    "reclamacoes_enriquecidas"
)

jdbc_url = (
    "jdbc:postgresql://postgres:5432/atividade3"
)

tabela_destino = (
    "public.delivery_reclamacoes_enriquecidas"
)

propriedades = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# =============================================================================
# LEITURA DA DELIVERY PARQUET
# =============================================================================

print("=" * 100)
print("ATIVIDADE 3 - GRAVACAO DA DELIVERY NO POSTGRESQL")
print("=" * 100)

delivery = spark.read.parquet(
    delivery_path
)

total_parquet = delivery.count()

print("\nDelivery Parquet:")
print(f"Registros encontrados: {total_parquet}")
print(f"Quantidade de colunas: {len(delivery.columns)}")

if total_parquet != 918:
    raise RuntimeError(
        f"Quantidade inesperada no Parquet: {total_parquet}"
    )

# =============================================================================
# GRAVACAO JDBC
# =============================================================================

print("\n" + "=" * 100)
print("GRAVACAO JDBC")
print("=" * 100)

print(f"Banco  : atividade3")
print(f"Tabela : {tabela_destino}")

(
    delivery.write
    .mode("overwrite")
    .jdbc(
        url=jdbc_url,
        table=tabela_destino,
        properties=propriedades
    )
)

print("Gravacao JDBC concluida.")

# =============================================================================
# RELEITURA PELO SPARK
# =============================================================================

print("\n" + "=" * 100)
print("VALIDACAO DA TABELA POSTGRESQL")
print("=" * 100)

delivery_postgres = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table=tabela_destino,
        properties=propriedades
    )
)

total_postgres = delivery_postgres.count()

print(f"Registros no Parquet   : {total_parquet}")
print(f"Registros no PostgreSQL: {total_postgres}")

if total_postgres != total_parquet:
    raise RuntimeError(
        "Divergencia entre Parquet e PostgreSQL."
    )

if total_postgres != 918:
    raise RuntimeError(
        f"Quantidade inesperada no PostgreSQL: {total_postgres}"
    )

# =============================================================================
# VALIDACAO DO SCHEMA
# =============================================================================

colunas_parquet = set(
    delivery.columns
)

colunas_postgres = set(
    delivery_postgres.columns
)

print(
    f"Colunas no Parquet   : "
    f"{len(colunas_parquet)}"
)

print(
    f"Colunas no PostgreSQL: "
    f"{len(colunas_postgres)}"
)

if colunas_parquet != colunas_postgres:

    faltantes_postgres = (
        colunas_parquet
        - colunas_postgres
    )

    extras_postgres = (
        colunas_postgres
        - colunas_parquet
    )

    print(
        f"Colunas ausentes no PostgreSQL: "
        f"{faltantes_postgres}"
    )

    print(
        f"Colunas extras no PostgreSQL: "
        f"{extras_postgres}"
    )

    raise RuntimeError(
        "Divergencia entre os schemas."
    )

print("Schema: OK")

# =============================================================================
# METRICAS DA TABELA FINAL
# =============================================================================

print("\n" + "=" * 100)
print("METRICAS DA TABELA FINAL")
print("=" * 100)

(
    delivery_postgres
    .groupBy(
        "metodo_enriquecimento"
    )
    .count()
    .orderBy(
        "metodo_enriquecimento"
    )
    .show(
        truncate=False
    )
)

(
    delivery_postgres
    .groupBy(
        "encontrou_enquadramento",
        "encontrou_glassdoor"
    )
    .count()
    .orderBy(
        "encontrou_enquadramento",
        "encontrou_glassdoor"
    )
    .show(
        truncate=False
    )
)

print("\nAmostra da tabela final:")

delivery_postgres.select(
    "ano",
    "trimestre",
    "tipo",
    "cnpj_if",
    "instituicao_financeira",
    "segmento_final",
    "nome_referencia",
    "metodo_enriquecimento",
    "encontrou_enquadramento",
    "encontrou_glassdoor"
).show(
    10,
    truncate=False
)

print("=" * 100)
print("DELIVERY_POSTGRESQL_OK")
print("=" * 100)

spark.stop()
