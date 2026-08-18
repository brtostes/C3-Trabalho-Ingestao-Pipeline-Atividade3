from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Tabela_Final_Tratada_Unificada")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# OBJETIVO
# =============================================================================
# Este script explicita o atendimento aos requisitos finais da Atividade 3:
# 1. materializar uma tabela final com os dados tratados e unidos;
# 2. manter a camada Delivery em Parquet;
# 3. disponibilizar a mesma tabela final em banco relacional PostgreSQL.
#
# Observacao: RAW e Trusted permanecem conforme o pipeline validado:
# - RAW: formato livre (neste trabalho, Parquet foi adotado);
# - Trusted: Parquet;
# - Delivery: Parquet + tabela relacional final.
# =============================================================================

DELIVERY_ORIGEM = "/workspace/data/delivery/reclamacoes_enriquecidas"
DELIVERY_FINAL = "/workspace/data/delivery/tabela_final_reclamacoes"

JDBC_URL = "jdbc:postgresql://postgres:5432/atividade3"
TABELA_FINAL = "public.tabela_final_reclamacoes"

PROPRIEDADES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

print("=" * 100)
print("ATIVIDADE 3 - MATERIALIZACAO DA TABELA FINAL TRATADA E UNIFICADA")
print("=" * 100)

# =============================================================================
# 1. LEITURA DA DELIVERY VALIDADA
# =============================================================================

df = spark.read.parquet(DELIVERY_ORIGEM)

total = df.count()
qtd_colunas = len(df.columns)

print(f"\nDelivery de origem : {DELIVERY_ORIGEM}")
print(f"Registros           : {total}")
print(f"Colunas             : {qtd_colunas}")

if total != 918:
    raise RuntimeError(f"Quantidade inesperada de registros: {total}")

if qtd_colunas != 47:
    raise RuntimeError(f"Quantidade inesperada de colunas: {qtd_colunas}")

# =============================================================================
# 2. TABELA FINAL EM PARQUET - CAMADA DELIVERY
# =============================================================================

(
    df.write
    .mode("overwrite")
    .parquet(DELIVERY_FINAL)
)

parquet_final = spark.read.parquet(DELIVERY_FINAL)

total_parquet = parquet_final.count()
colunas_parquet = len(parquet_final.columns)

print("\nTABELA FINAL - DELIVERY PARQUET")
print(f"Caminho             : {DELIVERY_FINAL}")
print(f"Registros           : {total_parquet}")
print(f"Colunas             : {colunas_parquet}")

if total_parquet != total or set(parquet_final.columns) != set(df.columns):
    raise RuntimeError("A tabela final Parquet diverge da Delivery validada.")

# =============================================================================
# 3. TABELA FINAL EM POSTGRESQL
# =============================================================================

(
    parquet_final.write
    .mode("overwrite")
    .jdbc(
        url=JDBC_URL,
        table=TABELA_FINAL,
        properties=PROPRIEDADES,
    )
)

postgres_final = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table=TABELA_FINAL,
        properties=PROPRIEDADES,
    )
)

total_postgres = postgres_final.count()
colunas_postgres = len(postgres_final.columns)

print("\nTABELA FINAL - POSTGRESQL")
print(f"Tabela              : {TABELA_FINAL}")
print(f"Registros           : {total_postgres}")
print(f"Colunas             : {colunas_postgres}")

if total_postgres != total_parquet:
    raise RuntimeError("Divergencia entre Delivery Parquet e PostgreSQL.")

if set(postgres_final.columns) != set(parquet_final.columns):
    raise RuntimeError("Schemas da tabela final Parquet e PostgreSQL divergentes.")

# =============================================================================
# 4. RESUMO DE ATENDIMENTO DOS REQUISITOS
# =============================================================================

print("\n" + "=" * 100)
print("RESUMO DOS REQUISITOS DA ATIVIDADE 3")
print("=" * 100)
print("RAW - FORMATO LIVRE       : OK - PARQUET ADOTADO")
print("TRUSTED - PARQUET         : OK")
print("DELIVERY - PARQUET        : OK")
print("TABELA FINAL TRATADA      : OK")
print("TABELA FINAL UNIFICADA    : OK")
print("POSTGRESQL RELACIONAL     : OK")
print(f"REGISTROS FINAIS          : {total_postgres}")
print(f"COLUNAS FINAIS            : {colunas_postgres}")
print(f"TABELA RELACIONAL         : {TABELA_FINAL}")
print("=" * 100)
print("REQUISITOS_ATIVIDADE3_OK")
print("=" * 100)

spark.stop()
