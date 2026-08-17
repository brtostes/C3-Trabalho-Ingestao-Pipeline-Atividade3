from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Teste_Conexao_JDBC")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

url = "jdbc:postgresql://postgres:5432/atividade3"

propriedades = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

print("=" * 100)
print("ATIVIDADE 3 - TESTE REAL DA CONEXAO JDBC COM POSTGRESQL")
print("=" * 100)

# Pequeno DataFrame criado exclusivamente para testar JDBC.
df_teste = spark.createDataFrame(
    [
        (1, "JDBC_OK")
    ],
    [
        "id",
        "status"
    ]
)

print("\nDataFrame de teste:")
df_teste.show()

# Grava uma pequena tabela de teste no PostgreSQL.
(
    df_teste.write
    .mode("overwrite")
    .jdbc(
        url=url,
        table="public.jdbc_teste_atividade3",
        properties=propriedades
    )
)

print("Gravacao JDBC concluida.")

# Relê a mesma tabela pelo Spark.
df_leitura = (
    spark.read
    .jdbc(
        url=url,
        table="public.jdbc_teste_atividade3",
        properties=propriedades
    )
)

print("\nLeitura da tabela pelo Spark:")
df_leitura.show()

quantidade = df_leitura.count()

print(f"Registros encontrados: {quantidade}")

if quantidade != 1:
    raise RuntimeError(
        f"Quantidade inesperada no teste JDBC: {quantidade}"
    )

registro = df_leitura.first()

if registro["status"] != "JDBC_OK":
    raise RuntimeError(
        "Conteudo inesperado no teste JDBC."
    )

print("=" * 100)
print("CONEXAO_JDBC_POSTGRESQL_OK")
print("=" * 100)

spark.stop()
