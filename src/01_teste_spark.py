from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Teste_Spark")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("ATIVIDADE 3 - TESTE DO APACHE SPARK")
print("=" * 60)

print(f"Versao do Spark: {spark.version}")

dados = [
    ("RAW", "Dados originais"),
    ("TRUSTED", "Dados tratados e padronizados"),
    ("DELIVERY", "Dados finais para consumo"),
]

colunas = ["camada", "descricao"]

df = spark.createDataFrame(dados, colunas)

print("\nSchema do DataFrame:")
df.printSchema()

print("\nConteudo do DataFrame:")
df.show(truncate=False)

quantidade = df.count()

print(f"\nQuantidade de registros: {quantidade}")

print("=" * 60)
print("TESTE_SPARK_OK")
print("=" * 60)

spark.stop()
