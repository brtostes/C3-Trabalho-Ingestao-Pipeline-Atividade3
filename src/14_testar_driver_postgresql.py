from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Atividade3_Teste_Driver_PostgreSQL")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 100)
print("ATIVIDADE 3 - TESTE DO DRIVER JDBC POSTGRESQL")
print("=" * 100)

try:

    spark.sparkContext._jvm.java.lang.Class.forName(
        "org.postgresql.Driver"
    )

    print("Driver encontrado: org.postgresql.Driver")
    print("DRIVER_POSTGRESQL_OK")

except Exception as erro:

    print("Falha ao carregar o driver PostgreSQL.")
    print(str(erro))

    raise

finally:

    spark.stop()
