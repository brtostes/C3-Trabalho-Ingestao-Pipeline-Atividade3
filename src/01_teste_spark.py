from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("Atividade3-Teste-Spark")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ATIVIDADE 3 - TESTE DO APACHE SPARK")
    print("=" * 60)

    print(f"Versao do Spark: {spark.version}")

    dados = [
        (1, "Python"),
        (2, "Spark"),
        (3, "PostgreSQL")
    ]

    colunas = ["id", "tecnologia"]

    df = spark.createDataFrame(dados, colunas)

    print("\nSchema do DataFrame:")
    df.printSchema()

    print("\nConteudo do DataFrame:")
    df.show()

    print(f"\nQuantidade de registros: {df.count()}")

    print("\nTESTE CONCLUIDO COM SUCESSO.")

    spark.stop()


if __name__ == "__main__":
    main()