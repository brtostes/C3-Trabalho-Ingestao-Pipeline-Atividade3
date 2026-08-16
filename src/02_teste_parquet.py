import os
import sys
import pyspark

# Configura explicitamente o ambiente do Spark e do Python.
# Isso evita que o driver e os workers utilizem versões diferentes do Python.
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Teste-Parquet")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("ATIVIDADE 3 - TESTE DE PARQUET")
    print("=" * 60)

    dados = [
        (1, "Python", "Programacao"),
        (2, "Spark", "Processamento distribuido"),
        (3, "PostgreSQL", "Banco de dados")
    ]

    colunas = [
        "id",
        "tecnologia",
        "categoria"
    ]

    df = spark.createDataFrame(dados, colunas)

    caminho_saida = "data/output/teste_parquet"

    print("\nDataFrame original:")
    df.show(truncate=False)

    print("\nGravando dados em formato Parquet...")

    (
        df.write
        .mode("overwrite")
        .parquet(caminho_saida)
    )

    print("Parquet gravado com sucesso.")

    print("\nLendo novamente os dados em Parquet...")

    df_parquet = spark.read.parquet(caminho_saida)

    print("\nSchema recuperado:")
    df_parquet.printSchema()

    print("\nDados recuperados:")
    df_parquet.show(truncate=False)

    quantidade = df_parquet.count()

    print(f"\nQuantidade de registros recuperados: {quantidade}")

    if quantidade == 3:
        print("\nTESTE DE PARQUET CONCLUIDO COM SUCESSO.")
    else:
        print("\nERRO: quantidade de registros diferente da esperada.")

    spark.stop()


if __name__ == "__main__":
    main()