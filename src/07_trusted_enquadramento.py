import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Trusted-Enquadramento")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    origem = "data/raw/enquadramento"
    destino = "data/trusted/enquadramento"

    print("=" * 80)
    print("ATIVIDADE 3 - CAMADA TRUSTED - ENQUADRAMENTO")
    print("=" * 80)

    # ---------------------------------------------------------
    # 1. Leitura da RAW
    # ---------------------------------------------------------

    df = spark.read.parquet(origem)

    registros_iniciais = df.count()

    print(f"\nRegistros recebidos da RAW: {registros_iniciais}")
    print(f"Colunas recebidas: {len(df.columns)}")
    print(f"Nomes originais: {df.columns}")

    # ---------------------------------------------------------
    # 2. Padronização dos nomes
    # ---------------------------------------------------------

    df = (
        df
        .withColumnRenamed("Segmento", "segmento")
        .withColumnRenamed("CNPJ", "cnpj")
        .withColumnRenamed("Nome", "nome")
    )

    # ---------------------------------------------------------
    # 3. Limpeza textual
    # ---------------------------------------------------------

    for coluna in ["segmento", "cnpj", "nome"]:

        df = df.withColumn(
            coluna,
            F.when(
                F.trim(F.col(coluna)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(coluna))
            )
        )

    # ---------------------------------------------------------
    # 4. Padronização do CNPJ
    # ---------------------------------------------------------

    df = df.withColumn(
        "cnpj",
        F.when(
            F.col("cnpj").isNull(),
            F.lit(None)
        ).otherwise(
            F.regexp_replace(
                F.col("cnpj"),
                r"[^0-9]",
                ""
            )
        )
    )

    # ---------------------------------------------------------
    # 5. Padronização do segmento
    # ---------------------------------------------------------

    df = df.withColumn(
        "segmento",
        F.upper(F.col("segmento"))
    )

    # ---------------------------------------------------------
    # 6. Validações
    # ---------------------------------------------------------

    registros_finais = df.count()

    print("\n" + "-" * 80)
    print("VALIDACOES")
    print("-" * 80)

    print(f"Registros antes: {registros_iniciais}")
    print(f"Registros depois: {registros_finais}")

    cnpj_nulos = (
        df
        .filter(F.col("cnpj").isNull())
        .count()
    )

    nome_nulos = (
        df
        .filter(F.col("nome").isNull())
        .count()
    )

    segmento_nulos = (
        df
        .filter(F.col("segmento").isNull())
        .count()
    )

    print(f"CNPJ nulos: {cnpj_nulos}")
    print(f"Nome nulos: {nome_nulos}")
    print(f"Segmento nulos: {segmento_nulos}")

    print("\nSEGMENTOS ENCONTRADOS:")

    (
        df
        .groupBy("segmento")
        .count()
        .orderBy("segmento")
        .show(truncate=False)
    )

    print("\nCOMPRIMENTO DOS CNPJS:")

    (
        df
        .withColumn(
            "tamanho_cnpj",
            F.length(F.col("cnpj"))
        )
        .groupBy("tamanho_cnpj")
        .count()
        .orderBy("tamanho_cnpj")
        .show()
    )

    if registros_iniciais == registros_finais == 1474:
        print("[OK] Quantidade de registros preservada: 1474.")
    else:
        print("[ERRO] Quantidade de registros foi alterada.")

    # ---------------------------------------------------------
    # 7. Schema
    # ---------------------------------------------------------

    print("\nSCHEMA FINAL:")
    df.printSchema()

    print("\nAMOSTRA:")
    df.show(10, truncate=False)

    # ---------------------------------------------------------
    # 8. Gravação
    # ---------------------------------------------------------

    (
        df.write
        .mode("overwrite")
        .parquet(destino)
    )

    print(f"\nTrusted gravada em: {destino}")
    print("\nCAMADA TRUSTED DE ENQUADRAMENTO CONCLUIDA COM SUCESSO.")

    spark.stop()


if __name__ == "__main__":
    main()
