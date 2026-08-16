import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def limpar_strings(df):
    for coluna, tipo in df.dtypes:
        if tipo == "string":
            df = df.withColumn(
                coluna,
                F.when(
                    F.trim(F.col(coluna)) == "",
                    F.lit(None)
                ).otherwise(
                    F.trim(F.col(coluna))
                )
            )
    return df


def padronizar_colunas(df):

    renomeacoes = {
        "employer-website": "employer_website",
        "employer-headquarters": "employer_headquarters",
        "employer-founded": "employer_founded",
        "employer-industry": "employer_industry",
        "employer-revenue": "employer_revenue",
        "Geral": "geral",
        "Cultura e valores": "cultura_valores",
        "Diversidade e inclusão": "diversidade_inclusao",
        "Qualidade de vida": "qualidade_vida",
        "Alta liderança": "alta_lideranca",
        "Remuneração e benefícios": "remuneracao_beneficios",
        "Oportunidades de carreira": "oportunidades_carreira",
        "Recomendam para outras pessoas(%)":
            "recomendam_outras_pessoas_pct",
        "Perspectiva positiva da empresa(%)":
            "perspectiva_positiva_empresa_pct",
        "Segmento": "segmento",
        "CNPJ": "cnpj",
        "Nome": "nome",
    }

    for antiga, nova in renomeacoes.items():
        if antiga in df.columns:
            df = df.withColumnRenamed(antiga, nova)

    return df


def converter_tipos(df):

    colunas_contagem = [
        "reviews_count",
        "culture_count",
        "salaries_count",
        "benefits_count",
    ]

    for coluna in colunas_contagem:
        if coluna in df.columns:
            df = df.withColumn(
                coluna,
                F.col(coluna).cast("long")
            )

    if "employer_founded" in df.columns:
        df = df.withColumn(
            "employer_founded",
            F.col("employer_founded")
            .cast("double")
            .cast("int")
        )

    colunas_decimais = [
        "geral",
        "cultura_valores",
        "diversidade_inclusao",
        "qualidade_vida",
        "alta_lideranca",
        "remuneracao_beneficios",
        "oportunidades_carreira",
        "recomendam_outras_pessoas_pct",
        "perspectiva_positiva_empresa_pct",
        "match_percent",
    ]

    for coluna in colunas_decimais:
        if coluna in df.columns:
            df = df.withColumn(
                coluna,
                F.col(coluna).cast(DecimalType(10, 2))
            )

    return df


def tratar_match(spark):

    origem = "data/raw/glassdoor_consolidado_join_match_v2"
    destino = "data/trusted/glassdoor_match"

    df = spark.read.parquet(origem)

    inicial = df.count()

    df = padronizar_colunas(df)
    df = limpar_strings(df)
    df = converter_tipos(df)

    if "segmento" in df.columns:
        df = df.withColumn(
            "segmento",
            F.upper(F.col("segmento"))
        )

    df = df.withColumn(
        "tipo_correspondencia",
        F.lit("match")
    )

    final = df.count()

    print("\n" + "=" * 80)
    print("GLASSDOOR MATCH")
    print("=" * 80)

    print(f"Registros antes: {inicial}")
    print(f"Registros depois: {final}")
    print(f"Colunas: {len(df.columns)}")

    print("\nDistribuicao do match_percent:")

    (
        df
        .groupBy("match_percent")
        .count()
        .orderBy(F.desc("match_percent"))
        .show(30, truncate=False)
    )

    print("\nSchema:")
    df.printSchema()

    print("\nAmostra:")
    df.show(5, truncate=False)

    (
        df.write
        .mode("overwrite")
        .parquet(destino)
    )

    if inicial == final == 34:
        print("[OK] Glassdoor match preservado: 34 registros.")

    print(f"Trusted gravada em: {destino}")


def tratar_match_less(spark):

    origem = "data/raw/glassdoor_consolidado_join_match_less_v2"
    destino = "data/trusted/glassdoor_match_less"

    df = spark.read.parquet(origem)

    inicial = df.count()

    df = padronizar_colunas(df)
    df = limpar_strings(df)

    if "cnpj" in df.columns:

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

        df = df.withColumn(
            "cnpj_8",
            F.when(
                F.col("cnpj").isNull(),
                F.lit(None)
            ).otherwise(
                F.lpad(
                    F.col("cnpj"),
                    8,
                    "0"
                )
            )
        )

    df = converter_tipos(df)

    df = df.withColumn(
        "tipo_correspondencia",
        F.lit("match_less")
    )

    final = df.count()

    print("\n" + "=" * 80)
    print("GLASSDOOR MATCH LESS")
    print("=" * 80)

    print(f"Registros antes: {inicial}")
    print(f"Registros depois: {final}")
    print(f"Colunas: {len(df.columns)}")

    print("\nCNPJ original e normalizado:")

    if "cnpj_8" in df.columns:
        (
            df
            .select(
                "employer_name",
                "cnpj",
                "cnpj_8",
                "nome",
                "match_percent"
            )
            .show(20, truncate=False)
        )

    print("\nSchema:")
    df.printSchema()

    (
        df.write
        .mode("overwrite")
        .parquet(destino)
    )

    if inicial == final == 5:
        print("[OK] Glassdoor match_less preservado: 5 registros.")

    print(f"Trusted gravada em: {destino}")


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Trusted-Glassdoor")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("ATIVIDADE 3 - CAMADA TRUSTED - GLASSDOOR")
    print("=" * 80)

    tratar_match(spark)
    tratar_match_less(spark)

    print("\n" + "=" * 80)
    print("TRUSTED GLASSDOOR CONCLUIDA COM SUCESSO.")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
