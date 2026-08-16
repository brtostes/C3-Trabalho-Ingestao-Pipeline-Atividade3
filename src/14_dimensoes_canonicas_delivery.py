import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def normalizar_nome(coluna):

    return F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.upper(F.trim(coluna)),
                r"\s*\(CONGLOMERADO\)\s*$",
                ""
            ),
            r"\s+",
            " "
        )
    )


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Dimensoes-Canonicas-Delivery")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("ATIVIDADE 3 - DIMENSOES CANONICAS PARA DELIVERY")
    print("=" * 80)

    # =========================================================
    # 1. ENQUADRAMENTO CANONICO
    # =========================================================

    enquadramento = spark.read.parquet(
        "data/trusted/enquadramento"
    )

    print("\n" + "=" * 80)
    print("1. ENQUADRAMENTO CANONICO")
    print("=" * 80)

    print(
        f"Registros recebidos: "
        f"{enquadramento.count()}"
    )

    enquadramento = enquadramento.withColumn(
        "eh_prudencial",
        F.when(
            F.upper(F.col("nome")).contains("PRUDENCIAL"),
            F.lit(1)
        ).otherwise(
            F.lit(0)
        )
    )

    janela_enquadramento = (
        Window
        .partitionBy("cnpj_8")
        .orderBy(
            F.col("eh_prudencial").asc(),
            F.length(F.col("nome")).desc(),
            F.col("nome").asc()
        )
    )

    enquadramento_canonico = (
        enquadramento
        .withColumn(
            "ordem_canonica",
            F.row_number().over(janela_enquadramento)
        )
        .filter(
            F.col("ordem_canonica") == 1
        )
        .drop(
            "ordem_canonica",
            "eh_prudencial"
        )
    )

    qtd_enq_canonico = enquadramento_canonico.count()

    duplicidades_enq = (
        enquadramento_canonico
        .groupBy("cnpj_8")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"Registros canonicos: "
        f"{qtd_enq_canonico}"
    )

    print(
        f"CNPJs duplicados apos canonicalizacao: "
        f"{duplicidades_enq}"
    )

    print("\nExemplos de CNPJs anteriormente duplicados:")

    cnpjs_exemplo = [
        "00000000",
        "07450604",
        "58160789",
        "59285411",
        "59588111",
        "60872504",
        "90400888"
    ]

    (
        enquadramento_canonico
        .filter(
            F.col("cnpj_8").isin(cnpjs_exemplo)
        )
        .select(
            "cnpj_8",
            "segmento",
            "nome"
        )
        .orderBy("cnpj_8")
        .show(50, truncate=False)
    )

    # =========================================================
    # 2. GLASSDOOR MATCH CANONICO
    # =========================================================

    glassdoor = spark.read.parquet(
        "data/trusted/glassdoor_match"
    )

    print("\n" + "=" * 80)
    print("2. GLASSDOOR MATCH CANONICO")
    print("=" * 80)

    print(
        f"Registros recebidos: "
        f"{glassdoor.count()}"
    )

    glassdoor = glassdoor.withColumn(
        "nome_norm",
        normalizar_nome(
            F.col("nome")
        )
    )

    janela_glassdoor = (
        Window
        .partitionBy("nome_norm")
        .orderBy(
            F.desc_nulls_last("match_percent"),
            F.desc_nulls_last("geral"),
            F.desc_nulls_last("reviews_count"),
            F.asc_nulls_last("employer_name")
        )
    )

    glassdoor_canonico = (
        glassdoor
        .withColumn(
            "ordem_canonica",
            F.row_number().over(janela_glassdoor)
        )
        .filter(
            F.col("ordem_canonica") == 1
        )
        .drop("ordem_canonica")
    )

    qtd_glassdoor_canonico = (
        glassdoor_canonico.count()
    )

    duplicidades_glassdoor = (
        glassdoor_canonico
        .groupBy("nome_norm")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(
        f"Registros canonicos: "
        f"{qtd_glassdoor_canonico}"
    )

    print(
        "Nomes duplicados apos canonicalizacao: "
        f"{duplicidades_glassdoor}"
    )

    print("\nSelecao para nomes anteriormente duplicados:")

    (
        glassdoor_canonico
        .filter(
            F.col("nome_norm").isin(
                "JP MORGAN CHASE",
                "VOTORANTIM"
            )
        )
        .select(
            "nome_norm",
            "employer_name",
            "segmento",
            "match_percent",
            "geral",
            "reviews_count"
        )
        .orderBy("nome_norm")
        .show(20, truncate=False)
    )

    # =========================================================
    # 3. GLASSDOOR MATCH_LESS
    # =========================================================

    glassdoor_less = spark.read.parquet(
        "data/trusted/glassdoor_match_less"
    )

    print("\n" + "=" * 80)
    print("3. GLASSDOOR MATCH_LESS")
    print("=" * 80)

    qtd_less = glassdoor_less.count()

    duplicidades_less = (
        glassdoor_less
        .groupBy("cnpj_8")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    print(f"Registros: {qtd_less}")

    print(
        f"CNPJs duplicados: "
        f"{duplicidades_less}"
    )

    # =========================================================
    # 4. VALIDACOES
    # =========================================================

    print("\n" + "=" * 80)
    print("RESULTADO DA CANONICALIZACAO")
    print("=" * 80)

    if qtd_enq_canonico == 1459:
        print(
            "[OK] Enquadramento canonico = "
            "1459 chaves."
        )
    else:
        print(
            "[ATENCAO] Enquadramento canonico "
            f"= {qtd_enq_canonico}."
        )

    if duplicidades_enq == 0:
        print(
            "[OK] Nenhuma duplicidade de cnpj_8 "
            "no enquadramento canonico."
        )

    if qtd_glassdoor_canonico == 32:
        print(
            "[OK] Glassdoor canonico = "
            "32 nomes distintos."
        )
    else:
        print(
            "[ATENCAO] Glassdoor canonico = "
            f"{qtd_glassdoor_canonico}."
        )

    if duplicidades_glassdoor == 0:
        print(
            "[OK] Nenhuma duplicidade de nome_norm "
            "na Glassdoor canonica."
        )

    if qtd_less == 5 and duplicidades_less == 0:
        print(
            "[OK] Glassdoor match_less possui "
            "5 CNPJs sem duplicidade."
        )

    # =========================================================
    # 5. GRAVACAO
    # =========================================================

    (
        enquadramento_canonico
        .write
        .mode("overwrite")
        .parquet(
            "data/delivery/staging_enquadramento_canonico"
        )
    )

    (
        glassdoor_canonico
        .write
        .mode("overwrite")
        .parquet(
            "data/delivery/staging_glassdoor_match_canonico"
        )
    )

    (
        glassdoor_less
        .write
        .mode("overwrite")
        .parquet(
            "data/delivery/staging_glassdoor_match_less"
        )
    )

    print("\nDimensoes de staging gravadas.")
    print(
        "CANONICALIZACAO PARA DELIVERY "
        "CONCLUIDA COM SUCESSO."
    )

    spark.stop()


if __name__ == "__main__":
    main()
