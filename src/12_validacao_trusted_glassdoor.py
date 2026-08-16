import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Validacao-Trusted-Glassdoor")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    match = spark.read.parquet(
        "data/trusted/glassdoor_match"
    )

    match_less = spark.read.parquet(
        "data/trusted/glassdoor_match_less"
    )

    enquadramento = spark.read.parquet(
        "data/trusted/enquadramento"
    )

    print("=" * 80)
    print("ATIVIDADE 3 - VALIDACAO DAS TRUSTED GLASSDOOR")
    print("=" * 80)

    # =========================================================
    # 1. Quantidades
    # =========================================================

    qtd_match = match.count()
    qtd_match_less = match_less.count()

    print("\nQUANTIDADES")
    print("-" * 80)
    print(f"Glassdoor match: {qtd_match}")
    print(f"Glassdoor match_less: {qtd_match_less}")

    # =========================================================
    # 2. Duplicidades no MATCH
    # =========================================================

    print("\nDUPLICIDADES DE NOME - MATCH")
    print("-" * 80)

    duplicados_match = (
        match
        .groupBy("nome")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicados_match.show(50, truncate=False)

    qtd_dup_match = duplicados_match.count()

    print(f"Quantidade de nomes duplicados: {qtd_dup_match}")

    # =========================================================
    # 3. Correspondência MATCH x Enquadramento por nome
    # =========================================================

    nomes_enq = (
        enquadramento
        .select(
            F.col("nome").alias("nome_enquadramento")
        )
        .distinct()
    )

    nomes_match = (
        match
        .select(
            F.col("nome").alias("nome_glassdoor")
        )
        .distinct()
    )

    correspondencias_nome = (
        nomes_match.alias("g")
        .join(
            nomes_enq.alias("e"),
            F.col("g.nome_glassdoor")
            == F.col("e.nome_enquadramento"),
            "inner"
        )
    )

    qtd_match_nome = correspondencias_nome.count()

    print(
        "\nNomes distintos da Glassdoor match encontrados "
        f"no enquadramento: {qtd_match_nome}"
    )

    # =========================================================
    # 4. MATCH_LESS x Enquadramento por CNPJ
    # =========================================================

    correspondencias_cnpj = (
        match_less.alias("g")
        .join(
            enquadramento.alias("e"),
            F.col("g.cnpj_8") == F.col("e.cnpj_8"),
            "left"
        )
        .select(
            F.col("g.employer_name"),
            F.col("g.cnpj").alias("cnpj_glassdoor"),
            F.col("g.cnpj_8"),
            F.col("g.nome").alias("nome_glassdoor"),
            F.col("g.match_percent"),
            F.col("e.nome").alias("nome_enquadramento"),
            F.col("e.segmento").alias("segmento_enquadramento")
        )
    )

    print("\nMATCH_LESS x ENQUADRAMENTO")
    print("-" * 80)

    correspondencias_cnpj.show(
        20,
        truncate=False
    )

    qtd_match_less_encontrados = (
        correspondencias_cnpj
        .filter(
            F.col("nome_enquadramento").isNotNull()
        )
        .count()
    )

    print(
        "Registros match_less encontrados pelo CNPJ: "
        f"{qtd_match_less_encontrados}"
    )

    # =========================================================
    # 5. Intervalo do match_percent
    # =========================================================

    print("\nINTERVALOS DE MATCH_PERCENT")
    print("-" * 80)

    match.agg(
        F.min("match_percent").alias("min_match"),
        F.max("match_percent").alias("max_match")
    ).show()

    match_less.agg(
        F.min("match_percent").alias("min_match_less"),
        F.max("match_percent").alias("max_match_less")
    ).show()

    # =========================================================
    # 6. Resultado
    # =========================================================

    print("\n" + "=" * 80)
    print("RESULTADO DA VALIDACAO")
    print("=" * 80)

    if qtd_match == 34:
        print("[OK] Glassdoor match = 34 registros.")

    if qtd_match_less == 5:
        print("[OK] Glassdoor match_less = 5 registros.")

    if qtd_dup_match == 0:
        print("[OK] Nao ha duplicidade na chave nome da match.")
    else:
        print(
            f"[ATENCAO] Existem {qtd_dup_match} "
            "nomes duplicados na match."
        )

    print(
        f"[INFO] Correspondencias por nome "
        f"match x enquadramento = {qtd_match_nome}."
    )

    print(
        f"[INFO] Correspondencias por CNPJ "
        f"match_less x enquadramento = "
        f"{qtd_match_less_encontrados}."
    )

    print("\nVALIDACAO DAS TRUSTED GLASSDOOR CONCLUIDA.")

    spark.stop()


if __name__ == "__main__":
    main()
