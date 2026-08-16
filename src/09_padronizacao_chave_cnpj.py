import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Padronizacao-Chave-CNPJ")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("ATIVIDADE 3 - PADRONIZACAO DA CHAVE CNPJ")
    print("=" * 80)

    # =========================================================
    # 1. Reclamações
    # =========================================================

    reclamacoes = spark.read.parquet(
        "data/trusted/reclamacoes"
    )

    total_rec_antes = reclamacoes.count()

    reclamacoes = reclamacoes.withColumn(
        "cnpj_8",
        F.when(
            F.col("cnpj_if").isNull(),
            F.lit(None)
        ).otherwise(
            F.lpad(
                F.col("cnpj_if"),
                8,
                "0"
            )
        )
    )

    total_rec_depois = reclamacoes.count()

    print("\nRECLAMACOES")
    print("-" * 80)
    print(f"Registros antes: {total_rec_antes}")
    print(f"Registros depois: {total_rec_depois}")

    print("\nComprimento de cnpj_8 nas reclamacoes:")

    (
        reclamacoes
        .filter(F.col("cnpj_8").isNotNull())
        .withColumn(
            "tamanho",
            F.length("cnpj_8")
        )
        .groupBy("tamanho")
        .count()
        .orderBy("tamanho")
        .show()
    )

    # =========================================================
    # 2. Enquadramento
    # =========================================================

    enquadramento = spark.read.parquet(
        "data/trusted/enquadramento"
    )

    total_enq_antes = enquadramento.count()

    enquadramento = enquadramento.withColumn(
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

    total_enq_depois = enquadramento.count()

    print("\nENQUADRAMENTO")
    print("-" * 80)
    print(f"Registros antes: {total_enq_antes}")
    print(f"Registros depois: {total_enq_depois}")

    print("\nComprimento de cnpj_8 no enquadramento:")

    (
        enquadramento
        .filter(F.col("cnpj_8").isNotNull())
        .withColumn(
            "tamanho",
            F.length("cnpj_8")
        )
        .groupBy("tamanho")
        .count()
        .orderBy("tamanho")
        .show()
    )

    # =========================================================
    # 3. Verificar colisões
    # =========================================================

    colisoes = (
        enquadramento
        .groupBy("cnpj_8")
        .agg(
            F.countDistinct("cnpj").alias(
                "qtd_cnpj_original"
            )
        )
        .filter(
            F.col("qtd_cnpj_original") > 1
        )
    )

    qtd_colisoes = colisoes.count()

    print(f"\nColisoes apos normalizacao: {qtd_colisoes}")

    # =========================================================
    # 4. Correspondências
    # =========================================================

    chaves_rec = (
        reclamacoes
        .filter(F.col("cnpj_8").isNotNull())
        .select("cnpj_8")
        .distinct()
    )

    chaves_enq = (
        enquadramento
        .filter(F.col("cnpj_8").isNotNull())
        .select("cnpj_8")
        .distinct()
    )

    matches = (
        chaves_rec
        .join(
            chaves_enq,
            on="cnpj_8",
            how="inner"
        )
        .count()
    )

    print(f"Correspondencias distintas por cnpj_8: {matches}")

    # =========================================================
    # 5. Validações
    # =========================================================

    print("\n" + "=" * 80)
    print("RESULTADO")
    print("=" * 80)

    if total_rec_antes == total_rec_depois == 918:
        print("[OK] Reclamações preservadas: 918.")

    if total_enq_antes == total_enq_depois == 1474:
        print("[OK] Enquadramento preservado: 1474.")

    if qtd_colisoes == 0:
        print("[OK] Nenhuma colisao criada pela normalizacao.")

    if matches == 72:
        print("[OK] Correspondencias distintas esperadas = 72.")
    else:
        print(
            f"[ATENCAO] Esperado=72; encontrado={matches}"
        )

    # =========================================================
    # 6. Gravação temporária e substituição controlada
    # =========================================================

    temp_rec = "data/trusted/reclamacoes_temp"
    temp_enq = "data/trusted/enquadramento_temp"

    (
        reclamacoes.write
        .mode("overwrite")
        .parquet(temp_rec)
    )

    (
        enquadramento.write
        .mode("overwrite")
        .parquet(temp_enq)
    )

    print("\nArquivos temporarios gravados com sucesso.")
    print("PADRONIZACAO DA CHAVE CNPJ CONCLUIDA.")

    spark.stop()


if __name__ == "__main__":
    main()
