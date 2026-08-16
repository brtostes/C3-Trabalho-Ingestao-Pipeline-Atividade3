import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Validacao-Trusted-Temp")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    rec_original = spark.read.parquet(
        "data/trusted/reclamacoes"
    )

    rec_temp = spark.read.parquet(
        "data/trusted/reclamacoes_temp"
    )

    enq_original = spark.read.parquet(
        "data/trusted/enquadramento"
    )

    enq_temp = spark.read.parquet(
        "data/trusted/enquadramento_temp"
    )

    print("=" * 80)
    print("ATIVIDADE 3 - VALIDACAO DAS TRUSTED TEMPORARIAS")
    print("=" * 80)

    # ---------------------------------------------------------
    # Reclamações
    # ---------------------------------------------------------

    print("\nRECLAMACOES")
    print("-" * 80)

    rec_orig_count = rec_original.count()
    rec_temp_count = rec_temp.count()

    print(f"Original: {rec_orig_count}")
    print(f"Temporaria: {rec_temp_count}")

    print(f"Colunas original: {len(rec_original.columns)}")
    print(f"Colunas temporaria: {len(rec_temp.columns)}")

    if "cnpj_8" in rec_temp.columns:
        print("[OK] cnpj_8 existe na temporaria.")
    else:
        print("[ERRO] cnpj_8 nao existe.")

    rec_cnpj_nulos = (
        rec_temp
        .filter(F.col("cnpj_8").isNull())
        .count()
    )

    rec_cnpj_preenchidos = (
        rec_temp
        .filter(F.col("cnpj_8").isNotNull())
        .count()
    )

    print(f"cnpj_8 preenchidos: {rec_cnpj_preenchidos}")
    print(f"cnpj_8 nulos: {rec_cnpj_nulos}")

    # ---------------------------------------------------------
    # Enquadramento
    # ---------------------------------------------------------

    print("\nENQUADRAMENTO")
    print("-" * 80)

    enq_orig_count = enq_original.count()
    enq_temp_count = enq_temp.count()

    print(f"Original: {enq_orig_count}")
    print(f"Temporaria: {enq_temp_count}")

    print(f"Colunas original: {len(enq_original.columns)}")
    print(f"Colunas temporaria: {len(enq_temp.columns)}")

    if "cnpj_8" in enq_temp.columns:
        print("[OK] cnpj_8 existe na temporaria.")
    else:
        print("[ERRO] cnpj_8 nao existe.")

    # ---------------------------------------------------------
    # Correspondências
    # ---------------------------------------------------------

    matches = (
        rec_temp
        .filter(F.col("cnpj_8").isNotNull())
        .select("cnpj_8")
        .distinct()
        .join(
            enq_temp
            .select("cnpj_8")
            .distinct(),
            on="cnpj_8",
            how="inner"
        )
        .count()
    )

    print(f"\nCorrespondencias distintas: {matches}")

    # ---------------------------------------------------------
    # Resultado
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("RESULTADO")
    print("=" * 80)

    if rec_orig_count == rec_temp_count == 918:
        print("[OK] Reclamações preservadas.")

    if enq_orig_count == enq_temp_count == 1474:
        print("[OK] Enquadramento preservado.")

    if rec_cnpj_preenchidos == 437:
        print("[OK] 437 registros de reclamacoes possuem cnpj_8.")

    if matches == 72:
        print("[OK] 72 chaves distintas possuem correspondencia.")

    print("\nVALIDACAO DAS TRUSTED TEMPORARIAS CONCLUIDA.")

    spark.stop()


if __name__ == "__main__":
    main()
