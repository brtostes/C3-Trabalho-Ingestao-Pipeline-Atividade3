import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Diagnostico-CNPJ")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    reclamacoes = spark.read.parquet(
        "data/trusted/reclamacoes"
    )

    enquadramento = spark.read.parquet(
        "data/trusted/enquadramento"
    )

    print("=" * 80)
    print("ATIVIDADE 3 - DIAGNOSTICO DA CHAVE CNPJ")
    print("=" * 80)

    # ---------------------------------------------------------
    # 1. CNPJs distintos nas reclamações
    # ---------------------------------------------------------

    cnpj_reclamacoes = (
        reclamacoes
        .filter(F.col("cnpj_if").isNotNull())
        .select(
            F.col("cnpj_if").alias("cnpj")
        )
        .distinct()
    )

    cnpj_enquadramento = (
        enquadramento
        .filter(F.col("cnpj").isNotNull())
        .select("cnpj")
        .distinct()
    )

    qtd_rec = cnpj_reclamacoes.count()
    qtd_enq = cnpj_enquadramento.count()

    print(f"\nCNPJs distintos nas reclamacoes: {qtd_rec}")
    print(f"CNPJs distintos no enquadramento: {qtd_enq}")

    # ---------------------------------------------------------
    # 2. Comprimento nas reclamações
    # ---------------------------------------------------------

    print("\nCOMPRIMENTO DOS CNPJS NAS RECLAMACOES")
    print("-" * 80)

    (
        cnpj_reclamacoes
        .withColumn(
            "tamanho",
            F.length("cnpj")
        )
        .groupBy("tamanho")
        .count()
        .orderBy("tamanho")
        .show()
    )

    # ---------------------------------------------------------
    # 3. Correspondência sem preenchimento
    # ---------------------------------------------------------

    correspondencias_originais = (
        cnpj_reclamacoes.alias("r")
        .join(
            cnpj_enquadramento.alias("e"),
            F.col("r.cnpj") == F.col("e.cnpj"),
            "inner"
        )
        .select(
            F.col("r.cnpj").alias("cnpj")
        )
        .distinct()
    )

    qtd_match_original = correspondencias_originais.count()

    print(
        "\nCorrespondencias distintas SEM completar zeros: "
        f"{qtd_match_original}"
    )

    # ---------------------------------------------------------
    # 4. Criar versões de 8 caracteres apenas para diagnóstico
    # ---------------------------------------------------------

    rec_pad = (
        cnpj_reclamacoes
        .withColumn(
            "cnpj_8",
            F.lpad(F.col("cnpj"), 8, "0")
        )
    )

    enq_pad = (
        cnpj_enquadramento
        .withColumn(
            "cnpj_8",
            F.lpad(F.col("cnpj"), 8, "0")
        )
    )

    correspondencias_pad = (
        rec_pad.alias("r")
        .join(
            enq_pad.alias("e"),
            F.col("r.cnpj_8") == F.col("e.cnpj_8"),
            "inner"
        )
        .select(
            F.col("r.cnpj").alias("cnpj_reclamacao"),
            F.col("e.cnpj").alias("cnpj_enquadramento"),
            F.col("r.cnpj_8").alias("cnpj_8")
        )
        .distinct()
    )

    qtd_match_pad = (
        correspondencias_pad
        .select("cnpj_8")
        .distinct()
        .count()
    )

    print(
        "Correspondencias distintas COM preenchimento para 8 digitos: "
        f"{qtd_match_pad}"
    )

    # ---------------------------------------------------------
    # 5. Mostrar correspondências criadas somente pelo padding
    # ---------------------------------------------------------

    novos_matches = (
        correspondencias_pad
        .filter(
            F.col("cnpj_reclamacao")
            != F.col("cnpj_enquadramento")
        )
        .orderBy("cnpj_8")
    )

    print("\nCORRESPONDENCIAS CRIADAS PELO PREENCHIMENTO")
    print("-" * 80)

    novos_matches.show(50, truncate=False)

    print(
        "Quantidade de correspondencias em que "
        "os textos originais eram diferentes:",
        novos_matches.count()
    )

    # ---------------------------------------------------------
    # 6. Verificar colisões no enquadramento após padding
    # ---------------------------------------------------------

    colisoes = (
        enq_pad
        .groupBy("cnpj_8")
        .agg(
            F.countDistinct("cnpj").alias(
                "quantidade_cnpjs_originais"
            )
        )
        .filter(
            F.col("quantidade_cnpjs_originais") > 1
        )
    )

    qtd_colisoes = colisoes.count()

    print("\nCOLISOES APOS PREENCHIMENTO PARA 8 DIGITOS")
    print("-" * 80)

    colisoes.show(50, truncate=False)

    print(f"Quantidade de colisoes: {qtd_colisoes}")

    # ---------------------------------------------------------
    # 7. Resultado
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("RESUMO DO DIAGNOSTICO")
    print("=" * 80)

    print(f"CNPJs distintos reclamacoes: {qtd_rec}")
    print(f"CNPJs distintos enquadramento: {qtd_enq}")
    print(f"Matches sem padding: {qtd_match_original}")
    print(f"Matches com padding: {qtd_match_pad}")
    print(f"Colisoes apos padding: {qtd_colisoes}")

    print("\nDIAGNOSTICO DA CHAVE CNPJ CONCLUIDO.")

    spark.stop()


if __name__ == "__main__":
    main()
