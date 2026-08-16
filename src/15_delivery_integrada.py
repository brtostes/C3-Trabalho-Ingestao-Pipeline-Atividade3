import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
        .appName("Atividade3-Delivery-Integrada")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("ATIVIDADE 3 - CONSTRUCAO DA CAMADA DELIVERY")
    print("=" * 80)

    # =========================================================
    # 1. Leitura
    # =========================================================

    reclamacoes = spark.read.parquet(
        "data/trusted/reclamacoes"
    )

    enquadramento = spark.read.parquet(
        "data/delivery/staging_enquadramento_canonico"
    )

    glassdoor_match = spark.read.parquet(
        "data/delivery/staging_glassdoor_match_canonico"
    )

    glassdoor_less = spark.read.parquet(
        "data/delivery/staging_glassdoor_match_less"
    )

    total_inicial = reclamacoes.count()

    print(f"\nRegistros de reclamacoes recebidos: {total_inicial}")

    # =========================================================
    # 2. Nome normalizado das reclamações
    # =========================================================

    reclamacoes = reclamacoes.withColumn(
        "nome_norm",
        normalizar_nome(
            F.col("instituicao_financeira")
        )
    )

    # =========================================================
    # 3. Preparar enquadramento
    # =========================================================

    enq = (
        enquadramento
        .select(
            "cnpj_8",
            F.col("segmento").alias("segmento_bacen"),
            F.col("nome").alias("nome_enquadramento")
        )
    )

    # =========================================================
    # 4. Preparar Glassdoor match
    # =========================================================

    gd_nome = (
        glassdoor_match
        .select(
            "nome_norm",
            F.col("employer_name")
                .alias("glassdoor_employer_nome"),
            F.col("geral")
                .alias("glassdoor_geral_nome"),
            F.col("cultura_valores")
                .alias("glassdoor_cultura_valores_nome"),
            F.col("diversidade_inclusao")
                .alias("glassdoor_diversidade_nome"),
            F.col("qualidade_vida")
                .alias("glassdoor_qualidade_vida_nome"),
            F.col("alta_lideranca")
                .alias("glassdoor_alta_lideranca_nome"),
            F.col("remuneracao_beneficios")
                .alias("glassdoor_remuneracao_nome"),
            F.col("oportunidades_carreira")
                .alias("glassdoor_carreira_nome"),
            F.col("recomendam_outras_pessoas_pct")
                .alias("glassdoor_recomendam_nome"),
            F.col("perspectiva_positiva_empresa_pct")
                .alias("glassdoor_perspectiva_nome"),
            F.col("match_percent")
                .alias("glassdoor_match_percent_nome")
        )
    )

    # =========================================================
    # 5. Preparar Glassdoor match_less
    # =========================================================

    gd_cnpj = (
        glassdoor_less
        .select(
            "cnpj_8",
            F.col("employer_name")
                .alias("glassdoor_employer_cnpj"),
            F.col("geral")
                .alias("glassdoor_geral_cnpj"),
            F.col("cultura_valores")
                .alias("glassdoor_cultura_valores_cnpj"),
            F.col("diversidade_inclusao")
                .alias("glassdoor_diversidade_cnpj"),
            F.col("qualidade_vida")
                .alias("glassdoor_qualidade_vida_cnpj"),
            F.col("alta_lideranca")
                .alias("glassdoor_alta_lideranca_cnpj"),
            F.col("remuneracao_beneficios")
                .alias("glassdoor_remuneracao_cnpj"),
            F.col("oportunidades_carreira")
                .alias("glassdoor_carreira_cnpj"),
            F.col("recomendam_outras_pessoas_pct")
                .alias("glassdoor_recomendam_cnpj"),
            F.col("perspectiva_positiva_empresa_pct")
                .alias("glassdoor_perspectiva_cnpj"),
            F.col("match_percent")
                .alias("glassdoor_match_percent_cnpj")
        )
    )

    # =========================================================
    # 6. Joins
    # =========================================================

    df = (
        reclamacoes
        .join(
            enq,
            on="cnpj_8",
            how="left"
        )
        .join(
            gd_nome,
            on="nome_norm",
            how="left"
        )
        .join(
            gd_cnpj,
            on="cnpj_8",
            how="left"
        )
    )

    total_apos_joins = df.count()

    # =========================================================
    # 7. Consolidar dados Glassdoor
    # =========================================================

    df = (
        df
        .withColumn(
            "glassdoor_employer",
            F.coalesce(
                F.col("glassdoor_employer_nome"),
                F.col("glassdoor_employer_cnpj")
            )
        )
        .withColumn(
            "glassdoor_geral",
            F.coalesce(
                F.col("glassdoor_geral_nome"),
                F.col("glassdoor_geral_cnpj")
            )
        )
        .withColumn(
            "glassdoor_cultura_valores",
            F.coalesce(
                F.col("glassdoor_cultura_valores_nome"),
                F.col("glassdoor_cultura_valores_cnpj")
            )
        )
        .withColumn(
            "glassdoor_diversidade_inclusao",
            F.coalesce(
                F.col("glassdoor_diversidade_nome"),
                F.col("glassdoor_diversidade_cnpj")
            )
        )
        .withColumn(
            "glassdoor_qualidade_vida",
            F.coalesce(
                F.col("glassdoor_qualidade_vida_nome"),
                F.col("glassdoor_qualidade_vida_cnpj")
            )
        )
        .withColumn(
            "glassdoor_alta_lideranca",
            F.coalesce(
                F.col("glassdoor_alta_lideranca_nome"),
                F.col("glassdoor_alta_lideranca_cnpj")
            )
        )
        .withColumn(
            "glassdoor_remuneracao_beneficios",
            F.coalesce(
                F.col("glassdoor_remuneracao_nome"),
                F.col("glassdoor_remuneracao_cnpj")
            )
        )
        .withColumn(
            "glassdoor_oportunidades_carreira",
            F.coalesce(
                F.col("glassdoor_carreira_nome"),
                F.col("glassdoor_carreira_cnpj")
            )
        )
        .withColumn(
            "glassdoor_recomendam_pct",
            F.coalesce(
                F.col("glassdoor_recomendam_nome"),
                F.col("glassdoor_recomendam_cnpj")
            )
        )
        .withColumn(
            "glassdoor_perspectiva_positiva_pct",
            F.coalesce(
                F.col("glassdoor_perspectiva_nome"),
                F.col("glassdoor_perspectiva_cnpj")
            )
        )
        .withColumn(
            "glassdoor_match_percent",
            F.coalesce(
                F.col("glassdoor_match_percent_nome"),
                F.col("glassdoor_match_percent_cnpj")
            )
        )
        .withColumn(
            "glassdoor_origem_match",
            F.when(
                F.col("glassdoor_employer_nome").isNotNull(),
                F.lit("nome")
            ).when(
                F.col("glassdoor_employer_cnpj").isNotNull(),
                F.lit("cnpj_match_less")
            )
        )
    )

    # =========================================================
    # 8. Remover colunas auxiliares
    # =========================================================

    colunas_auxiliares = [
        "glassdoor_employer_nome",
        "glassdoor_geral_nome",
        "glassdoor_cultura_valores_nome",
        "glassdoor_diversidade_nome",
        "glassdoor_qualidade_vida_nome",
        "glassdoor_alta_lideranca_nome",
        "glassdoor_remuneracao_nome",
        "glassdoor_carreira_nome",
        "glassdoor_recomendam_nome",
        "glassdoor_perspectiva_nome",
        "glassdoor_match_percent_nome",
        "glassdoor_employer_cnpj",
        "glassdoor_geral_cnpj",
        "glassdoor_cultura_valores_cnpj",
        "glassdoor_diversidade_cnpj",
        "glassdoor_qualidade_vida_cnpj",
        "glassdoor_alta_lideranca_cnpj",
        "glassdoor_remuneracao_cnpj",
        "glassdoor_carreira_cnpj",
        "glassdoor_recomendam_cnpj",
        "glassdoor_perspectiva_cnpj",
        "glassdoor_match_percent_cnpj",
        "nome_norm"
    ]

    df = df.drop(*colunas_auxiliares)

    # =========================================================
    # 9. Métricas da Delivery
    # =========================================================

    total_final = df.count()

    com_enquadramento = (
        df
        .filter(F.col("segmento_bacen").isNotNull())
        .count()
    )

    com_glassdoor = (
        df
        .filter(F.col("glassdoor_employer").isNotNull())
        .count()
    )

    por_origem_glassdoor = (
        df
        .groupBy("glassdoor_origem_match")
        .count()
        .orderBy("glassdoor_origem_match")
    )

    print("\n" + "=" * 80)
    print("VALIDACOES DA DELIVERY")
    print("=" * 80)

    print(f"Registros iniciais: {total_inicial}")
    print(f"Registros apos joins: {total_apos_joins}")
    print(f"Registros finais: {total_final}")
    print(f"Com enquadramento BACEN: {com_enquadramento}")
    print(f"Com informacoes Glassdoor: {com_glassdoor}")

    print("\nORIGEM DAS CORRESPONDENCIAS GLASSDOOR:")
    por_origem_glassdoor.show(truncate=False)

    if total_inicial == total_apos_joins == total_final == 918:
        print("[OK] Cardinalidade preservada: 918 registros.")
    else:
        print("[ERRO] Cardinalidade alterada durante os joins.")

    # =========================================================
    # 10. Amostra
    # =========================================================

    print("\nSCHEMA FINAL:")
    df.printSchema()

    print("\nAMOSTRA COM ENQUADRAMENTO E GLASSDOOR:")

    (
        df
        .filter(
            F.col("segmento_bacen").isNotNull()
            | F.col("glassdoor_employer").isNotNull()
        )
        .select(
            "ano",
            "trimestre",
            "instituicao_financeira",
            "cnpj_8",
            "segmento_bacen",
            "nome_enquadramento",
            "glassdoor_employer",
            "glassdoor_geral",
            "glassdoor_match_percent",
            "glassdoor_origem_match"
        )
        .show(30, truncate=False)
    )

    # =========================================================
    # 11. Gravação
    # =========================================================

    destino = "data/delivery/reclamacoes_integradas"

    (
        df.write
        .mode("overwrite")
        .parquet(destino)
    )

    print(f"\nDelivery gravada em: {destino}")

    print("\nCAMADA DELIVERY CONCLUIDA COM SUCESSO.")

    spark.stop()


if __name__ == "__main__":
    main()
