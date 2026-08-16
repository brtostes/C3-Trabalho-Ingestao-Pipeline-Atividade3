import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def normalizar_nome(coluna):
    """
    Normalizacao conservadora para diagnostico:
    - remove espaços externos
    - converte para maiusculas
    - remove o sufixo '(conglomerado)'
    - reduz espaços repetidos

    Nao remove acentos nem palavras do nome.
    """
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
        .appName("Atividade3-Diagnostico-Integracao-Delivery")
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

    glassdoor_match = spark.read.parquet(
        "data/trusted/glassdoor_match"
    )

    glassdoor_less = spark.read.parquet(
        "data/trusted/glassdoor_match_less"
    )

    print("=" * 80)
    print("ATIVIDADE 3 - DIAGNOSTICO DA INTEGRACAO DA DELIVERY")
    print("=" * 80)

    # =========================================================
    # 1. Duplicidades da chave CNPJ no enquadramento
    # =========================================================

    duplicidades_enq = (
        enquadramento
        .groupBy("cnpj_8")
        .agg(
            F.count("*").alias("qtd_linhas"),
            F.countDistinct("nome").alias("qtd_nomes"),
            F.countDistinct("segmento").alias("qtd_segmentos")
        )
        .filter(F.col("qtd_linhas") > 1)
    )

    qtd_chaves_duplicadas = duplicidades_enq.count()

    print("\n" + "=" * 80)
    print("1. DUPLICIDADES DE CNPJ_8 NO ENQUADRAMENTO")
    print("=" * 80)

    print(
        "Quantidade de chaves cnpj_8 com mais de uma linha: "
        f"{qtd_chaves_duplicadas}"
    )

    exemplos_dup = (
        enquadramento.alias("e")
        .join(
            duplicidades_enq.select("cnpj_8").alias("d"),
            on="cnpj_8",
            how="inner"
        )
        .select(
            "cnpj_8",
            "cnpj",
            "segmento",
            "nome"
        )
        .orderBy(
            "cnpj_8",
            "nome"
        )
    )

    print("\nExemplos:")
    exemplos_dup.show(50, truncate=False)

    # Quantas linhas duplicadas possuem a marca PRUDENCIAL?
    prudenciais_dup = (
        exemplos_dup
        .filter(
            F.upper(F.col("nome")).contains("PRUDENCIAL")
        )
        .count()
    )

    print(
        "Linhas duplicadas contendo 'PRUDENCIAL': "
        f"{prudenciais_dup}"
    )

    # =========================================================
    # 2. Duplicidades na Glassdoor MATCH
    # =========================================================

    print("\n" + "=" * 80)
    print("2. DUPLICIDADES NA GLASSDOOR MATCH")
    print("=" * 80)

    match_norm = (
        glassdoor_match
        .withColumn(
            "nome_norm",
            normalizar_nome(F.col("nome"))
        )
    )

    duplicidades_match = (
        match_norm
        .groupBy("nome_norm")
        .count()
        .filter(F.col("count") > 1)
    )

    print("Chaves duplicadas:")

    (
        match_norm.alias("g")
        .join(
            duplicidades_match.select("nome_norm"),
            on="nome_norm",
            how="inner"
        )
        .select(
            "nome_norm",
            "employer_name",
            "nome",
            "segmento",
            "match_percent",
            "geral"
        )
        .orderBy(
            "nome_norm",
            F.desc("match_percent"),
            "employer_name"
        )
        .show(50, truncate=False)
    )

    # =========================================================
    # 3. Glassdoor MATCH x reclamações por nome
    # =========================================================

    print("\n" + "=" * 80)
    print("3. GLASSDOOR MATCH x RECLAMACOES POR NOME")
    print("=" * 80)

    rec_nomes = (
        reclamacoes
        .withColumn(
            "nome_norm",
            normalizar_nome(
                F.col("instituicao_financeira")
            )
        )
    )

    chaves_rec_nome = (
        rec_nomes
        .select(
            "nome_norm"
        )
        .filter(
            F.col("nome_norm").isNotNull()
        )
        .distinct()
    )

    chaves_match_nome = (
        match_norm
        .select(
            "nome_norm"
        )
        .filter(
            F.col("nome_norm").isNotNull()
        )
        .distinct()
    )

    nomes_encontrados = (
        chaves_match_nome
        .join(
            chaves_rec_nome,
            on="nome_norm",
            how="inner"
        )
    )

    qtd_nomes_match = (
        chaves_match_nome.count()
    )

    qtd_nomes_encontrados = (
        nomes_encontrados.count()
    )

    print(
        "Nomes distintos na Glassdoor match: "
        f"{qtd_nomes_match}"
    )

    print(
        "Nomes Glassdoor encontrados nas reclamacoes: "
        f"{qtd_nomes_encontrados}"
    )

    print("\nNomes encontrados:")

    nomes_encontrados.orderBy("nome_norm").show(
        100,
        truncate=False
    )

    # Quantidade de linhas de reclamações alcançadas
    linhas_rec_com_match_nome = (
        rec_nomes.alias("r")
        .join(
            nomes_encontrados.alias("g"),
            on="nome_norm",
            how="inner"
        )
        .count()
    )

    print(
        "Linhas de reclamacoes alcançadas por nome: "
        f"{linhas_rec_com_match_nome}"
    )

    # =========================================================
    # 4. Glassdoor MATCH_LESS x reclamações por CNPJ
    # =========================================================

    print("\n" + "=" * 80)
    print("4. GLASSDOOR MATCH_LESS x RECLAMACOES POR CNPJ_8")
    print("=" * 80)

    chaves_less = (
        glassdoor_less
        .select(
            "employer_name",
            "cnpj",
            "cnpj_8",
            "nome",
            "match_percent"
        )
    )

    correspondencias_less = (
        chaves_less.alias("g")
        .join(
            reclamacoes.alias("r"),
            on="cnpj_8",
            how="left"
        )
        .select(
            F.col("g.employer_name"),
            F.col("g.cnpj"),
            F.col("g.cnpj_8"),
            F.col("g.nome").alias("nome_glassdoor"),
            F.col("g.match_percent"),
            F.col("r.instituicao_financeira"),
            F.col("r.ano"),
            F.col("r.trimestre")
        )
    )

    correspondencias_less.show(
        100,
        truncate=False
    )

    less_encontrados_distintos = (
        correspondencias_less
        .filter(
            F.col("instituicao_financeira").isNotNull()
        )
        .select("cnpj_8")
        .distinct()
        .count()
    )

    linhas_rec_match_less = (
        correspondencias_less
        .filter(
            F.col("instituicao_financeira").isNotNull()
        )
        .count()
    )

    print(
        "CNPJs distintos de match_less encontrados "
        f"nas reclamacoes: {less_encontrados_distintos}"
    )

    print(
        "Linhas de reclamacoes alcançadas por match_less: "
        f"{linhas_rec_match_less}"
    )

    # =========================================================
    # 5. Resumo
    # =========================================================

    print("\n" + "=" * 80)
    print("RESUMO DO DIAGNOSTICO")
    print("=" * 80)

    print(
        f"CNPJs duplicados no enquadramento: "
        f"{qtd_chaves_duplicadas}"
    )

    print(
        f"Nomes distintos Glassdoor match: "
        f"{qtd_nomes_match}"
    )

    print(
        f"Nomes Glassdoor encontrados nas reclamacoes: "
        f"{qtd_nomes_encontrados}"
    )

    print(
        f"Linhas de reclamacoes alcançadas por nome: "
        f"{linhas_rec_com_match_nome}"
    )

    print(
        f"CNPJs match_less encontrados nas reclamacoes: "
        f"{less_encontrados_distintos}"
    )

    print(
        f"Linhas de reclamacoes alcançadas por match_less: "
        f"{linhas_rec_match_less}"
    )

    print("\nDIAGNOSTICO DA INTEGRACAO CONCLUIDO.")

    spark.stop()


if __name__ == "__main__":
    main()
