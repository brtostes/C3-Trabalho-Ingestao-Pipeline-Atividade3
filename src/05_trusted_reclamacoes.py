import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Trusted-Reclamacoes")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_dir = Path("data/raw")
    trusted_dir = Path("data/trusted/reclamacoes")

    bases = [
        "2021_tri_01",
        "2021_tri_02",
        "2021_tri_03",
        "2021_tri_04",
        "2022_tri_01",
        "2022_tri_03",
        "2022_tri_04",
    ]

    print("=" * 80)
    print("ATIVIDADE 3 - CAMADA TRUSTED - RECLAMACOES")
    print("=" * 80)

    # ---------------------------------------------------------
    # 1. Leitura e união das bases RAW
    # ---------------------------------------------------------

    dfs = []

    for base in bases:

        df = spark.read.parquet(str(raw_dir / base))

        df = df.withColumn(
            "arquivo_origem",
            F.lit(base)
        )

        dfs.append(df)

    df = dfs[0]

    for proximo in dfs[1:]:
        df = df.unionByName(proximo)

    registros_iniciais = df.count()

    print(f"\nRegistros recebidos da RAW: {registros_iniciais}")
    print(f"Colunas recebidas da RAW: {len(df.columns)}")

    # ---------------------------------------------------------
    # 2. Remoção da coluna estrutural vazia
    # ---------------------------------------------------------

    if "_c14" in df.columns:
        df = df.drop("_c14")
        print("[OK] Coluna _c14 removida.")
    else:
        print("[ATENCAO] Coluna _c14 nao encontrada.")

    # ---------------------------------------------------------
    # 3. Padronização dos nomes das colunas
    # ---------------------------------------------------------

    renomeacoes = {
        "Ano": "ano",
        "Trimestre": "trimestre",
        "Categoria": "categoria",
        "Tipo": "tipo",
        "CNPJ IF": "cnpj_if",
        "Instituição financeira": "instituicao_financeira",
        "Índice": "indice",
        "Quantidade de reclamações reguladas procedentes":
            "qtd_reclamacoes_reguladas_procedentes",
        "Quantidade de reclamações reguladas - outras":
            "qtd_reclamacoes_reguladas_outras",
        "Quantidade de reclamações não reguladas":
            "qtd_reclamacoes_nao_reguladas",
        "Quantidade total de reclamações":
            "qtd_total_reclamacoes",
        "Quantidade total de clientes – CCS e SCR":
            "qtd_total_clientes_ccs_scr",
        "Quantidade de clientes – CCS":
            "qtd_clientes_ccs",
        "Quantidade de clientes – SCR":
            "qtd_clientes_scr",
    }

    for antiga, nova in renomeacoes.items():
        df = df.withColumnRenamed(antiga, nova)

    # ---------------------------------------------------------
    # 4. Limpeza básica dos campos textuais
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # 5. Tratamento do CNPJ
    # ---------------------------------------------------------

    df = df.withColumn(
        "cnpj_if",
        F.when(
            F.col("cnpj_if").isNull(),
            F.lit(None)
        ).otherwise(
            F.regexp_replace(
                F.col("cnpj_if"),
                r"[^0-9]",
                ""
            )
        )
    )

    # ---------------------------------------------------------
    # 6. Tipagem semântica
    # ---------------------------------------------------------

    df = df.withColumn(
        "ano",
        F.col("ano").cast("int")
    )

    df = df.withColumn(
        "trimestre_num",
        F.regexp_extract(
            F.col("trimestre"),
            r"([1-4])",
            1
        ).cast("int")
    )

    df = df.withColumn(
        "indice",
        F.regexp_replace(
            F.regexp_replace(
                F.col("indice"),
                r"\.",
                ""
            ),
            ",",
            "."
        ).cast(DecimalType(18, 2))
    )

    colunas_inteiras = [
        "qtd_reclamacoes_reguladas_procedentes",
        "qtd_reclamacoes_reguladas_outras",
        "qtd_reclamacoes_nao_reguladas",
        "qtd_total_reclamacoes",
        "qtd_total_clientes_ccs_scr",
        "qtd_clientes_ccs",
        "qtd_clientes_scr",
    ]

    for coluna in colunas_inteiras:
        df = df.withColumn(
            coluna,
            F.col(coluna).cast("long")
        )

    # ---------------------------------------------------------
    # 7. Validações
    # ---------------------------------------------------------

    registros_finais = df.count()

    print("\n" + "-" * 80)
    print("VALIDACOES DA TRUSTED")
    print("-" * 80)

    print(f"Registros antes: {registros_iniciais}")
    print(f"Registros depois: {registros_finais}")
    print(f"Colunas finais: {len(df.columns)}")

    anos_invalidos = (
        df
        .filter(
            F.col("ano").isNull()
        )
        .count()
    )

    trimestres_invalidos = (
        df
        .filter(
            F.col("trimestre_num").isNull()
            | (~F.col("trimestre_num").between(1, 4))
        )
        .count()
    )

    print(f"Anos invalidos: {anos_invalidos}")
    print(f"Trimestres invalidos: {trimestres_invalidos}")

    if registros_iniciais == registros_finais == 918:
        print("[OK] Quantidade de registros preservada: 918.")
    else:
        print("[ERRO] Quantidade de registros alterada.")

    if "_c14" not in df.columns:
        print("[OK] _c14 nao existe na Trusted.")

    if anos_invalidos == 0:
        print("[OK] Todos os anos foram convertidos corretamente.")

    if trimestres_invalidos == 0:
        print("[OK] Todos os trimestres foram convertidos corretamente.")

    # ---------------------------------------------------------
    # 8. Schema e amostra
    # ---------------------------------------------------------

    print("\nSCHEMA FINAL:")
    df.printSchema()

    print("\nAMOSTRA DA TRUSTED:")
    df.show(5, truncate=False)

    # ---------------------------------------------------------
    # 9. Gravação em Parquet
    # ---------------------------------------------------------

    (
        df.write
        .mode("overwrite")
        .parquet(str(trusted_dir))
    )

    print(f"\nTrusted gravada em: {trusted_dir}")
    print("\nCAMADA TRUSTED DE RECLAMACOES CONCLUIDA COM SUCESSO.")

    spark.stop()


if __name__ == "__main__":
    main()
