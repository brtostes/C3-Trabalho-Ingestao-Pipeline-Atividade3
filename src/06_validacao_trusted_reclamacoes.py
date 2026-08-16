import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Validacao-Trusted-Reclamacoes")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    caminho = "data/trusted/reclamacoes"

    df = spark.read.parquet(caminho)

    print("=" * 80)
    print("ATIVIDADE 3 - VALIDACAO DA TRUSTED DE RECLAMACOES")
    print("=" * 80)

    # ---------------------------------------------------------
    # 1. Quantidade total
    # ---------------------------------------------------------

    total = df.count()

    print(f"\nTotal de registros: {total}")
    print(f"Total de colunas: {len(df.columns)}")

    # ---------------------------------------------------------
    # 2. Distribuição por arquivo de origem
    # ---------------------------------------------------------

    print("\nREGISTROS POR ARQUIVO DE ORIGEM")
    print("-" * 80)

    (
        df
        .groupBy("arquivo_origem")
        .count()
        .orderBy("arquivo_origem")
        .show(truncate=False)
    )

    # ---------------------------------------------------------
    # 3. Valores especiais de índice
    # ---------------------------------------------------------

    print("\nVALIDACAO DOS INDICES COM SEPARADOR DE MILHAR")
    print("-" * 80)

    valores_esperados = [
        2055.01,
        14015.05,
        16699.13
    ]

    (
        df
        .filter(
            F.col("indice").isin(valores_esperados)
        )
        .select(
            "ano",
            "trimestre",
            "instituicao_financeira",
            "indice",
            "arquivo_origem"
        )
        .orderBy("indice")
        .show(20, truncate=False)
    )

    encontrados_especiais = (
        df
        .filter(
            F.col("indice").isin(valores_esperados)
        )
        .count()
    )

    print(
        f"Quantidade de registros especiais encontrados: "
        f"{encontrados_especiais}"
    )

    # ---------------------------------------------------------
    # 4. Validação de ano e trimestre
    # ---------------------------------------------------------

    anos = (
        df
        .select("ano")
        .distinct()
        .orderBy("ano")
    )

    print("\nANOS ENCONTRADOS")
    anos.show()

    print("\nANO E TRIMESTRE")
    (
        df
        .select(
            "ano",
            "trimestre",
            "trimestre_num"
        )
        .distinct()
        .orderBy(
            "ano",
            "trimestre_num"
        )
        .show()
    )

    # ---------------------------------------------------------
    # 5. Validações de integridade
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("RESULTADO DA VALIDACAO")
    print("=" * 80)

    if total == 918:
        print("[OK] Total de registros = 918.")
    else:
        print(f"[ERRO] Total esperado=918; obtido={total}")

    if "_c14" not in df.columns:
        print("[OK] Coluna _c14 ausente na Trusted.")
    else:
        print("[ERRO] Coluna _c14 ainda existe.")

    if encontrados_especiais == 3:
        print(
            "[OK] Os 3 indices com separador de milhar "
            "foram convertidos corretamente."
        )
    else:
        print(
            "[ATENCAO] Quantidade inesperada de valores "
            "especiais convertidos."
        )

    print("\nVALIDACAO DA TRUSTED DE RECLAMACOES CONCLUIDA.")

    spark.stop()


if __name__ == "__main__":
    main()
