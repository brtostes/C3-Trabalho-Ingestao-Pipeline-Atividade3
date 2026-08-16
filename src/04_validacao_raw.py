import os
import sys
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Validacao-RAW")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_dir = Path("data/raw")

    print("=" * 80)
    print("ATIVIDADE 3 - VALIDACAO DA CAMADA RAW")
    print("=" * 80)

    # =========================================================
    # 1. Reclamações
    # =========================================================

    bases_reclamacoes = [
        "2021_tri_01",
        "2021_tri_02",
        "2021_tri_03",
        "2021_tri_04",
        "2022_tri_01",
        "2022_tri_03",
        "2022_tri_04",
    ]

    total_reclamacoes = 0
    total_c14_preenchida = 0

    print("\nVALIDACAO DAS BASES DE RECLAMACOES")
    print("-" * 80)

    for base in bases_reclamacoes:

        df = spark.read.parquet(str(raw_dir / base))

        quantidade = df.count()
        total_reclamacoes += quantidade

        if "_c14" in df.columns:

            preenchidos_c14 = (
                df
                .filter(
                    F.col("_c14").isNotNull()
                    & (F.trim(F.col("_c14")) != "")
                )
                .count()
            )

        else:
            preenchidos_c14 = -1

        total_c14_preenchida += max(preenchidos_c14, 0)

        tipos = {campo.name: campo.dataType.simpleString()
                 for campo in df.schema.fields}

        todos_string = all(tipo == "string" for tipo in tipos.values())

        print(
            f"{base}: "
            f"registros={quantidade}; "
            f"colunas={len(df.columns)}; "
            f"_c14_preenchida={preenchidos_c14}; "
            f"todas_colunas_string={todos_string}"
        )

    print("-" * 80)
    print(f"TOTAL DE RECLAMACOES: {total_reclamacoes}")
    print(f"TOTAL DE _c14 PREENCHIDA: {total_c14_preenchida}")

    # =========================================================
    # 2. Enquadramento
    # =========================================================

    df_enquadramento = spark.read.parquet(
        str(raw_dir / "enquadramento")
    )

    print("\nVALIDACAO DA BASE DE ENQUADRAMENTO")
    print("-" * 80)
    print(f"Registros: {df_enquadramento.count()}")
    print(f"Colunas: {len(df_enquadramento.columns)}")
    print(f"Nomes: {df_enquadramento.columns}")

    # =========================================================
    # 3. Glassdoor
    # =========================================================

    bases_glassdoor = [
        "glassdoor_consolidado_join_match_less_v2",
        "glassdoor_consolidado_join_match_v2",
    ]

    print("\nVALIDACAO DAS BASES GLASSDOOR")
    print("-" * 80)

    for base in bases_glassdoor:

        df = spark.read.parquet(str(raw_dir / base))

        print(
            f"{base}: "
            f"registros={df.count()}; "
            f"colunas={len(df.columns)}"
        )

        print(f"Colunas: {df.columns}")

    # =========================================================
    # 4. Resultado das verificações principais
    # =========================================================

    print("\n" + "=" * 80)
    print("RESULTADO DA AUDITORIA RAW")
    print("=" * 80)

    if total_reclamacoes == 918:
        print("[OK] Total de reclamacoes = 918")
    else:
        print(
            f"[ERRO] Total de reclamacoes esperado=918; "
            f"obtido={total_reclamacoes}"
        )

    if total_c14_preenchida == 0:
        print("[OK] _c14 esta vazia em todos os registros.")
    else:
        print(
            f"[ATENCAO] Foram encontrados "
            f"{total_c14_preenchida} registros preenchidos em _c14."
        )

    if df_enquadramento.count() == 1474:
        print("[OK] Enquadramento = 1474 registros.")
    else:
        print("[ERRO] Quantidade inesperada no enquadramento.")

    print("\nVALIDACAO DA CAMADA RAW CONCLUIDA.")

    spark.stop()


if __name__ == "__main__":
    main()
