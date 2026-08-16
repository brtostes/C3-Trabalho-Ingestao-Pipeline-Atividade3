import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def verificar(df, coluna, nome_camada):

    print("\n" + "=" * 80)
    print(nome_camada)
    print("=" * 80)

    if coluna not in df.columns:
        print(f"[ATENCAO] Coluna {coluna} nao encontrada.")
        return

    problemas = (
        df
        .filter(
            F.col(coluna).contains("\uFFFD")
        )
        .select(coluna)
        .distinct()
        .orderBy(coluna)
    )

    quantidade = problemas.count()

    print(
        "Quantidade de valores distintos contendo "
        f"caractere de substituicao: {quantidade}"
    )

    if quantidade > 0:
        problemas.show(100, truncate=False)
    else:
        print("[OK] Nenhum caractere de substituicao encontrado.")


def main():

    spark = (
        SparkSession.builder
        .appName("Atividade3-Diagnostico-Caracteres")
        .master("local[*]")
        .config("spark.sql.legacy.javaCharsets", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("ATIVIDADE 3 - DIAGNOSTICO DE CARACTERES")
    print("=" * 80)

    # ---------------------------------------------------------
    # 1. Arquivo de entrada original lido como UTF-8
    # ---------------------------------------------------------

    input_enq = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .csv("data/input/EnquadramentoInicia_v2.tsv")
    )

    verificar(
        input_enq,
        "Nome",
        "1. INPUT - ENQUADRAMENTO"
    )

    # ---------------------------------------------------------
    # 2. RAW
    # ---------------------------------------------------------

    raw = spark.read.parquet(
        "data/raw/enquadramento"
    )

    verificar(
        raw,
        "Nome",
        "2. RAW - ENQUADRAMENTO"
    )

    # ---------------------------------------------------------
    # 3. TRUSTED
    # ---------------------------------------------------------

    trusted = spark.read.parquet(
        "data/trusted/enquadramento"
    )

    verificar(
        trusted,
        "nome",
        "3. TRUSTED - ENQUADRAMENTO"
    )

    # ---------------------------------------------------------
    # 4. STAGING CANONICO
    # ---------------------------------------------------------

    staging = spark.read.parquet(
        "data/delivery/staging_enquadramento_canonico"
    )

    verificar(
        staging,
        "nome",
        "4. STAGING - ENQUADRAMENTO CANONICO"
    )

    # ---------------------------------------------------------
    # 5. DELIVERY
    # ---------------------------------------------------------

    delivery = spark.read.parquet(
        "data/delivery/reclamacoes_integradas"
    )

    verificar(
        delivery,
        "nome_enquadramento",
        "5. DELIVERY"
    )

    # ---------------------------------------------------------
    # Casos específicos identificados visualmente
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("CASOS ESPECIFICOS")
    print("=" * 80)

    (
        input_enq
        .filter(
            F.upper(F.col("Nome")).contains("RIBEIR")
            | F.upper(F.col("Nome")).contains("TOP")
        )
        .select(
            "Segmento",
            "CNPJ",
            "Nome"
        )
        .show(100, truncate=False)
    )

    print("\nDIAGNOSTICO DE CARACTERES CONCLUIDO.")

    spark.stop()


if __name__ == "__main__":
    main()
