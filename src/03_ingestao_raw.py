import os
import sys
from pathlib import Path

# Garante que driver e workers usem o mesmo Python do ambiente virtual
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def criar_spark():
    return (
        SparkSession.builder
        .appName("Atividade3-Ingestao-RAW")
        .master("local[*]")
	.config("spark.sql.legacy.javaCharsets", "true")
        .getOrCreate()
    )


def ler_csv(spark, caminho, separador, encoding):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", separador)
        .option("encoding", encoding)
        .option("mode", "PERMISSIVE")
        .csv(str(caminho))
    )


def registrar_base(nome, df):
    quantidade = df.count()

    print("\n" + "=" * 80)
    print(f"BASE: {nome}")
    print(f"REGISTROS: {quantidade}")
    print(f"COLUNAS: {len(df.columns)}")
    print("NOMES DAS COLUNAS:")

    for coluna in df.columns:
        print(f"  - {coluna}")

    print("\nAMOSTRA:")
    df.show(3, truncate=False)

    return quantidade


def main():

    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")

    input_dir = Path("data/input")
    raw_dir = Path("data/raw")

    raw_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("ATIVIDADE 3 - INGESTAO DA CAMADA RAW COM PYSPARK")
    print("=" * 80)
    print(f"Spark: {spark.version}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Python executavel: {sys.executable}")

    # ---------------------------------------------------------
    # 1. Bases de reclamações
    # ---------------------------------------------------------

    arquivos_reclamacoes = sorted(input_dir.glob("20*_tri_*.csv"))

    print(f"\nArquivos de reclamacoes encontrados: {len(arquivos_reclamacoes)}")

    total_reclamacoes = 0

    for arquivo in arquivos_reclamacoes:

        df = ler_csv(
            spark=spark,
            caminho=arquivo,
            separador=";",
            encoding="windows-1252"
        )

        quantidade = registrar_base(arquivo.name, df)
        total_reclamacoes += quantidade

        destino = raw_dir / arquivo.stem

        (
            df.write
            .mode("overwrite")
            .parquet(str(destino))
        )

        print(f"RAW gravada em: {destino}")

    # ---------------------------------------------------------
    # 2. Base de enquadramento
    # ---------------------------------------------------------

    arquivo_enquadramento = input_dir / "EnquadramentoInicia_v2.tsv"

    df_enquadramento = ler_csv(
        spark=spark,
        caminho=arquivo_enquadramento,
        separador="\t",
        encoding="UTF-8"
    )

    registrar_base(
        arquivo_enquadramento.name,
        df_enquadramento
    )

    (
        df_enquadramento.write
        .mode("overwrite")
        .parquet(str(raw_dir / "enquadramento"))
    )

    print("RAW gravada em: data/raw/enquadramento")

    # ---------------------------------------------------------
    # 3. Bases Glassdoor
    # ---------------------------------------------------------

    arquivos_glassdoor = sorted(
        input_dir.glob("glassdoor_*.csv")
    )

    print(f"\nArquivos Glassdoor encontrados: {len(arquivos_glassdoor)}")

    for arquivo in arquivos_glassdoor:

        df = ler_csv(
            spark=spark,
            caminho=arquivo,
            separador="|",
            encoding="UTF-8"
        )

        registrar_base(arquivo.name, df)

        destino = raw_dir / arquivo.stem

        (
            df.write
            .mode("overwrite")
            .parquet(str(destino))
        )

        print(f"RAW gravada em: {destino}")

    # ---------------------------------------------------------
    # Resumo
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("RESUMO DA INGESTAO RAW")
    print("=" * 80)
    print(f"Arquivos de reclamacoes: {len(arquivos_reclamacoes)}")
    print(f"Registros totais de reclamacoes: {total_reclamacoes}")
    print(f"Arquivos Glassdoor: {len(arquivos_glassdoor)}")
    print("Base de enquadramento: 1")
    print("\nINGESTAO RAW CONCLUIDA COM SUCESSO.")

    spark.stop()


if __name__ == "__main__":
    main()
