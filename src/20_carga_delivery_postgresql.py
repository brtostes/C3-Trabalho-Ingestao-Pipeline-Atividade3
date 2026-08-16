import os
import sys
import getpass

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def main():

    host = os.environ.get("WINDOWS_HOST")

    if not host:
        raise RuntimeError(
            "Variavel WINDOWS_HOST nao definida."
        )

    usuario = "atividade3_user"
    banco = "atividade3"
    tabela = "public.reclamacoes_integradas"

    senha = getpass.getpass(
        "Senha do usuario atividade3_user: "
    )

    jdbc_url = (
        f"jdbc:postgresql://{host}:5432/{banco}"
    )

    jar = "drivers/postgresql-42.7.13.jar"

    print("=" * 80)
    print("ATIVIDADE 3 - CARGA DA DELIVERY NO POSTGRESQL")
    print("=" * 80)

    print(f"\nHost: {host}")
    print("Porta: 5432")
    print(f"Banco: {banco}")
    print(f"Usuario: {usuario}")
    print(f"Tabela destino: {tabela}")

    spark = (
        SparkSession.builder
        .appName("Atividade3-Carga-Delivery-PostgreSQL")
        .master("local[*]")
        .config(
            "spark.jars",
            jar
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    propriedades = {
        "user": usuario,
        "password": senha,
        "driver": "org.postgresql.Driver",
    }

    # ---------------------------------------------------------
    # 1. Leitura da Delivery em Parquet
    # ---------------------------------------------------------

    origem = "data/delivery/reclamacoes_integradas"

    df = spark.read.parquet(origem)

    total_parquet = df.count()

    print("\n" + "-" * 80)
    print("VALIDACAO ANTES DA CARGA")
    print("-" * 80)

    print(f"Registros no Parquet: {total_parquet}")
    print(f"Colunas no Parquet: {len(df.columns)}")

    if total_parquet != 918:
        raise RuntimeError(
            f"Quantidade inesperada na Delivery: "
            f"{total_parquet}. Esperado: 918."
        )

    print("[OK] Delivery possui 918 registros.")

    # ---------------------------------------------------------
    # 2. Escrita via Spark JDBC
    # ---------------------------------------------------------

    print("\nIniciando gravacao JDBC...")

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=tabela,
            properties=propriedades
        )
    )

    print("[OK] Gravacao JDBC concluida.")

    # ---------------------------------------------------------
    # 3. Leitura da tabela gravada
    # ---------------------------------------------------------

    print("\nValidando tabela gravada...")

    df_postgres = (
        spark.read
        .jdbc(
            url=jdbc_url,
            table=tabela,
            properties=propriedades
        )
    )

    total_postgres = df_postgres.count()

    print(f"Registros no PostgreSQL: {total_postgres}")
    print(f"Colunas no PostgreSQL: {len(df_postgres.columns)}")

    # ---------------------------------------------------------
    # 4. Validação final
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("RESULTADO")
    print("=" * 80)

    if total_parquet == total_postgres == 918:
        print(
            "[OK] Parquet e PostgreSQL possuem "
            "918 registros."
        )
    else:
        raise RuntimeError(
            "Divergencia entre Parquet e PostgreSQL."
        )

    if len(df.columns) == len(df_postgres.columns):
        print(
            "[OK] Quantidade de colunas preservada: "
            f"{len(df.columns)}."
        )
    else:
        print(
            "[ATENCAO] Quantidade de colunas divergiu."
        )

    print("\nSCHEMA LIDO DO POSTGRESQL:")
    df_postgres.printSchema()

    print("\nAMOSTRA DA TABELA RELACIONAL:")

    (
        df_postgres
        .select(
            "ano",
            "trimestre",
            "instituicao_financeira",
            "cnpj_8",
            "segmento_bacen",
            "glassdoor_employer",
            "glassdoor_match_percent",
            "glassdoor_origem_match"
        )
        .show(20, truncate=False)
    )

    print(
        "\nTABELA DELIVERY GRAVADA NO POSTGRESQL "
        "COM SUCESSO."
    )

    spark.stop()


if __name__ == "__main__":
    main()
