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

    senha = getpass.getpass(
        "Senha do usuario atividade3_user: "
    )

    jar = "drivers/postgresql-42.7.13.jar"

    jdbc_url = (
        f"jdbc:postgresql://{host}:5432/{banco}"
    )

    print("=" * 80)
    print("ATIVIDADE 3 - TESTE JDBC SPARK x POSTGRESQL")
    print("=" * 80)

    print(f"\nHost: {host}")
    print("Porta: 5432")
    print(f"Banco: {banco}")
    print(f"Usuario: {usuario}")
    print(f"Driver JDBC: {jar}")

    spark = (
        SparkSession.builder
        .appName("Atividade3-Teste-JDBC-PostgreSQL")
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

    print("\nTentando conexao JDBC...")

    df = spark.read.jdbc(
        url=jdbc_url,
        table="pg_catalog.pg_database",
        properties=propriedades,
    )

    total_bancos = df.count()

    print(
        f"[OK] Conexao JDBC realizada. "
        f"Bancos visiveis: {total_bancos}"
    )

    print("\nBANCOS VISIVEIS:")

    (
        df
        .select("datname")
        .orderBy("datname")
        .show(
            truncate=False
        )
    )

    atividade3_existe = (
        df
        .filter(
            df.datname == "atividade3"
        )
        .count()
    )

    print("\n" + "=" * 80)
    print("RESULTADO")
    print("=" * 80)

    if atividade3_existe == 1:
        print(
            "[OK] Banco atividade3 localizado "
            "por meio do Spark/JDBC."
        )
    else:
        print(
            "[ERRO] Banco atividade3 nao localizado."
        )

    print(
        "[OK] Driver PostgreSQL carregado pelo Spark."
    )

    print(
        "[OK] Autenticacao JDBC concluida."
    )

    print(
        "\nTESTE JDBC CONCLUIDO COM SUCESSO."
    )

    spark.stop()


if __name__ == "__main__":
    main()
