from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = (
    SparkSession.builder
    .appName("Atividade3_Validacao_RAW")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base = "/workspace/data/raw"

fontes = [
    (
        "reclamacoes/2021_tri_01",
        "2021_tri_01",
        105
    ),
    (
        "reclamacoes/2021_tri_02",
        "2021_tri_02",
        111
    ),
    (
        "reclamacoes/2021_tri_03",
        "2021_tri_03",
        113
    ),
    (
        "reclamacoes/2021_tri_04",
        "2021_tri_04",
        135
    ),
    (
        "reclamacoes/2022_tri_01",
        "2022_tri_01",
        137
    ),
    (
        "reclamacoes/2022_tri_03",
        "2022_tri_03",
        163
    ),
    (
        "reclamacoes/2022_tri_04",
        "2022_tri_04",
        154
    ),
    (
        "enquadramento/enquadramento_inicial",
        "enquadramento",
        1474
    ),
    (
        "glassdoor/match_less",
        "glassdoor_match_less",
        5
    ),
    (
        "glassdoor/match",
        "glassdoor_match",
        34
    ),
]

print("=" * 100)
print("ATIVIDADE 3 - VALIDACAO FORMAL DA CAMADA RAW")
print("=" * 100)

total = 0
erros = []

for caminho_relativo, nome, esperado in fontes:

    caminho = f"{base}/{caminho_relativo}"

    df = spark.read.parquet(caminho)

    registros = df.count()
    colunas = len(df.columns)

    total += registros

    status = "OK" if registros == esperado else "ERRO"

    print("\n" + "-" * 100)
    print(f"DATASET: {nome}")
    print(f"Registros encontrados : {registros}")
    print(f"Registros esperados   : {esperado}")
    print(f"Quantidade de colunas : {colunas}")
    print(f"Validacao da contagem : {status}")

    if registros != esperado:
        erros.append(
            f"{nome}: esperado={esperado}, encontrado={registros}"
        )

    if nome.startswith("2021_") or nome.startswith("2022_"):

        if "_c14" not in df.columns:
            erros.append(
                f"{nome}: coluna _c14 nao encontrada"
            )
        else:

            nao_vazios = (
                df
                .filter(
                    col("_c14").isNotNull()
                    & (trim(col("_c14")) != "")
                )
                .count()
            )

            print(
                f"Valores nao vazios em _c14: {nao_vazios}"
            )

            if nao_vazios != 0:
                erros.append(
                    f"{nome}: _c14 possui "
                    f"{nao_vazios} valores nao vazios"
                )

print("\n" + "=" * 100)
print("RESUMO DA VALIDACAO")
print("=" * 100)

print(f"Total encontrado: {total}")
print("Total esperado  : 2431")

if total != 2431:
    erros.append(
        f"Total geral divergente: {total}"
    )

if len(erros) == 0:

    print("Integridade das contagens: OK")
    print("Coluna _c14: 100% vazia nas reclamacoes")
    print("VALIDACAO_RAW_OK")

else:

    print("Foram encontradas inconsistencias:")

    for erro in erros:
        print(f"- {erro}")

    raise RuntimeError(
        "VALIDACAO_RAW_FALHOU"
    )

print("=" * 100)

spark.stop()
