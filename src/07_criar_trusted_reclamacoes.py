from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    when,
    regexp_replace,
    regexp_extract
)
from functools import reduce

spark = (
    SparkSession.builder
    .appName("Atividade3_Trusted_Reclamacoes")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_base = "/workspace/data/raw/reclamacoes"
trusted_saida = "/workspace/data/trusted/reclamacoes"

fontes = [
    ("2021_tri_01", "2021_tri_01.csv"),
    ("2021_tri_02", "2021_tri_02.csv"),
    ("2021_tri_03", "2021_tri_03.csv"),
    ("2021_tri_04", "2021_tri_04.csv"),
    ("2022_tri_01", "2022_tri_01.csv"),
    ("2022_tri_03", "2022_tri_03.csv"),
    ("2022_tri_04", "2022_tri_04.csv"),
]

colunas_padronizadas = [
    "ano",
    "trimestre",
    "categoria",
    "tipo",
    "cnpj_if",
    "instituicao_financeira",
    "indice",
    "qtd_reclamacoes_reguladas_procedentes",
    "qtd_reclamacoes_reguladas_outras",
    "qtd_reclamacoes_nao_reguladas",
    "qtd_total_reclamacoes",
    "qtd_total_clientes_ccs_scr",
    "qtd_clientes_ccs",
    "qtd_clientes_scr",
    "_c14",
]

colunas_numericas_inteiras = [
    "qtd_reclamacoes_reguladas_procedentes",
    "qtd_reclamacoes_reguladas_outras",
    "qtd_reclamacoes_nao_reguladas",
    "qtd_total_reclamacoes",
    "qtd_total_clientes_ccs_scr",
    "qtd_clientes_ccs",
    "qtd_clientes_scr",
]

dataframes = []

print("=" * 100)
print("ATIVIDADE 3 - CRIACAO DA CAMADA TRUSTED - RECLAMACOES")
print("=" * 100)

for pasta, arquivo_origem in fontes:

    caminho = f"{raw_base}/{pasta}"

    print("\n" + "-" * 100)
    print(f"FONTE: {arquivo_origem}")

    df = spark.read.parquet(caminho)

    registros_raw = df.count()

    print(f"Registros RAW: {registros_raw}")
    print(f"Colunas RAW  : {len(df.columns)}")

    if len(df.columns) != 15:
        raise RuntimeError(
            f"Quantidade inesperada de colunas em {arquivo_origem}: "
            f"{len(df.columns)}"
        )

    # Padronização dos nomes das colunas pela posição conhecida
    # da estrutura original.
    df = df.toDF(*colunas_padronizadas)

    # _c14 foi comprovadamente 100% vazia na validação da RAW.
    df = df.drop("_c14")

    # Rastreabilidade da origem.
    df = df.withColumn(
        "arquivo_origem",
        lit(arquivo_origem)
    )

    # Limpeza conservadora de todas as colunas ainda textuais:
    # 1. corrige byte 0x96 interpretado como U+0096;
    # 2. remove espaços nas extremidades;
    # 3. converte strings vazias em NULL.
    for nome_coluna in df.columns:

        if nome_coluna != "arquivo_origem":

            df = df.withColumn(
                nome_coluna,
                regexp_replace(
                    col(nome_coluna),
                    "\u0096",
                    "–"
                )
            )

            df = df.withColumn(
                nome_coluna,
                trim(col(nome_coluna))
            )

            df = df.withColumn(
                nome_coluna,
                when(
                    col(nome_coluna) == "",
                    None
                ).otherwise(col(nome_coluna))
            )

    # Ano: inteiro.
    df = df.withColumn(
        "ano",
        col("ano").cast("int")
    )

    # Trimestre original contém valores como 1º.
    # Extraímos somente a parte numérica.
    df = df.withColumn(
        "trimestre",
        regexp_extract(
            col("trimestre"),
            r"(\d+)",
            1
        ).cast("int")
    )

    # CNPJ permanece STRING.
    # São mantidos somente dígitos para facilitar futuros joins.
    df = df.withColumn(
        "cnpj_if",
        regexp_replace(
            col("cnpj_if"),
            r"[^0-9]",
            ""
        )
    )

    df = df.withColumn(
        "cnpj_if",
        when(
            col("cnpj_if") == "",
            None
        ).otherwise(col("cnpj_if"))
    )

        # O índice utiliza notação numérica brasileira.
    # Exemplos:
    # 54,79     -> 54.79
    # 16.699,13 -> 16699.13
    # 0         -> 0.0
    df = df.withColumn(
        "indice",
        when(
            col("indice").contains(","),
            regexp_replace(
                regexp_replace(
                    col("indice"),
                    r"\.",
                    ""
                ),
                ",",
                "."
            )
        ).otherwise(
            col("indice")
        )
    )

    df = df.withColumn(
        "indice",
        col("indice").cast("double")
    )

    # Conversão das colunas quantitativas para inteiro longo.
    for nome_coluna in colunas_numericas_inteiras:

        df = df.withColumn(
            nome_coluna,
            col(nome_coluna).cast("long")
        )

    dataframes.append(df)

    print(
        f"Registros preparados para Trusted: {df.count()}"
    )
    

# União dos sete períodos.
df_trusted = reduce(
    lambda esquerda, direita:
        esquerda.unionByName(
            direita,
            allowMissingColumns=False
        ),
    dataframes
)

total_trusted = df_trusted.count()

print("\n" + "=" * 100)
print("VALIDACOES DA TRUSTED")
print("=" * 100)

print(f"Total de registros: {total_trusted}")
print(f"Total esperado     : 918")
print(f"Quantidade colunas : {len(df_trusted.columns)}")

if total_trusted != 918:
    raise RuntimeError(
        f"Total incorreto de reclamacoes: {total_trusted}"
    )

# Validação do ano.
anos_invalidos = (
    df_trusted
    .filter(
        (~col("ano").isin(2021, 2022))
        | col("ano").isNull()
    )
    .count()
)

# Validação do trimestre.
trimestres_invalidos = (
    df_trusted
    .filter(
        (~col("trimestre").isin(1, 2, 3, 4))
        | col("trimestre").isNull()
    )
    .count()
)

# Verificação da proveniência.
fontes_encontradas = (
    df_trusted
    .select("arquivo_origem")
    .distinct()
    .count()
)

print(f"Anos invalidos           : {anos_invalidos}")
print(f"Trimestres invalidos     : {trimestres_invalidos}")
print(f"Arquivos origem distintos: {fontes_encontradas}")

if anos_invalidos != 0:
    raise RuntimeError(
        "Foram encontrados anos invalidos."
    )

if trimestres_invalidos != 0:
    raise RuntimeError(
        "Foram encontrados trimestres invalidos."
    )

if fontes_encontradas != 7:
    raise RuntimeError(
        "Numero incorreto de arquivos de origem."
    )

print("\nSchema da camada Trusted:")
df_trusted.printSchema()

print("\nAmostra da camada Trusted:")
df_trusted.select(
    "ano",
    "trimestre",
    "categoria",
    "tipo",
    "cnpj_if",
    "instituicao_financeira",
    "indice",
    "qtd_total_reclamacoes",
    "arquivo_origem",
).show(
    10,
    truncate=False
)

# Gravação em Parquet.
(
    df_trusted.write
    .mode("overwrite")
    .parquet(trusted_saida)
)

# Releitura para validar persistência.
df_validacao = spark.read.parquet(
    trusted_saida
)

total_gravado = df_validacao.count()

print("\n" + "=" * 100)
print("VALIDACAO DA GRAVACAO")
print("=" * 100)

print(f"Registros antes da gravacao: {total_trusted}")
print(f"Registros apos a gravacao : {total_gravado}")

if total_gravado != total_trusted:
    raise RuntimeError(
        "Divergencia apos gravacao da Trusted."
    )

print("TRUSTED_RECLAMACOES_OK")
print("=" * 100)

spark.stop()
