from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_replace,
    length,
    lpad,
    when,
    count,
    sum as spark_sum
)

spark = (
    SparkSession.builder
    .appName("Atividade3_Diagnostico_Chaves_Integracao")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================================
# LEITURA DA TRUSTED
# ============================================================================

reclamacoes = spark.read.parquet(
    "/workspace/data/trusted/reclamacoes"
)

enquadramento = spark.read.parquet(
    "/workspace/data/trusted/enquadramento"
)

glassdoor_match = spark.read.parquet(
    "/workspace/data/trusted/glassdoor/match"
)

glassdoor_less = spark.read.parquet(
    "/workspace/data/trusted/glassdoor/match_less"
)

print("=" * 100)
print("ATIVIDADE 3 - DIAGNOSTICO DAS CHAVES DE INTEGRACAO")
print("=" * 100)

print("\nQuantidades de entrada:")
print(f"Reclamacoes       : {reclamacoes.count()}")
print(f"Enquadramento     : {enquadramento.count()}")
print(f"Glassdoor Match   : {glassdoor_match.count()}")
print(f"Glassdoor Less    : {glassdoor_less.count()}")

# ============================================================================
# FUNCOES DE NORMALIZACAO PARA DIAGNOSTICO
# ============================================================================

def preparar_cnpj(df, coluna_original):

    df = df.withColumn(
        "cnpj_digitos",
        regexp_replace(
            col(coluna_original),
            r"[^0-9]",
            ""
        )
    )

    df = df.withColumn(
        "cnpj_digitos",
        when(
            col("cnpj_digitos") == "",
            None
        ).otherwise(col("cnpj_digitos"))
    )

    df = df.withColumn(
        "cnpj_tamanho",
        length(col("cnpj_digitos"))
    )

    # A chave de oito posições é criada SOMENTE para diagnóstico.
    # Valores maiores que oito dígitos não são truncados.
    df = df.withColumn(
        "cnpj_chave_8",
        when(
            col("cnpj_tamanho").between(1, 8),
            lpad(
                col("cnpj_digitos"),
                8,
                "0"
            )
        ).otherwise(None)
    )

    return df


def preparar_nome(df, coluna_nome):

    return df.withColumn(
        "nome_chave",
        upper(
            trim(
                regexp_replace(
                    col(coluna_nome),
                    r"\s+",
                    " "
                )
            )
        )
    )


# ============================================================================
# PREPARACAO DAS BASES
# ============================================================================

rec = preparar_cnpj(
    reclamacoes,
    "cnpj_if"
)

rec = preparar_nome(
    rec,
    "instituicao_financeira"
)

enq = preparar_cnpj(
    enquadramento,
    "cnpj"
)

enq = preparar_nome(
    enq,
    "nome"
)

enq = enq.withColumn(
    "segmento_chave",
    upper(trim(col("segmento")))
)

gd_match = preparar_nome(
    glassdoor_match,
    "nome"
)

gd_match = gd_match.withColumn(
    "segmento_chave",
    upper(trim(col("segmento")))
)

gd_less = preparar_cnpj(
    glassdoor_less,
    "cnpj"
)

gd_less = preparar_nome(
    gd_less,
    "nome"
)

# ============================================================================
# 1. DISTRIBUICAO DOS TAMANHOS DE CNPJ
# ============================================================================

print("\n" + "=" * 100)
print("1. DISTRIBUICAO DO TAMANHO DOS CNPJS")
print("=" * 100)

print("\nReclamacoes:")

(
    rec
    .groupBy("cnpj_tamanho")
    .count()
    .orderBy("cnpj_tamanho")
    .show(
        30,
        truncate=False
    )
)

print("\nEnquadramento:")

(
    enq
    .groupBy("cnpj_tamanho")
    .count()
    .orderBy("cnpj_tamanho")
    .show(
        30,
        truncate=False
    )
)

print("\nGlassdoor Match Less:")

(
    gd_less
    .groupBy("cnpj_tamanho")
    .count()
    .orderBy("cnpj_tamanho")
    .show(
        30,
        truncate=False
    )
)

# ============================================================================
# 2. VALORES QUE NAO PODEM SER NORMALIZADOS PARA 8 DIGITOS
# ============================================================================

print("\n" + "=" * 100)
print("2. CNPJS COM MAIS DE 8 DIGITOS")
print("=" * 100)

invalidos_rec = (
    rec
    .filter(col("cnpj_tamanho") > 8)
    .count()
)

invalidos_enq = (
    enq
    .filter(col("cnpj_tamanho") > 8)
    .count()
)

invalidos_less = (
    gd_less
    .filter(col("cnpj_tamanho") > 8)
    .count()
)

print(f"Reclamacoes       : {invalidos_rec}")
print(f"Enquadramento     : {invalidos_enq}")
print(f"Glassdoor Less    : {invalidos_less}")

# ============================================================================
# 3. CNPJS ESPECIAIS / ZERO
# ============================================================================

print("\n" + "=" * 100)
print("3. CHAVE 00000000")
print("=" * 100)

zero_rec = (
    rec
    .filter(col("cnpj_chave_8") == "00000000")
    .count()
)

zero_enq = (
    enq
    .filter(col("cnpj_chave_8") == "00000000")
    .count()
)

print(f"Reclamacoes   : {zero_rec}")
print(f"Enquadramento : {zero_enq}")

# ============================================================================
# 4. DUPLICIDADES DE CNPJ NO ENQUADRAMENTO
# ============================================================================

print("\n" + "=" * 100)
print("4. DUPLICIDADES DE CNPJ NORMALIZADO NO ENQUADRAMENTO")
print("=" * 100)

duplicados = (
    enq
    .filter(
        col("cnpj_chave_8").isNotNull()
    )
    .groupBy("cnpj_chave_8")
    .count()
    .filter(
        col("count") > 1
    )
)

qtd_chaves_duplicadas = duplicados.count()

linhas_em_chaves_duplicadas = (
    duplicados
    .agg(
        spark_sum("count").alias("total")
    )
    .first()["total"]
)

if linhas_em_chaves_duplicadas is None:
    linhas_em_chaves_duplicadas = 0

print(
    f"Quantidade de chaves duplicadas: "
    f"{qtd_chaves_duplicadas}"
)

print(
    f"Registros envolvidos           : "
    f"{linhas_em_chaves_duplicadas}"
)

print("\nDetalhamento das duplicidades:")

(
    enq
    .join(
        duplicados.select("cnpj_chave_8"),
        on="cnpj_chave_8",
        how="inner"
    )
    .select(
        "cnpj_chave_8",
        "segmento",
        "cnpj",
        "nome"
    )
    .orderBy(
        "cnpj_chave_8",
        "segmento",
        "nome"
    )
    .show(
        100,
        truncate=False
    )
)

# ============================================================================
# 5. COBERTURA RECLAMACOES X ENQUADRAMENTO
# ============================================================================

print("\n" + "=" * 100)
print("5. COBERTURA DE CNPJ - RECLAMACOES X ENQUADRAMENTO")
print("=" * 100)

rec_chaves = (
    rec
    .filter(
        col("cnpj_chave_8").isNotNull()
        & (col("cnpj_chave_8") != "00000000")
    )
    .select("cnpj_chave_8")
    .distinct()
)

enq_chaves = (
    enq
    .filter(
        col("cnpj_chave_8").isNotNull()
        & (col("cnpj_chave_8") != "00000000")
    )
    .select("cnpj_chave_8")
    .distinct()
)

total_rec_chaves = rec_chaves.count()

encontradas = (
    rec_chaves
    .join(
        enq_chaves,
        on="cnpj_chave_8",
        how="inner"
    )
    .count()
)

nao_encontradas = (
    rec_chaves
    .join(
        enq_chaves,
        on="cnpj_chave_8",
        how="left_anti"
    )
)

qtd_nao_encontradas = nao_encontradas.count()

if total_rec_chaves > 0:

    cobertura = (
        encontradas
        / total_rec_chaves
        * 100
    )

else:

    cobertura = 0.0

print(
    f"CNPJs distintos nas reclamacoes : "
    f"{total_rec_chaves}"
)

print(
    f"CNPJs encontrados                : "
    f"{encontradas}"
)

print(
    f"CNPJs sem correspondencia        : "
    f"{qtd_nao_encontradas}"
)

print(
    f"Cobertura                        : "
    f"{cobertura:.2f}%"
)

print("\nExemplos de CNPJs sem correspondencia:")

(
    rec
    .join(
        nao_encontradas,
        on="cnpj_chave_8",
        how="inner"
    )
    .select(
        "cnpj_chave_8",
        "cnpj_if",
        "instituicao_financeira"
    )
    .distinct()
    .orderBy(
        "cnpj_chave_8"
    )
    .show(
        100,
        truncate=False
    )
)

# ============================================================================
# 6. GLASSDOOR MATCH X ENQUADRAMENTO
# ============================================================================

print("\n" + "=" * 100)
print("6. GLASSDOOR MATCH X ENQUADRAMENTO")
print("=" * 100)

enq_nome_segmento = (
    enq
    .select(
        "segmento_chave",
        "nome_chave"
    )
    .distinct()
)

gd_match_chaves = (
    gd_match
    .select(
        "segmento_chave",
        "nome_chave"
    )
    .distinct()
)

gd_match_encontrados = (
    gd_match_chaves
    .join(
        enq_nome_segmento,
        on=[
            "segmento_chave",
            "nome_chave"
        ],
        how="inner"
    )
    .count()
)

gd_match_nao_encontrados = (
    gd_match_chaves
    .join(
        enq_nome_segmento,
        on=[
            "segmento_chave",
            "nome_chave"
        ],
        how="left_anti"
    )
)

print(
    f"Chaves Glassdoor Match       : "
    f"{gd_match_chaves.count()}"
)

print(
    f"Correspondencias encontradas : "
    f"{gd_match_encontrados}"
)

print(
    f"Sem correspondencia          : "
    f"{gd_match_nao_encontrados.count()}"
)

print("\nMatch sem correspondencia exata:")

gd_match_nao_encontrados.show(
    100,
    truncate=False
)

# ============================================================================
# 7. GLASSDOOR MATCH LESS X ENQUADRAMENTO
# ============================================================================

print("\n" + "=" * 100)
print("7. GLASSDOOR MATCH LESS X ENQUADRAMENTO")
print("=" * 100)

gd_less_chaves = (
    gd_less
    .filter(
        col("cnpj_chave_8").isNotNull()
        & (col("cnpj_chave_8") != "00000000")
    )
    .select(
        "cnpj_chave_8"
    )
    .distinct()
)

gd_less_encontrados = (
    gd_less_chaves
    .join(
        enq_chaves,
        on="cnpj_chave_8",
        how="inner"
    )
    .count()
)

gd_less_nao = (
    gd_less_chaves
    .join(
        enq_chaves,
        on="cnpj_chave_8",
        how="left_anti"
    )
)

print(
    f"Chaves Glassdoor Match Less  : "
    f"{gd_less_chaves.count()}"
)

print(
    f"Correspondencias encontradas : "
    f"{gd_less_encontrados}"
)

print(
    f"Sem correspondencia          : "
    f"{gd_less_nao.count()}"
)

print("\nMatch Less sem correspondencia:")

gd_less_nao.show(
    100,
    truncate=False
)

print("\n" + "=" * 100)
print("DIAGNOSTICO_CHAVES_OK")
print("=" * 100)

spark.stop()
