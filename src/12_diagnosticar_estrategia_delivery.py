from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    regexp_replace,
    translate,
    length,
    lpad,
    when,
    row_number,
    count
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("Atividade3_Diagnostico_Estrategia_Delivery")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

reclamacoes = spark.read.parquet(
    "/workspace/data/trusted/reclamacoes"
)

enquadramento = spark.read.parquet(
    "/workspace/data/trusted/enquadramento"
)

glassdoor = spark.read.parquet(
    "/workspace/data/trusted/glassdoor/match"
)

print("=" * 100)
print("ATIVIDADE 3 - DIAGNOSTICO DA ESTRATEGIA DA DELIVERY")
print("=" * 100)


# ============================================================================
# FUNCOES
# ============================================================================

def normalizar_cnpj(coluna):

    digitos = regexp_replace(
        coluna,
        r"[^0-9]",
        ""
    )

    return when(
        (length(digitos) >= 1)
        & (length(digitos) <= 8),
        lpad(digitos, 8, "0")
    ).otherwise(None)


def normalizar_nome(coluna):

    resultado = upper(
        trim(coluna)
    )

    # Remove indicação de conglomerado ao final.
    resultado = regexp_replace(
        resultado,
        r"\s*\(CONGLOMERADO\)\s*$",
        ""
    )

    # Remove acentuação portuguesa mais comum.
    resultado = translate(
        resultado,
        "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "AAAAAEEEEIIIIOOOOOUUUUC"
    )

    # Pontuação vira espaço.
    resultado = regexp_replace(
        resultado,
        r"[^A-Z0-9]+",
        " "
    )

    # Consolida espaços.
    resultado = regexp_replace(
        resultado,
        r"\s+",
        " "
    )

    return trim(resultado)


# ============================================================================
# 1. PERFIL DAS RECLAMACOES
# ============================================================================

print("\n" + "=" * 100)
print("1. PERFIL DAS RECLAMACOES SEGUNDO A DISPONIBILIDADE DE CNPJ")
print("=" * 100)

com_cnpj = reclamacoes.filter(
    col("cnpj_if").isNotNull()
)

sem_cnpj = reclamacoes.filter(
    col("cnpj_if").isNull()
)

print(
    f"Total de reclamacoes : "
    f"{reclamacoes.count()}"
)

print(
    f"Com CNPJ             : "
    f"{com_cnpj.count()}"
)

print(
    f"Sem CNPJ             : "
    f"{sem_cnpj.count()}"
)

print("\nDistribuicao de TIPO entre registros sem CNPJ:")

(
    sem_cnpj
    .groupBy("tipo")
    .count()
    .orderBy(
        col("count").desc()
    )
    .show(
        30,
        truncate=False
    )
)


# ============================================================================
# 2. ENQUADRAMENTO CANONICO
# ============================================================================

print("\n" + "=" * 100)
print("2. TESTE DE DEDUPLICACAO DO ENQUADRAMENTO")
print("=" * 100)

enq = enquadramento.withColumn(
    "cnpj_chave",
    normalizar_cnpj(
        col("cnpj")
    )
)

# 00000000 não será utilizado como chave relacional.
enq = enq.filter(
    col("cnpj_chave").isNotNull()
    & (col("cnpj_chave") != "00000000")
)

# Prioridade:
# 1 = registro institucional comum
# 2 = registro identificado como PRUDENCIAL
enq = enq.withColumn(
    "prioridade",
    when(
        upper(col("nome")).contains("PRUDENCIAL"),
        2
    ).otherwise(1)
)

janela_cnpj = Window.partitionBy(
    "cnpj_chave"
).orderBy(
    col("prioridade").asc(),
    col("nome").asc()
)

enq_canonico = (
    enq
    .withColumn(
        "ordem",
        row_number().over(janela_cnpj)
    )
    .filter(
        col("ordem") == 1
    )
    .drop(
        "ordem",
        "prioridade"
    )
)

chaves_enq_original = (
    enq
    .select("cnpj_chave")
    .distinct()
    .count()
)

chaves_enq_canonico = (
    enq_canonico
    .select("cnpj_chave")
    .distinct()
    .count()
)

print(
    f"Chaves distintas antes da deduplicacao : "
    f"{chaves_enq_original}"
)

print(
    f"Chaves distintas apos a deduplicacao  : "
    f"{chaves_enq_canonico}"
)

duplicados_apos = (
    enq_canonico
    .groupBy("cnpj_chave")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    f"Chaves duplicadas apos deduplicacao    : "
    f"{duplicados_apos}"
)

print("\nExemplos das escolhas para CNPJs antes duplicados:")

cnpjs_exemplo = [
    "59285411",
    "60872504",
    "90400888",
    "92702067",
    "58160789",
]

(
    enq_canonico
    .filter(
        col("cnpj_chave").isin(
            cnpjs_exemplo
        )
    )
    .select(
        "cnpj_chave",
        "segmento",
        "nome"
    )
    .orderBy("cnpj_chave")
    .show(
        50,
        truncate=False
    )
)


# ============================================================================
# 3. TESTE DO JOIN COM CNPJ
# ============================================================================

print("\n" + "=" * 100)
print("3. TESTE RECLAMACOES COM CNPJ X ENQUADRAMENTO CANONICO")
print("=" * 100)

rec_cnpj = com_cnpj.withColumn(
    "cnpj_chave",
    normalizar_cnpj(
        col("cnpj_if")
    )
)

quantidade_antes = rec_cnpj.count()

teste_cnpj = (
    rec_cnpj.alias("r")
    .join(
        enq_canonico.alias("e"),
        on="cnpj_chave",
        how="left"
    )
)

quantidade_depois = teste_cnpj.count()

linhas_encontradas = (
    teste_cnpj
    .filter(
        col("e.nome").isNotNull()
    )
    .count()
)

linhas_nao_encontradas = (
    teste_cnpj
    .filter(
        col("e.nome").isNull()
    )
    .count()
)

print(
    f"Linhas antes do join       : "
    f"{quantidade_antes}"
)

print(
    f"Linhas depois do join      : "
    f"{quantidade_depois}"
)

print(
    f"Linhas com enquadramento   : "
    f"{linhas_encontradas}"
)

print(
    f"Linhas sem enquadramento   : "
    f"{linhas_nao_encontradas}"
)

if quantidade_antes == quantidade_depois:
    print(
        "Cardinalidade do join por CNPJ: PRESERVADA"
    )
else:
    print(
        "Cardinalidade do join por CNPJ: ALTERADA"
    )


# ============================================================================
# 4. NOMES DOS CONGLOMERADOS
# ============================================================================

print("\n" + "=" * 100)
print("4. PERFIL DOS NOMES DAS RECLAMACOES SEM CNPJ")
print("=" * 100)

rec_nomes = (
    sem_cnpj
    .withColumn(
        "nome_chave",
        normalizar_nome(
            col("instituicao_financeira")
        )
    )
)

nomes_distintos = (
    rec_nomes
    .select("nome_chave")
    .distinct()
    .count()
)

print(
    f"Nomes distintos sem CNPJ: "
    f"{nomes_distintos}"
)

print("\nExemplos de normalizacao:")

(
    rec_nomes
    .select(
        "instituicao_financeira",
        "nome_chave"
    )
    .distinct()
    .orderBy("nome_chave")
    .show(
        30,
        truncate=False
    )
)


# ============================================================================
# 5. PREPARACAO DO GLASSDOOR
# ============================================================================

print("\n" + "=" * 100)
print("5. CHAVES DO GLASSDOOR MATCH")
print("=" * 100)

gd = glassdoor.withColumn(
    "nome_chave",
    normalizar_nome(
        col("nome")
    )
)

gd_nomes = (
    gd
    .select("nome_chave")
    .distinct()
)

print(
    f"Registros Glassdoor         : "
    f"{gd.count()}"
)

print(
    f"Nomes distintos Glassdoor   : "
    f"{gd_nomes.count()}"
)

duplicados_gd = (
    gd
    .groupBy("nome_chave")
    .count()
    .filter(col("count") > 1)
)

print(
    f"Nomes duplicados Glassdoor  : "
    f"{duplicados_gd.count()}"
)

print("\nDuplicidades do Glassdoor:")

(
    gd
    .join(
        duplicados_gd.select("nome_chave"),
        on="nome_chave",
        how="inner"
    )
    .select(
        "nome_chave",
        "employer_name",
        "segmento",
        "nome",
        "match_percent"
    )
    .orderBy(
        "nome_chave",
        col("match_percent").desc()
    )
    .show(
        50,
        truncate=False
    )
)


# ============================================================================
# 6. COBERTURA DOS CONGLOMERADOS COM GLASSDOOR
# ============================================================================

print("\n" + "=" * 100)
print("6. RECLAMACOES SEM CNPJ X GLASSDOOR MATCH")
print("=" * 100)

rec_nomes_distintos = (
    rec_nomes
    .select("nome_chave")
    .distinct()
)

nomes_encontrados = (
    rec_nomes_distintos
    .join(
        gd_nomes,
        on="nome_chave",
        how="inner"
    )
)

nomes_nao_encontrados = (
    rec_nomes_distintos
    .join(
        gd_nomes,
        on="nome_chave",
        how="left_anti"
    )
)

total_nomes = rec_nomes_distintos.count()
total_encontrados = nomes_encontrados.count()
total_nao = nomes_nao_encontrados.count()

if total_nomes > 0:
    cobertura_nomes = (
        total_encontrados
        / total_nomes
        * 100
    )
else:
    cobertura_nomes = 0.0

print(
    f"Nomes distintos nas reclamacoes : "
    f"{total_nomes}"
)

print(
    f"Nomes encontrados no Glassdoor  : "
    f"{total_encontrados}"
)

print(
    f"Nomes sem Glassdoor             : "
    f"{total_nao}"
)

print(
    f"Cobertura por nome              : "
    f"{cobertura_nomes:.2f}%"
)

# ============================================================================
# 7. COBERTURA EM LINHAS DAS RECLAMACOES
# ============================================================================

print("\n" + "=" * 100)
print("7. COBERTURA GLASSDOOR EM LINHAS DE RECLAMACOES SEM CNPJ")
print("=" * 100)

teste_glassdoor = (
    rec_nomes.alias("r")
    .join(
        gd_nomes.alias("g"),
        on="nome_chave",
        how="left"
    )
)

linhas_sem_cnpj = teste_glassdoor.count()

linhas_com_glassdoor = (
    teste_glassdoor
    .filter(
        col("g.nome_chave").isNotNull()
    )
    .count()
)

linhas_sem_glassdoor = (
    linhas_sem_cnpj
    - linhas_com_glassdoor
)

if linhas_sem_cnpj > 0:
    cobertura_linhas = (
        linhas_com_glassdoor
        / linhas_sem_cnpj
        * 100
    )
else:
    cobertura_linhas = 0.0

print(
    f"Linhas sem CNPJ              : "
    f"{linhas_sem_cnpj}"
)

print(
    f"Linhas com Glassdoor         : "
    f"{linhas_com_glassdoor}"
)

print(
    f"Linhas sem Glassdoor         : "
    f"{linhas_sem_glassdoor}"
)

print(
    f"Cobertura em linhas          : "
    f"{cobertura_linhas:.2f}%"
)

print("\nNomes encontrados:")

(
    nomes_encontrados
    .orderBy("nome_chave")
    .show(
        100,
        truncate=False
    )
)

print("\nNomes sem correspondencia:")

(
    nomes_nao_encontrados
    .orderBy("nome_chave")
    .show(
        100,
        truncate=False
    )
)

print("\n" + "=" * 100)
print("DIAGNOSTICO_ESTRATEGIA_DELIVERY_OK")
print("=" * 100)

spark.stop()
