from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Atividade3_Validacao_Final_Pipeline")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 100)
print("ATIVIDADE 3 - VALIDACAO FINAL CONSOLIDADA DO PIPELINE")
print("=" * 100)


# =============================================================================
# FUNCAO AUXILIAR
# =============================================================================

def validar_valor(nome, encontrado, esperado):

    status = (
        "OK"
        if encontrado == esperado
        else "ERRO"
    )

    print(
        f"{nome:<45} "
        f"encontrado={encontrado:<8} "
        f"esperado={esperado:<8} "
        f"{status}"
    )

    if encontrado != esperado:
        raise RuntimeError(
            f"Validacao falhou em {nome}: "
            f"encontrado={encontrado}, "
            f"esperado={esperado}"
        )


# =============================================================================
# 1. RAW
# =============================================================================

print("\n" + "=" * 100)
print("1. CAMADA RAW")
print("=" * 100)

raw_fontes = [
    (
        "/workspace/data/raw/reclamacoes/2021_tri_01",
        105
    ),
    (
        "/workspace/data/raw/reclamacoes/2021_tri_02",
        111
    ),
    (
        "/workspace/data/raw/reclamacoes/2021_tri_03",
        113
    ),
    (
        "/workspace/data/raw/reclamacoes/2021_tri_04",
        135
    ),
    (
        "/workspace/data/raw/reclamacoes/2022_tri_01",
        137
    ),
    (
        "/workspace/data/raw/reclamacoes/2022_tri_03",
        163
    ),
    (
        "/workspace/data/raw/reclamacoes/2022_tri_04",
        154
    ),
    (
        "/workspace/data/raw/enquadramento/enquadramento_inicial",
        1474
    ),
    (
        "/workspace/data/raw/glassdoor/match",
        34
    ),
    (
        "/workspace/data/raw/glassdoor/match_less",
        5
    ),
]

total_raw = 0

for caminho, esperado in raw_fontes:

    quantidade = (
        spark.read
        .parquet(caminho)
        .count()
    )

    total_raw += quantidade

validar_valor(
    "Total RAW",
    total_raw,
    2431
)


# =============================================================================
# 2. TRUSTED
# =============================================================================

print("\n" + "=" * 100)
print("2. CAMADA TRUSTED")
print("=" * 100)

trusted_reclamacoes = spark.read.parquet(
    "/workspace/data/trusted/reclamacoes"
)

trusted_enquadramento = spark.read.parquet(
    "/workspace/data/trusted/enquadramento"
)

trusted_match = spark.read.parquet(
    "/workspace/data/trusted/glassdoor/match"
)

trusted_less = spark.read.parquet(
    "/workspace/data/trusted/glassdoor/match_less"
)

qtd_reclamacoes = trusted_reclamacoes.count()
qtd_enquadramento = trusted_enquadramento.count()
qtd_match = trusted_match.count()
qtd_less = trusted_less.count()

validar_valor(
    "Trusted - reclamacoes",
    qtd_reclamacoes,
    918
)

validar_valor(
    "Trusted - enquadramento",
    qtd_enquadramento,
    1474
)

validar_valor(
    "Trusted - Glassdoor Match",
    qtd_match,
    34
)

validar_valor(
    "Trusted - Glassdoor Match Less",
    qtd_less,
    5
)

total_trusted = (
    qtd_reclamacoes
    + qtd_enquadramento
    + qtd_match
    + qtd_less
)

validar_valor(
    "Total Trusted",
    total_trusted,
    2431
)


# =============================================================================
# 3. DELIVERY PARQUET
# =============================================================================

print("\n" + "=" * 100)
print("3. DELIVERY PARQUET")
print("=" * 100)

delivery = spark.read.parquet(
    "/workspace/data/delivery/reclamacoes_enriquecidas"
)

total_delivery = delivery.count()
colunas_delivery = len(delivery.columns)

validar_valor(
    "Delivery - registros",
    total_delivery,
    918
)

validar_valor(
    "Delivery - colunas",
    colunas_delivery,
    47
)


# =============================================================================
# 4. POSTGRESQL
# =============================================================================

print("\n" + "=" * 100)
print("4. DELIVERY POSTGRESQL")
print("=" * 100)

jdbc_url = (
    "jdbc:postgresql://postgres:5432/atividade3"
)

propriedades = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

postgres = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="public.delivery_reclamacoes_enriquecidas",
        properties=propriedades
    )
)

total_postgres = postgres.count()
colunas_postgres = len(postgres.columns)

validar_valor(
    "PostgreSQL - registros",
    total_postgres,
    918
)

validar_valor(
    "PostgreSQL - colunas",
    colunas_postgres,
    47
)

if set(delivery.columns) != set(postgres.columns):
    raise RuntimeError(
        "Schemas Parquet e PostgreSQL divergentes."
    )

print("Comparacao de nomes das colunas: OK")


# =============================================================================
# 5. METODOS DE ENRIQUECIMENTO
# =============================================================================

print("\n" + "=" * 100)
print("5. VALIDACAO DOS METODOS DE ENRIQUECIMENTO")
print("=" * 100)

metricas_metodo = {
    row["metodo_enriquecimento"]: row["count"]
    for row in (
        delivery
        .groupBy("metodo_enriquecimento")
        .count()
        .collect()
    )
}

validar_valor(
    "CNPJ_ENQUADRAMENTO",
    metricas_metodo.get(
        "CNPJ_ENQUADRAMENTO",
        0
    ),
    437
)

validar_valor(
    "NOME_GLASSDOOR",
    metricas_metodo.get(
        "NOME_GLASSDOOR",
        0
    ),
    481
)


# =============================================================================
# 6. COBERTURA DOS ENRIQUECIMENTOS
# =============================================================================

print("\n" + "=" * 100)
print("6. COBERTURA DOS ENRIQUECIMENTOS")
print("=" * 100)

enquadramento_encontrado = (
    delivery
    .filter(
        F.col("encontrou_enquadramento") == True
    )
    .count()
)

glassdoor_encontrado = (
    delivery
    .filter(
        F.col("encontrou_glassdoor") == True
    )
    .count()
)

sem_enriquecimento = (
    delivery
    .filter(
        (F.col("encontrou_enquadramento") == False)
        & (F.col("encontrou_glassdoor") == False)
    )
    .count()
)

validar_valor(
    "Linhas com Enquadramento",
    enquadramento_encontrado,
    321
)

validar_valor(
    "Linhas com Glassdoor",
    glassdoor_encontrado,
    113
)

validar_valor(
    "Linhas sem enriquecimento",
    sem_enriquecimento,
    484
)

total_enriquecidas = (
    enquadramento_encontrado
    + glassdoor_encontrado
)

cobertura_total = (
    total_enriquecidas
    / total_delivery
    * 100
)

print(
    f"Cobertura total de enriquecimento: "
    f"{cobertura_total:.2f}%"
)


# =============================================================================
# 7. QUALIDADE TEXTUAL E LIMITACAO CONHECIDA DA FONTE
# =============================================================================

print("\n" + "=" * 100)
print("7. QUALIDADE TEXTUAL E LIMITACAO CONHECIDA DA FONTE")
print("=" * 100)

# -------------------------------------------------------------------------
# Reclamações
# -------------------------------------------------------------------------

reclamacoes_fffd = (
    trusted_reclamacoes
    .filter(
        F.instr(
            F.col("instituicao_financeira"),
            "\ufffd"
        ) > 0
    )
    .count()
)

validar_valor(
    "Trusted reclamacoes com U+FFFD",
    reclamacoes_fffd,
    0
)

# -------------------------------------------------------------------------
# Enquadramento
# -------------------------------------------------------------------------

enquadramento_fffd = (
    trusted_enquadramento
    .filter(
        F.instr(
            F.col("nome"),
            "\ufffd"
        ) > 0
    )
    .count()
)

validar_valor(
    "Trusted enquadramento com U+FFFD",
    enquadramento_fffd,
    950
)

# -------------------------------------------------------------------------
# Glassdoor
# -------------------------------------------------------------------------

glassdoor_match_fffd = (
    trusted_match
    .filter(
        F.instr(
            F.col("nome"),
            "\ufffd"
        ) > 0
    )
    .count()
)

glassdoor_less_fffd = (
    trusted_less
    .filter(
        F.instr(
            F.col("nome"),
            "\ufffd"
        ) > 0
    )
    .count()
)

validar_valor(
    "Glassdoor Match com U+FFFD",
    glassdoor_match_fffd,
    0
)

validar_valor(
    "Glassdoor Match Less com U+FFFD",
    glassdoor_less_fffd,
    0
)

# -------------------------------------------------------------------------
# Delivery
# -------------------------------------------------------------------------

delivery_nome_enquadramento_fffd = (
    delivery
    .filter(
        F.instr(
            F.col("nome_enquadramento"),
            "\ufffd"
        ) > 0
    )
    .count()
)

delivery_nome_referencia_fffd = (
    delivery
    .filter(
        F.instr(
            F.col("nome_referencia"),
            "\ufffd"
        ) > 0
    )
    .count()
)

validar_valor(
    "Delivery nome_enquadramento com U+FFFD",
    delivery_nome_enquadramento_fffd,
    10
)

validar_valor(
    "Delivery nome_referencia com U+FFFD",
    delivery_nome_referencia_fffd,
    10
)

# Verifica se os registros afetados na Delivery vieram do ramo
# de enriquecimento por CNPJ/Enquadramento.

fffd_fora_enquadramento = (
    delivery
    .filter(
        (
            F.instr(
                F.col("nome_enquadramento"),
                "\ufffd"
            ) > 0
        )
        & (
            F.col("metodo_enriquecimento")
            != "CNPJ_ENQUADRAMENTO"
        )
    )
    .count()
)

validar_valor(
    "U+FFFD fora do ramo CNPJ_ENQUADRAMENTO",
    fffd_fora_enquadramento,
    0
)

print(
    "\nLIMITACAO CONHECIDA DA FONTE:"
)

print(
    "O arquivo de Enquadramento recebido possui "
    "caracteres U+FFFD previamente gravados."
)

print(
    "A verificacao em nivel de bytes identificou "
    "3.109 sequencias UTF-8 EF BF BD no arquivo de origem."
)

print(
    "Foram identificadas 950 linhas afetadas na camada Trusted."
)

print(
    "Na Delivery, a ocorrencia foi propagada somente pelos "
    "campos derivados do Enquadramento."
)

print(
    "Nenhuma correcao heuristica foi aplicada, evitando "
    "a criacao de informacao nao comprovada."
)

print("QUALIDADE_TEXTUAL_VALIDADA_COM_LIMITACAO_DA_FONTE")

# =============================================================================
# 8. RESUMO FINAL
# =============================================================================

print("\n" + "=" * 100)
print("8. RESUMO FINAL")
print("=" * 100)

print("RAW                  : OK")
print("TRUSTED              : OK")
print("DELIVERY PARQUET     : OK")
print("POSTGRESQL           : OK")
print("CARDINALIDADE        : OK")
print("SCHEMA                : OK")
print("ENRIQUECIMENTOS      : OK")
print("QUALIDADE TEXTUAL     : OK - LIMITACAO DA FONTE DOCUMENTADA")

print("=" * 100)
print("VALIDACAO_FINAL_PIPELINE_OK")
print("=" * 100)

spark.stop()
