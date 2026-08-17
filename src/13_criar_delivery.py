from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    upper,
    trim,
    regexp_replace,
    translate,
    length,
    lpad,
    when,
    row_number
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("Atividade3_Camada_Delivery")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# CAMINHOS
# =============================================================================

reclamacoes_path = "/workspace/data/trusted/reclamacoes"

enquadramento_path = "/workspace/data/trusted/enquadramento"

glassdoor_path = "/workspace/data/trusted/glassdoor/match"

delivery_path = "/workspace/data/delivery/reclamacoes_enriquecidas"


# =============================================================================
# FUNCOES DE NORMALIZACAO
# =============================================================================

def normalizar_cnpj(coluna):

    digitos = regexp_replace(
        coluna,
        r"[^0-9]",
        ""
    )

    return when(
        (length(digitos) >= 1)
        & (length(digitos) <= 8),
        lpad(
            digitos,
            8,
            "0"
        )
    ).otherwise(None)


def normalizar_nome(coluna):

    resultado = upper(
        trim(coluna)
    )

    resultado = regexp_replace(
        resultado,
        r"\s*\(CONGLOMERADO\)\s*$",
        ""
    )

    resultado = translate(
        resultado,
        "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ",
        "AAAAAEEEEIIIIOOOOOUUUUC"
    )

    resultado = regexp_replace(
        resultado,
        r"[^A-Z0-9]+",
        " "
    )

    resultado = regexp_replace(
        resultado,
        r"\s+",
        " "
    )

    return trim(resultado)


# =============================================================================
# LEITURA
# =============================================================================

reclamacoes = spark.read.parquet(
    reclamacoes_path
)

enquadramento = spark.read.parquet(
    enquadramento_path
)

glassdoor = spark.read.parquet(
    glassdoor_path
)

print("=" * 100)
print("ATIVIDADE 3 - CRIACAO DA CAMADA DELIVERY")
print("=" * 100)

print("\nEntradas:")
print(f"Reclamacoes   : {reclamacoes.count()}")
print(f"Enquadramento : {enquadramento.count()}")
print(f"Glassdoor     : {glassdoor.count()}")


# =============================================================================
# 1. ENQUADRAMENTO CANONICO
# =============================================================================

print("\n" + "=" * 100)
print("1. PREPARACAO DO ENQUADRAMENTO CANONICO")
print("=" * 100)

enq = enquadramento.withColumn(
    "cnpj_chave",
    normalizar_cnpj(
        col("cnpj")
    )
)

enq = enq.filter(
    col("cnpj_chave").isNotNull()
    & (col("cnpj_chave") != "00000000")
)

# Registros institucionais têm prioridade
# sobre registros identificados como PRUDENCIAL.
enq = enq.withColumn(
    "prioridade",
    when(
        upper(col("nome")).contains("PRUDENCIAL"),
        2
    ).otherwise(1)
)

janela_enq = Window.partitionBy(
    "cnpj_chave"
).orderBy(
    col("prioridade").asc(),
    col("nome").asc()
)

enq_canonico = (
    enq
    .withColumn(
        "ordem",
        row_number().over(janela_enq)
    )
    .filter(
        col("ordem") == 1
    )
    .drop(
        "ordem",
        "prioridade"
    )
)

duplicados_enq = (
    enq_canonico
    .groupBy("cnpj_chave")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    f"Registros no enquadramento canonico: "
    f"{enq_canonico.count()}"
)

print(
    f"Chaves duplicadas apos tratamento : "
    f"{duplicados_enq}"
)

if duplicados_enq != 0:
    raise RuntimeError(
        "Enquadramento canonico ainda possui duplicidades."
    )


# =============================================================================
# 2. GLASSDOOR CANONICO
# =============================================================================

print("\n" + "=" * 100)
print("2. PREPARACAO DO GLASSDOOR CANONICO")
print("=" * 100)

gd = glassdoor.withColumn(
    "nome_chave",
    normalizar_nome(
        col("nome")
    )
)

# Para nomes duplicados:
# 1. maior percentual de match;
# 2. maior quantidade de reviews;
# 3. employer_name em ordem alfabetica.
janela_gd = Window.partitionBy(
    "nome_chave"
).orderBy(
    col("match_percent").desc_nulls_last(),
    col("reviews_count").desc_nulls_last(),
    col("employer_name").asc()
)

gd_canonico = (
    gd
    .withColumn(
        "ordem",
        row_number().over(janela_gd)
    )
    .filter(
        col("ordem") == 1
    )
    .drop("ordem")
)

duplicados_gd = (
    gd_canonico
    .groupBy("nome_chave")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    f"Registros Glassdoor originais : "
    f"{glassdoor.count()}"
)

print(
    f"Nomes Glassdoor canonicos     : "
    f"{gd_canonico.count()}"
)

print(
    f"Duplicidades apos tratamento  : "
    f"{duplicados_gd}"
)

if duplicados_gd != 0:
    raise RuntimeError(
        "Glassdoor canonico ainda possui duplicidades."
    )


# =============================================================================
# 3. RAMO A - RECLAMACOES COM CNPJ
# =============================================================================

print("\n" + "=" * 100)
print("3. ENRIQUECIMENTO DAS RECLAMACOES COM CNPJ")
print("=" * 100)

rec_cnpj = (
    reclamacoes
    .filter(
        col("cnpj_if").isNotNull()
    )
    .withColumn(
        "cnpj_chave",
        normalizar_cnpj(
            col("cnpj_if")
        )
    )
)

qtd_rec_cnpj = rec_cnpj.count()

delivery_cnpj = (
    rec_cnpj.alias("r")
    .join(
        enq_canonico.alias("e"),
        on="cnpj_chave",
        how="left"
    )
    .select(
        "r.*",

        col("e.segmento").alias(
            "segmento_enquadramento"
        ),

        col("e.nome").alias(
            "nome_enquadramento"
        )
    )
)


delivery_cnpj = delivery_cnpj.withColumn(
    "encontrou_enquadramento",
    col("nome_enquadramento").isNotNull()
)

delivery_cnpj = delivery_cnpj.withColumn(
    "encontrou_glassdoor",
    lit(False)
)

delivery_cnpj = delivery_cnpj.withColumn(
    "metodo_enriquecimento",
    lit("CNPJ_ENQUADRAMENTO")
)

delivery_cnpj = delivery_cnpj.withColumn(
    "chave_integracao",
    col("cnpj_chave")
)

delivery_cnpj = delivery_cnpj.withColumn(
    "segmento_final",
    col("segmento_enquadramento")
)

delivery_cnpj = delivery_cnpj.withColumn(
    "nome_referencia",
    col("nome_enquadramento")
)

qtd_delivery_cnpj = delivery_cnpj.count()

com_enquadramento = (
    delivery_cnpj
    .filter(
        col("encontrou_enquadramento")
    )
    .count()
)

sem_enquadramento = (
    qtd_delivery_cnpj
    - com_enquadramento
)

print(
    f"Linhas de entrada            : "
    f"{qtd_rec_cnpj}"
)

print(
    f"Linhas apos join             : "
    f"{qtd_delivery_cnpj}"
)

print(
    f"Com enquadramento            : "
    f"{com_enquadramento}"
)

print(
    f"Sem enquadramento            : "
    f"{sem_enquadramento}"
)

if qtd_rec_cnpj != qtd_delivery_cnpj:
    raise RuntimeError(
        "Join CNPJ alterou a cardinalidade."
    )


# =============================================================================
# 4. RAMO B - CONGLOMERADOS SEM CNPJ
# =============================================================================

print("\n" + "=" * 100)
print("4. ENRIQUECIMENTO DOS CONGLOMERADOS COM GLASSDOOR")
print("=" * 100)

rec_nome = (
    reclamacoes
    .filter(
        col("cnpj_if").isNull()
    )
    .withColumn(
        "nome_chave",
        normalizar_nome(
            col("instituicao_financeira")
        )
    )
)

qtd_rec_nome = rec_nome.count()

delivery_nome = (
    rec_nome.alias("r")
    .join(
        gd_canonico.alias("g"),
        on="nome_chave",
        how="left"
    )
    .select(
        "r.*",

        col("g.segmento").alias(
            "segmento_glassdoor"
        ),

        col("g.nome").alias(
            "nome_glassdoor"
        ),

        col("g.employer_name").alias(
            "glassdoor_employer_name"
        ),

        col("g.reviews_count").alias(
            "glassdoor_reviews_count"
        ),

        col("g.culture_count").alias(
            "glassdoor_culture_count"
        ),

        col("g.salaries_count").alias(
            "glassdoor_salaries_count"
        ),

        col("g.benefits_count").alias(
            "glassdoor_benefits_count"
        ),

        col("g.employer_website").alias(
            "glassdoor_website"
        ),

        col("g.employer_headquarters").alias(
            "glassdoor_headquarters"
        ),

        col("g.employer_founded").alias(
            "glassdoor_founded"
        ),

        col("g.employer_industry").alias(
            "glassdoor_industry"
        ),

        col("g.employer_revenue").alias(
            "glassdoor_revenue"
        ),

        col("g.geral").alias(
            "glassdoor_geral"
        ),

        col("g.cultura_valores").alias(
            "glassdoor_cultura_valores"
        ),

        col("g.diversidade_inclusao").alias(
            "glassdoor_diversidade_inclusao"
        ),

        col("g.qualidade_vida").alias(
            "glassdoor_qualidade_vida"
        ),

        col("g.alta_lideranca").alias(
            "glassdoor_alta_lideranca"
        ),

        col("g.remuneracao_beneficios").alias(
            "glassdoor_remuneracao_beneficios"
        ),

        col("g.oportunidades_carreira").alias(
            "glassdoor_oportunidades_carreira"
        ),

        col("g.recomendam_percentual").alias(
            "glassdoor_recomendam_percentual"
        ),

        col("g.perspectiva_positiva_percentual").alias(
            "glassdoor_perspectiva_positiva_percentual"
        ),

        col("g.match_percent").alias(
            "glassdoor_match_percent"
        )
    )
)

delivery_nome = delivery_nome.withColumn(
    "encontrou_enquadramento",
    lit(False)
)

delivery_nome = delivery_nome.withColumn(
    "encontrou_glassdoor",
    col("glassdoor_employer_name").isNotNull()
)

delivery_nome = delivery_nome.withColumn(
    "metodo_enriquecimento",
    lit("NOME_GLASSDOOR")
)

delivery_nome = delivery_nome.withColumn(
    "chave_integracao",
    col("nome_chave")
)

delivery_nome = delivery_nome.withColumn(
    "segmento_final",
    col("segmento_glassdoor")
)

delivery_nome = delivery_nome.withColumn(
    "nome_referencia",
    col("nome_glassdoor")
)

qtd_delivery_nome = delivery_nome.count()

com_glassdoor = (
    delivery_nome
    .filter(
        col("encontrou_glassdoor")
    )
    .count()
)

sem_glassdoor = (
    qtd_delivery_nome
    - com_glassdoor
)

print(
    f"Linhas de entrada            : "
    f"{qtd_rec_nome}"
)

print(
    f"Linhas apos join             : "
    f"{qtd_delivery_nome}"
)

print(
    f"Com Glassdoor                : "
    f"{com_glassdoor}"
)

print(
    f"Sem Glassdoor                : "
    f"{sem_glassdoor}"
)

if qtd_rec_nome != qtd_delivery_nome:
    raise RuntimeError(
        "Join Glassdoor alterou a cardinalidade."
    )


# =============================================================================
# 5. UNIAO DOS RAMOS
# =============================================================================

print("\n" + "=" * 100)
print("5. UNIAO E VALIDACAO DA DELIVERY")
print("=" * 100)

delivery = delivery_cnpj.unionByName(
    delivery_nome,
    allowMissingColumns=True
)

total_delivery = delivery.count()

print(
    f"Ramo CNPJ             : "
    f"{qtd_delivery_cnpj}"
)

print(
    f"Ramo nome             : "
    f"{qtd_delivery_nome}"
)

print(
    f"Total Delivery        : "
    f"{total_delivery}"
)

print(
    "Total esperado        : 918"
)

if total_delivery != 918:
    raise RuntimeError(
        f"Total incorreto na Delivery: {total_delivery}"
    )


# =============================================================================
# 6. METRICAS FINAIS
# =============================================================================

print("\n" + "=" * 100)
print("6. METRICAS FINAIS")
print("=" * 100)

(
    delivery
    .groupBy(
        "metodo_enriquecimento"
    )
    .count()
    .orderBy(
        "metodo_enriquecimento"
    )
    .show(
        truncate=False
    )
)

(
    delivery
    .groupBy(
        "metodo_enriquecimento",
        "encontrou_enquadramento",
        "encontrou_glassdoor"
    )
    .count()
    .orderBy(
        "metodo_enriquecimento",
        "encontrou_enquadramento",
        "encontrou_glassdoor"
    )
    .show(
        truncate=False
    )
)

print("\nSchema da Delivery:")

delivery.printSchema()

print("\nAmostra da Delivery:")

delivery.select(
    "ano",
    "trimestre",
    "tipo",
    "cnpj_if",
    "instituicao_financeira",
    "segmento_final",
    "nome_referencia",
    "metodo_enriquecimento",
    "encontrou_enquadramento",
    "encontrou_glassdoor",
    "glassdoor_geral",
    "glassdoor_match_percent"
).show(
    20,
    truncate=False
)


# =============================================================================
# 7. GRAVACAO PARQUET
# =============================================================================

print("\n" + "=" * 100)
print("7. GRAVACAO DA DELIVERY EM PARQUET")
print("=" * 100)

(
    delivery.write
    .mode("overwrite")
    .parquet(delivery_path)
)

validacao = spark.read.parquet(
    delivery_path
)

total_gravado = validacao.count()

print(
    f"Registros antes da gravacao: "
    f"{total_delivery}"
)

print(
    f"Registros apos a gravacao : "
    f"{total_gravado}"
)

if total_gravado != 918:
    raise RuntimeError(
        "Falha na validacao da gravacao da Delivery."
    )

print("=" * 100)
print("DELIVERY_PARQUET_OK")
print("=" * 100)

spark.stop()
