from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Atividade3_Diagnostico_Unicode")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 100)
print("ATIVIDADE 3 - DIAGNOSTICO DE CARACTERES U+FFFD")
print("=" * 100)

datasets = {
    "TRUSTED_RECLAMACOES":
        "/workspace/data/trusted/reclamacoes",

    "TRUSTED_ENQUADRAMENTO":
        "/workspace/data/trusted/enquadramento",

    "TRUSTED_GLASSDOOR_MATCH":
        "/workspace/data/trusted/glassdoor/match",

    "TRUSTED_GLASSDOOR_MATCH_LESS":
        "/workspace/data/trusted/glassdoor/match_less",

    "DELIVERY":
        "/workspace/data/delivery/reclamacoes_enriquecidas",
}

total_geral = 0

for nome_dataset, caminho in datasets.items():

    print("\n" + "=" * 100)
    print(f"DATASET: {nome_dataset}")
    print("=" * 100)

    df = spark.read.parquet(caminho)

    colunas_string = [
        campo.name
        for campo in df.schema.fields
        if campo.dataType.simpleString() == "string"
    ]

    encontrou = False

    for nome_coluna in colunas_string:

        filtro = (
            F.col(nome_coluna).isNotNull()
            & (
                F.instr(
                    F.col(nome_coluna),
                    "\ufffd"
                ) > 0
            )
        )

        quantidade = (
            df
            .filter(filtro)
            .count()
        )

        if quantidade > 0:

            encontrou = True
            total_geral += quantidade

            print(
                f"\nColuna: {nome_coluna}"
            )

            print(
                f"Linhas contendo U+FFFD: "
                f"{quantidade}"
            )

            colunas_exibicao = []

            candidatos = [
                "ano",
                "trimestre",
                "cnpj_if",
                "cnpj",
                "instituicao_financeira",
                "nome",
                "employer_name",
                "arquivo_origem",
                nome_coluna,
            ]

            for candidato in candidatos:

                if (
                    candidato in df.columns
                    and candidato not in colunas_exibicao
                ):
                    colunas_exibicao.append(
                        candidato
                    )

            linhas = (
                df
                .filter(filtro)
                .select(
                    *colunas_exibicao
                )
                .distinct()
                .collect()
            )

            print(
                "Valores encontrados "
                "(representacao Unicode segura):"
            )

            for linha in linhas:

                valores = []

                for campo in colunas_exibicao:

                    valor = linha[campo]

                    if isinstance(valor, str):
                        valor = ascii(valor)

                    valores.append(
                        f"{campo}={valor}"
                    )

                print(
                    " | ".join(valores)
                )

    if not encontrou:

        print(
            "Nenhuma ocorrencia de U+FFFD."
        )

print("\n" + "=" * 100)
print("RESUMO")
print("=" * 100)

print(
    f"Total de ocorrencias por celula "
    f"nos datasets analisados: {total_geral}"
)

print("DIAGNOSTICO_UNICODE_OK")
print("=" * 100)

spark.stop()
