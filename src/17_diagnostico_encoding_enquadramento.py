from pathlib import Path


arquivo = Path("data/input/EnquadramentoInicia_v2.tsv")

dados = arquivo.read_bytes()

print("=" * 80)
print("ATIVIDADE 3 - DIAGNOSTICO DE ENCODING DO ENQUADRAMENTO")
print("=" * 80)

print(f"\nArquivo: {arquivo}")
print(f"Tamanho em bytes: {len(dados)}")

# ------------------------------------------------------------------
# 1. Verificar BOM
# ------------------------------------------------------------------

print("\n" + "=" * 80)
print("1. BOM")
print("=" * 80)

if dados.startswith(b"\xef\xbb\xbf"):
    print("UTF-8 BOM detectado.")
elif dados.startswith(b"\xff\xfe"):
    print("UTF-16 LE BOM detectado.")
elif dados.startswith(b"\xfe\xff"):
    print("UTF-16 BE BOM detectado.")
else:
    print("Nenhum BOM conhecido detectado.")

# ------------------------------------------------------------------
# 2. Verificar se o caractere de substituicao ja esta fisicamente
#    gravado no arquivo como UTF-8: EF BF BD
# ------------------------------------------------------------------

print("\n" + "=" * 80)
print("2. CARACTERE DE SUBSTITUICAO GRAVADO NO ARQUIVO")
print("=" * 80)

qtd_replacement_bytes = dados.count(b"\xef\xbf\xbd")

print(
    "Ocorrencias da sequencia UTF-8 EF BF BD "
    f"(caractere U+FFFD): {qtd_replacement_bytes}"
)

# ------------------------------------------------------------------
# 3. Testar decodificacoes estritas
# ------------------------------------------------------------------

print("\n" + "=" * 80)
print("3. TESTE DE DECODIFICACAO")
print("=" * 80)

codificacoes = [
    "utf-8",
    "cp1252",
    "latin-1"
]

resultados = {}

for encoding in codificacoes:

    print(f"\nTestando: {encoding}")

    try:
        texto = dados.decode(
            encoding,
            errors="strict"
        )

        resultados[encoding] = texto

        print("[OK] Decodificacao estrita concluida.")

        qtd_replacement = texto.count("\ufffd")

        print(
            "Caracteres U+FFFD apos decodificacao: "
            f"{qtd_replacement}"
        )

    except UnicodeDecodeError as erro:

        resultados[encoding] = None

        print("[FALHA] Decodificacao estrita falhou.")
        print(
            f"Posicao aproximada do erro: {erro.start}"
        )
        print(
            f"Motivo: {erro.reason}"
        )

# ------------------------------------------------------------------
# 4. Procurar casos conhecidos em cada decodificacao valida
# ------------------------------------------------------------------

print("\n" + "=" * 80)
print("4. CASOS ESPECIFICOS")
print("=" * 80)

termos = [
    "RIBEIR",
    "TOP",
    "CR",
    "SÃO",
    "S�O"
]

for encoding, texto in resultados.items():

    if texto is None:
        continue

    print("\n" + "-" * 80)
    print(f"ENCODING: {encoding}")
    print("-" * 80)

    linhas = texto.splitlines()

    selecionadas = []

    for linha in linhas:

        linha_upper = linha.upper()

        if (
            "RIBEIR" in linha_upper
            or "TOP" in linha_upper
        ):
            selecionadas.append(linha)

    for linha in selecionadas[:20]:
        print(linha)

# ------------------------------------------------------------------
# 5. Resumo
# ------------------------------------------------------------------

print("\n" + "=" * 80)
print("RESUMO")
print("=" * 80)

utf8_ok = resultados["utf-8"] is not None
cp1252_ok = resultados["cp1252"] is not None

print(f"UTF-8 estrito valido: {utf8_ok}")
print(f"CP1252 estrito valido: {cp1252_ok}")
print(
    "EF BF BD fisicamente presente: "
    f"{qtd_replacement_bytes}"
)

if (
    not utf8_ok
    and cp1252_ok
    and qtd_replacement_bytes == 0
):
    print(
        "\n[INDICIO FORTE] O arquivo aparenta estar em CP1252 "
        "e foi interpretado incorretamente como UTF-8."
    )

elif (
    utf8_ok
    and qtd_replacement_bytes > 0
):
    print(
        "\n[ATENCAO] O caractere U+FFFD parece ja estar "
        "gravado fisicamente no arquivo."
    )

else:
    print(
        "\n[ATENCAO] O resultado requer avaliacao adicional "
        "antes de definir a codificacao."
    )

print("\nDIAGNOSTICO DE ENCODING CONCLUIDO.")
