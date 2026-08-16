from pathlib import Path
import hashlib

raizes = [
    Path("/mnt/d/Users/Breno Tostes/OneDrive"),
    Path("/mnt/d/GitHub"),
]

print("=" * 100)
print("ATIVIDADE 3 - BUSCA POR COPIA INTEGRA DO ENQUADRAMENTO")
print("=" * 100)

arquivos = []

for raiz in raizes:
    if not raiz.exists():
        continue

    try:
        arquivos.extend(
            raiz.rglob("Enquadramento*.tsv")
        )
    except Exception:
        pass

# Eliminar caminhos repetidos
arquivos = sorted(set(arquivos))

print(f"\nArquivos encontrados: {len(arquivos)}")

for i, arquivo in enumerate(arquivos, start=1):

    print("\n" + "-" * 100)
    print(f"ARQUIVO {i}")
    print("-" * 100)

    try:
        dados = arquivo.read_bytes()

        sha256 = hashlib.sha256(dados).hexdigest()

        qtd_fffd = dados.count(b"\xef\xbf\xbd")

        try:
            texto_utf8 = dados.decode(
                "utf-8",
                errors="strict"
            )
            utf8_valido = True
            qtd_unicode_fffd = texto_utf8.count("\ufffd")
        except UnicodeDecodeError:
            utf8_valido = False
            qtd_unicode_fffd = None

        print(f"Caminho: {arquivo}")
        print(f"Tamanho: {len(dados)} bytes")
        print(f"SHA-256: {sha256}")
        print(f"UTF-8 estrito valido: {utf8_valido}")
        print(
            "Sequencias EF BF BD nos bytes: "
            f"{qtd_fffd}"
        )

        if qtd_unicode_fffd is not None:
            print(
                "Caracteres U+FFFD apos UTF-8: "
                f"{qtd_unicode_fffd}"
            )

        if qtd_fffd == 0:
            print(
                "*** CANDIDATO A COPIA INTEGRA: "
                "nenhum U+FFFD fisicamente gravado. ***"
            )

            if utf8_valido:
                linhas = texto_utf8.splitlines()

                print("\nExemplos contendo RIBEIR ou TOP:")

                contador = 0

                for linha in linhas:
                    if (
                        "RIBEIR" in linha.upper()
                        or "TOP" in linha.upper()
                    ):
                        print(linha)
                        contador += 1

                        if contador >= 10:
                            break

        else:
            print(
                "[CORROMPIDO] Possui caracteres "
                "de substituicao gravados."
            )

    except Exception as erro:
        print(f"[ERRO] Nao foi possivel analisar: {erro}")

print("\n" + "=" * 100)
print("BUSCA CONCLUIDA")
print("=" * 100)
