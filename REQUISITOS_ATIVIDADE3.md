# Atendimento explícito aos requisitos finais da Atividade 3

Este documento complementa o README e registra de forma objetiva como a implementação atende aos requisitos de entrega.

## Arquitetura final

```text
Arquivos CSV / TSV
        |
        v
RAW
formato livre
(Parquet adotado neste trabalho)
        |
        v
Trusted
Parquet
        |
        v
Delivery
Parquet
        |
        v
Tabela final tratada e unificada
        |
        +------------------------------+
        |                              |
        v                              v
Parquet                         PostgreSQL
                                public.tabela_final_reclamacoes
```

## Requisitos e implementação

| Requisito | Atendimento |
|---|---|
| Gerar uma tabela final com os dados tratados e unidos | `data/delivery/tabela_final_reclamacoes` |
| RAW em formato livre | Parquet foi adotado como formato da RAW |
| Trusted em Parquet | `data/trusted/` |
| Delivery em Parquet | `data/delivery/tabela_final_reclamacoes` |
| Delivery como tabela final em banco relacional | `public.tabela_final_reclamacoes` no PostgreSQL |
| Processamento dos dados com Spark | Implementado com PySpark/DataFrame API |
| Tratamento sem SQL | SQL não é utilizado como mecanismo de transformação |

## Tabela final

A tabela final é derivada da Delivery validada e preserva o produto final do pipeline:

- **918 registros**;
- **47 colunas**;
- dados de reclamações tratados;
- enriquecimento por Enquadramento quando existe correspondência por CNPJ;
- enriquecimento por Glassdoor para conglomerados sem CNPJ quando existe correspondência nominal;
- cardinalidade preservada em relação às 918 reclamações de entrada.

### Parquet

```text
data/delivery/tabela_final_reclamacoes
```

### PostgreSQL

```text
Banco: atividade3
Schema: public
Tabela: tabela_final_reclamacoes
Nome completo: public.tabela_final_reclamacoes
```

## Script de materialização e validação

O script abaixo lê a Delivery já tratada e unificada, materializa a tabela final em Parquet, grava a mesma estrutura no PostgreSQL e compara quantidade de registros, colunas e schema:

```text
src/20_materializar_tabela_final.py
```

Execução:

```powershell
docker compose run --rm spark `
  /opt/spark/bin/spark-submit `
  --master "local[2]" `
  --driver-memory 1g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.postgresql:postgresql:42.7.13 `
  /workspace/src/20_materializar_tabela_final.py
```

Ao final, o script emite o marcador:

```text
REQUISITOS_ATIVIDADE3_OK
```

O marcador somente é produzido após a validação de que a tabela final Parquet e a tabela final PostgreSQL possuem os mesmos 918 registros e o mesmo conjunto de 47 colunas.
