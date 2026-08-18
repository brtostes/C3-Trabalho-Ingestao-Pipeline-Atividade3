# Atividade 3 — Ingestão e ETL com Python e Apache Spark

## Especialização em Engenharia de Dados & Big Data — PECE/Poli/USP

Este repositório apresenta o desenvolvimento da **Atividade 3 da disciplina de Ingestão de Dados e Pipeline**, realizada no âmbito da Especialização em Engenharia de Dados & Big Data do PECE/Poli/USP.

O trabalho implementa um pipeline de ingestão, tratamento, integração e disponibilização de dados com **Python e Apache Spark**, estruturado nas camadas **RAW, Trusted e Delivery**, com persistência analítica em **Parquet** e disponibilização da tabela final em **PostgreSQL**.

Todas as operações de tratamento, padronização, integração e enriquecimento foram realizadas com a API de DataFrames do Spark. O PostgreSQL foi utilizado como banco relacional para armazenamento e validação da tabela final.

---

## 1. Atendimento aos requisitos da atividade

| Requisito | Atendimento |
|---|---|
| Gerar uma tabela final com os dados tratados e unidos | `data/delivery/tabela_final_reclamacoes` |
| RAW em formato livre | Parquet adotado neste trabalho |
| Trusted em Parquet | `data/trusted/` |
| Delivery em Parquet | `data/delivery/tabela_final_reclamacoes` |
| Delivery como tabela final em banco relacional | `public.tabela_final_reclamacoes` no PostgreSQL |
| Processamento com Python + Spark | PySpark/DataFrame API |
| Tratamento sem SQL | SQL não foi utilizado como mecanismo de transformação |

A materialização final foi validada com **918 registros e 47 colunas** tanto no Parquet quanto no PostgreSQL.

---

## 2. Arquitetura final do pipeline

```text
Arquivos CSV / TSV
        |
        v
RAW
formato livre
(Parquet adotado)
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
        +-------------------------------+
        |                               |
        v                               v
Parquet                         PostgreSQL
                               public.tabela_final_reclamacoes
```

### Tabela final Parquet

```text
data/delivery/tabela_final_reclamacoes
```

### Tabela final PostgreSQL

```text
Banco  : atividade3
Schema : public
Tabela : tabela_final_reclamacoes
Nome   : public.tabela_final_reclamacoes
```

---

## 3. Tecnologias utilizadas

- Python;
- PySpark;
- Apache Spark 4.1.3;
- PostgreSQL 17;
- PostgreSQL JDBC Driver 42.7.13;
- Docker e Docker Compose;
- Windows 11 e WSL2;
- Git e GitHub.

Configuração de execução do Spark:

```text
master = local[2]
driver-memory = 1g
```

---

## 4. Dados de entrada

Foram utilizados dez arquivos:

### Reclamações

```text
2021_tri_01.csv
2021_tri_02.csv
2021_tri_03.csv
2021_tri_04.csv
2022_tri_01.csv
2022_tri_03.csv
2022_tri_04.csv
```

Os sete arquivos totalizam **918 reclamações**. Não havia arquivo de reclamações do segundo trimestre de 2022 no conjunto disponibilizado.

### Enquadramento

```text
EnquadramentoInicia_v2.tsv
```

Registros: **1.474**.

### Glassdoor

```text
glassdoor_consolidado_join_match_v2.csv
glassdoor_consolidado_join_match_less_v2.csv
```

Registros: **34** e **5**, respectivamente.

Total de registros ingeridos na RAW: **2.431**.

---

## 5. Camada RAW

A RAW preserva os dados ingeridos antes das regras de tratamento. O requisito permitia formato livre; neste trabalho foi adotado **Parquet** também para essa camada, garantindo leitura eficiente e reprodutibilidade.

A camada contém:

| Conjunto | Registros |
|---|---:|
| Reclamações | 918 |
| Enquadramento | 1.474 |
| Glassdoor Match | 34 |
| Glassdoor Match Less | 5 |
| **Total** | **2.431** |

Nos arquivos de reclamações foi observada uma coluna adicional vazia causada por delimitador terminal. Ela foi preservada na RAW e removida somente na Trusted após validação de que estava vazia nos 918 registros.

---

## 6. Camada Trusted

A Trusted foi produzida em **Parquet** e concentrou as operações de tratamento e padronização realizadas com Spark, incluindo:

- padronização de nomes de colunas;
- tratamento de espaços e valores nulos;
- conversão de tipos;
- normalização de CNPJ;
- tratamento do índice de reclamações;
- preparação de chaves de integração;
- padronização de nomes;
- preservação de rastreabilidade por arquivo de origem.

As quantidades foram preservadas:

| Conjunto Trusted | Registros |
|---|---:|
| Reclamações | 918 |
| Enquadramento | 1.474 |
| Glassdoor Match | 34 |
| Glassdoor Match Less | 5 |
| **Total** | **2.431** |

---

## 7. Tratamento do índice de reclamações

Foram encontrados valores no padrão numérico brasileiro, como:

```text
16.699,13
14.015,05
2.055,01
```

A regra aplicada foi:

1. remover o ponto de milhar quando existia vírgula decimal;
2. substituir a vírgula decimal por ponto;
3. converter para tipo numérico.

Após o tratamento, não permaneceram valores inválidos decorrentes dessa conversão.

---

## 8. Estratégia de integração

As 918 reclamações foram separadas em dois ramos.

### Registros com CNPJ

```text
437 registros
```

Integração com a base de Enquadramento por CNPJ normalizado:

```text
321 com Enquadramento encontrado
116 sem correspondência
```

### Registros sem CNPJ

```text
481 registros
```

Todos pertenciam ao tipo `Conglomerado`. Para esse grupo, a integração foi realizada com a base Glassdoor por nome normalizado:

```text
113 com Glassdoor encontrado
368 sem correspondência
```

Os dois ramos foram unidos novamente com `unionByName`, preservando as **918 linhas**.

---

## 9. Controle de duplicidades

Antes dos joins foram criadas referências canônicas para evitar multiplicação de linhas.

No Enquadramento, registros não identificados como `PRUDENCIAL` receberam prioridade. Na Glassdoor Match, a seleção canônica priorizou:

1. maior `match_percent`;
2. maior `reviews_count`;
3. `employer_name` como desempate adicional.

Com isso, os joins mantiveram a cardinalidade da base principal.

O arquivo `glassdoor_consolidado_join_match_less_v2.csv` foi preservado na Trusted, mas não utilizado automaticamente no enriquecimento da Delivery por critério conservador de qualidade semântica.

---

## 10. Camada Delivery e tabela final

A Delivery foi gerada em **Parquet** com os dados tratados, integrados e enriquecidos.

Resultado:

```text
Registros = 918
Colunas   = 47
```

Distribuição dos métodos de integração:

| Método | Registros |
|---|---:|
| CNPJ_ENQUADRAMENTO | 437 |
| NOME_GLASSDOOR | 481 |
| **Total** | **918** |

Resultados dos enriquecimentos:

| Resultado | Registros |
|---|---:|
| Enquadramento encontrado | 321 |
| Glassdoor encontrado | 113 |
| Sem enriquecimento | 484 |

Total enriquecido: **434 registros**, correspondente a **47,28%** das reclamações.

O script `src/20_materializar_tabela_final.py` materializa explicitamente essa Delivery como tabela final tratada e unificada em:

```text
data/delivery/tabela_final_reclamacoes
```

---

## 11. Persistência da tabela final no PostgreSQL

A mesma estrutura final foi gravada por JDBC no banco relacional:

```text
public.tabela_final_reclamacoes
```

A execução confirmou:

```text
Parquet    : 918 registros / 47 colunas
PostgreSQL : 918 registros / 47 colunas
Schema     : equivalente
```

Uma validação independente pelo cliente `psql` confirmou posteriormente:

```text
public | tabela_final_reclamacoes | table | postgres
registros = 918
colunas   = 47
```

Portanto, o requisito de disponibilização da camada Delivery como **tabela final em banco de dados relacional** está materialmente atendido.

---

## 12. Qualidade textual e limitação da fonte

Foi identificada a presença do caractere Unicode de substituição `U+FFFD` em nomes provenientes da base de Enquadramento.

A análise em nível de bytes identificou **3.109 ocorrências da sequência UTF-8 EF BF BD** no próprio arquivo de origem e **950 linhas afetadas** na Trusted de Enquadramento.

Na Delivery, a limitação atingiu 10 registros em `nome_enquadramento` e 10 em `nome_referencia`, exclusivamente no ramo de Enquadramento.

Não foi aplicada correção heurística, evitando introdução de informação não comprovada.

---

## 13. Validação final

A validação consolidada do pipeline apresentou:

```text
RAW                  : OK
TRUSTED              : OK
DELIVERY PARQUET     : OK
POSTGRESQL           : OK
CARDINALIDADE        : OK
SCHEMA               : OK
ENRIQUECIMENTOS      : OK
QUALIDADE TEXTUAL    : OK - LIMITACAO DA FONTE DOCUMENTADA

VALIDACAO_FINAL_PIPELINE_OK
```

A etapa adicional de materialização formal da tabela final apresentou:

```text
RAW - FORMATO LIVRE       : OK - PARQUET ADOTADO
TRUSTED - PARQUET         : OK
DELIVERY - PARQUET        : OK
TABELA FINAL TRATADA      : OK
TABELA FINAL UNIFICADA    : OK
POSTGRESQL RELACIONAL     : OK
REGISTROS FINAIS          : 918
COLUNAS FINAIS            : 47
TABELA RELACIONAL         : public.tabela_final_reclamacoes

REQUISITOS_ATIVIDADE3_OK
```

---

## 14. Scripts

A pasta `src/` contém os scripts do pipeline e os diagnósticos realizados durante o desenvolvimento. Entre eles:

- criação e validação da RAW;
- criação das camadas Trusted;
- diagnóstico de encoding;
- tratamento do índice;
- diagnóstico das chaves;
- criação da Delivery;
- testes JDBC;
- gravação no PostgreSQL;
- validação final consolidada;
- `20_materializar_tabela_final.py` — materialização e validação da tabela final tratada e unificada.

---

## 15. Evidências finais

A pasta `evidencias/` mantém logs de ambiente, Docker, Spark, PostgreSQL, RAW, Trusted, Delivery, validações e entrega.

As duas evidências finais que comprovam explicitamente os requisitos adicionais são:

```text
evidencias/09_entrega/E36_materializacao_tabela_final.log
evidencias/09_entrega/E37_validacao_independente_tabela_final.log
```

A `E36` registra a execução Spark que materializou a tabela final em Parquet e PostgreSQL e terminou com `REQUISITOS_ATIVIDADE3_OK`.

A `E37` registra a consulta independente ao PostgreSQL que confirmou a existência de `public.tabela_final_reclamacoes`, com **918 registros e 47 colunas**.

---

## 16. Execução da etapa final

```powershell
docker compose run --rm spark `
  /opt/spark/bin/spark-submit `
  --master "local[2]" `
  --driver-memory 1g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.postgresql:postgresql:42.7.13 `
  /workspace/src/20_materializar_tabela_final.py
```

Validação independente no PostgreSQL:

```powershell
docker exec -i postgres_atividade3 `
  psql -U postgres -d atividade3 `
  -c "SELECT COUNT(*) FROM public.tabela_final_reclamacoes;"
```

---

## 17. Resultado final

A Atividade 3 resultou em um pipeline reprodutível com:

- RAW em formato livre, com Parquet adotado;
- Trusted em Parquet;
- Delivery em Parquet;
- tabela final tratada e unificada;
- **918 registros e 47 colunas**;
- persistência da mesma tabela em `public.tabela_final_reclamacoes` no PostgreSQL;
- processamento de dados integralmente realizado com Spark;
- validações de cardinalidade, schema, enriquecimento e qualidade textual;
- evidências técnicas da materialização e da validação independente.

A execução final foi encerrada com:

```text
REQUISITOS_ATIVIDADE3_OK
```
