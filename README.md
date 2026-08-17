# Atividade 3 — Ingestão e ETL com Python e Apache Spark

## Especialização em Engenharia de Dados & Big Data — PECE/Poli/USP

Este repositório apresenta o desenvolvimento da **Atividade 3 da disciplina de Ingestão de Dados e Pipeline**, realizada no âmbito da Especialização em Engenharia de Dados & Big Data do PECE/Poli/USP.

O trabalho teve como objetivo implementar um pipeline de ingestão, tratamento, integração e disponibilização de dados utilizando **Python e Apache Spark**, com persistência intermediária em **Parquet** e disponibilização final em **PostgreSQL**.

Todas as operações de tratamento, padronização, integração e enriquecimento dos dados foram realizadas com **Apache Spark**. O PostgreSQL foi utilizado exclusivamente como banco de dados relacional para armazenamento e validação da camada final.

---

## 1. Objetivo da atividade

A atividade consistiu na construção de um pipeline de dados organizado em três camadas:

- **RAW**: preservação dos dados ingeridos;
- **Trusted**: tratamento, tipagem, limpeza e padronização;
- **Delivery**: integração, enriquecimento e disponibilização dos dados finais.

A camada Delivery foi produzida em formato **Parquet** e também persistida em uma tabela PostgreSQL por meio de conexão JDBC.

---

## 2. Tecnologias utilizadas

O ambiente utilizado no desenvolvimento foi composto por:

- Python;
- PySpark;
- Apache Spark 4.1.3;
- PostgreSQL 17;
- PostgreSQL JDBC Driver 42.7.13;
- Docker;
- Docker Compose;
- Windows 11;
- WSL2;
- Git;
- GitHub.

O Spark foi executado em modo local com a seguinte configuração:

```text
master = local[2]
driver-memory = 1g
```

Essa configuração foi suficiente para o volume de dados processado e adequada aos recursos disponíveis no ambiente acadêmico utilizado.

---

## 3. Arquitetura do pipeline

```text
Arquivos CSV / TSV
        |
        v
+------------------+
|       RAW        |
|     Parquet      |
+------------------+
        |
        v
+------------------+
|     TRUSTED      |
| limpeza, tipos,  |
| padronização     |
+------------------+
        |
        v
+------------------+
|     DELIVERY     |
| integração e     |
| enriquecimento   |
+------------------+
        |
        +--------------------+
        |                    |
        v                    v
     Parquet             PostgreSQL
                         public.
                         delivery_reclamacoes_
                         enriquecidas
```

---

## 4. Dados de entrada

Foram utilizados dez arquivos de entrada.

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

Não havia registros disponibilizados para o segundo trimestre de 2022 no conjunto de dados utilizado.

### Enquadramento das instituições

```text
EnquadramentoInicia_v2.tsv
```

### Glassdoor

```text
glassdoor_consolidado_join_match_v2.csv
glassdoor_consolidado_join_match_less_v2.csv
```

Antes do processamento foram realizadas verificações de quantidade de registros, delimitadores, estrutura dos arquivos e integridade das cópias utilizadas na atividade.

---

## 5. Camada RAW

A camada RAW foi utilizada para armazenar os dados ingeridos em formato Parquet antes da aplicação das principais regras de tratamento.

A quantidade total validada foi:

| Conjunto | Registros |
|---|---:|
| Reclamações | 918 |
| Enquadramento | 1.474 |
| Glassdoor Match | 34 |
| Glassdoor Match Less | 5 |
| **Total** | **2.431** |

Nos arquivos de reclamações foi identificada uma coluna adicional vazia, gerada pela presença de um delimitador ao final das linhas dos arquivos CSV.

Essa coluna foi preservada inicialmente na RAW e removida somente na camada Trusted, após a validação de que permanecia vazia em todos os 918 registros.

---

## 6. Camada Trusted

A camada Trusted concentrou as operações de tratamento e padronização realizadas com Spark.

Entre as principais ações executadas destacam-se:

- padronização dos nomes das colunas;
- tratamento de espaços e valores nulos;
- conversão dos tipos de dados;
- normalização de CNPJ;
- conversão de campos quantitativos para tipos numéricos;
- tratamento do índice de reclamações;
- preparação de chaves para integração;
- padronização de nomes;
- preservação da rastreabilidade por arquivo de origem.

A quantidade de registros foi integralmente preservada:

| Conjunto Trusted | Registros |
|---|---:|
| Reclamações | 918 |
| Enquadramento | 1.474 |
| Glassdoor Match | 34 |
| Glassdoor Match Less | 5 |
| **Total** | **2.431** |

---

## 7. Tratamento do índice de reclamações

Durante a transformação foram identificados diferentes formatos no campo de índice, incluindo valores como:

```text
16.699,13
14.015,05
2.055,01
```

Como os valores utilizavam convenção numérica brasileira, foi aplicada a seguinte regra:

1. remoção do ponto utilizado como separador de milhar;
2. substituição da vírgula decimal por ponto;
3. conversão para tipo numérico.

Após a aplicação da regra, não permaneceram valores inválidos decorrentes desse processo de conversão.

---

## 8. Estratégia de integração

A análise dos dados mostrou a existência de dois grupos distintos na base de reclamações.

### 8.1. Registros com CNPJ

Foram identificados:

```text
437 registros com CNPJ
```

Esses registros foram integrados à base de Enquadramento utilizando uma chave de CNPJ padronizada.

Resultado:

```text
321 registros com Enquadramento encontrado
116 registros sem correspondência
```

### 8.2. Registros sem CNPJ

Foram identificados:

```text
481 registros sem CNPJ
```

Todos esses registros pertenciam ao tipo:

```text
Conglomerado
```

Para esse grupo, a integração foi realizada com a base Glassdoor utilizando nomes normalizados.

Resultado:

```text
113 registros com Glassdoor encontrado
368 registros sem correspondência
```

Após o processamento independente dos dois grupos, os resultados foram novamente unidos com Spark.

---

## 9. Controle de duplicidades

A base de Enquadramento apresentava ocorrências repetidas após a normalização das chaves de CNPJ.

Para evitar multiplicação de registros durante os joins, foi construída uma referência canônica com regra determinística, priorizando registros que não continham a indicação:

```text
PRUDENCIAL
```

Também foram identificados nomes duplicados na base Glassdoor.

Para seleção da referência utilizada no enriquecimento foram aplicados os seguintes critérios:

1. maior percentual de correspondência;
2. maior número de avaliações;
3. nome da organização como critério adicional de desempate.

Com essas regras foi possível preservar a cardinalidade da base principal.

---

## 10. Uso da base Glassdoor Match Less

O arquivo:

```text
glassdoor_consolidado_join_match_less_v2.csv
```

foi mantido na camada Trusted, porém não foi utilizado automaticamente para enriquecimento da Delivery.

A análise mostrou associações de menor confiabilidade semântica. Dessa forma, foi adotado um critério conservador, evitando incorporar à tabela final relações cuja qualidade não pudesse ser suficientemente sustentada.

---

## 11. Camada Delivery

A camada Delivery consolidou os dois métodos de enriquecimento.

Resultado final:

```text
Registros = 918
Colunas   = 47
```

Distribuição dos métodos:

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

A cobertura global foi de:

```text
47,28%
```

A cardinalidade da base principal foi preservada:

```text
Reclamações de entrada = 918
Delivery               = 918
```

Nenhuma reclamação foi eliminada em razão da ausência de correspondência nas bases auxiliares.

---

## 12. Persistência no PostgreSQL

A camada Delivery também foi armazenada no PostgreSQL.

Banco de dados:

```text
atividade3
```

Tabela:

```text
public.delivery_reclamacoes_enriquecidas
```

A conexão entre Spark e PostgreSQL foi realizada por JDBC.

As validações apresentaram:

```text
Delivery Parquet    = 918 registros
Delivery PostgreSQL = 918 registros

Parquet             = 47 colunas
PostgreSQL          = 47 colunas
```

A equivalência entre a estrutura Parquet e a tabela PostgreSQL também foi validada.

---

## 13. Qualidade textual e limitação da fonte

Durante a validação final foi identificada a presença do caractere Unicode de substituição:

```text
U+FFFD
�
```

em nomes provenientes do arquivo de Enquadramento.

A investigação em nível de bytes identificou no próprio arquivo de entrada:

```text
3.109 ocorrências da sequência EF BF BD
```

Foram identificadas:

```text
950 linhas afetadas
```

na camada Trusted de Enquadramento.

Outras cópias disponíveis do arquivo apresentaram a mesma quantidade de ocorrências. Após a normalização das quebras de linha, os conteúdos comparados mostraram-se idênticos.

Dessa forma, concluiu-se que a perda dos caracteres já estava presente na fonte antes do processamento realizado nesta atividade.

Não foram realizadas substituições heurísticas para tentar reconstruir os caracteres, pois esse procedimento poderia introduzir informações não comprovadas.

Na Delivery, a limitação foi propagada somente pelos campos derivados do Enquadramento:

```text
nome_enquadramento = 10 registros
nome_referencia    = 10 registros
```

Nenhuma ocorrência foi identificada fora do ramo de integração baseado em CNPJ e Enquadramento.

---

## 14. Validação final do pipeline

A validação consolidada apresentou:

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

O resultado confirma a consistência estrutural do pipeline e a preservação da cardinalidade dos dados.

---

## 15. Estrutura do projeto

```text
Atividade_3-Ingestao_ETL_Python_Spark
│
├── data
│   ├── input
│   ├── raw
│   ├── trusted
│   └── delivery
│
├── evidencias
│   ├── 01_ambiente
│   ├── 02_docker
│   ├── 03_spark
│   ├── 04_postgresql
│   ├── 05_raw
│   ├── 06_trusted
│   ├── 07_delivery
│   ├── 08_validacao
│   └── 09_entrega
│
├── logs
├── relatorio
├── src
│
├── .gitignore
├── docker-compose.yml
└── README.md
```

As camadas geradas pelo processamento não precisam ser versionadas, pois podem ser reproduzidas pelos scripts do pipeline.

---

## 16. Scripts

A pasta `src/` contém os códigos utilizados no desenvolvimento, incluindo scripts de:

- teste do Spark;
- teste de gravação Parquet;
- inspeção dos arquivos;
- diagnóstico de encoding;
- criação da camada RAW;
- validação da RAW;
- criação das camadas Trusted;
- diagnóstico e padronização das chaves;
- análise da estratégia de integração;
- criação da Delivery;
- testes JDBC;
- gravação no PostgreSQL;
- diagnóstico de qualidade textual;
- validação final consolidada.

Os scripts de diagnóstico foram preservados como parte das evidências metodológicas do processo de desenvolvimento.

---

## 17. Execução do ambiente

Para iniciar o PostgreSQL:

```powershell
docker compose up -d postgres
```

Para verificar os serviços:

```powershell
docker compose ps
```

Exemplo de execução de um script Spark:

```powershell
docker compose run --rm spark `
  /opt/spark/bin/spark-submit `
  --master "local[2]" `
  --driver-memory 1g `
  /workspace/src/05_criar_raw.py
```

Para scripts que utilizam conexão com PostgreSQL:

```powershell
docker compose run --rm spark `
  /opt/spark/bin/spark-submit `
  --master "local[2]" `
  --driver-memory 1g `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.postgresql:postgresql:42.7.13 `
  /workspace/src/16_gravar_delivery_postgresql.py
```

---

## 18. Evidências

A pasta `evidencias/` reúne os logs e registros produzidos ao longo do desenvolvimento.

As evidências documentam:

- configuração do ambiente;
- execução do Docker;
- funcionamento do Spark;
- inspeção dos dados;
- integridade das fontes;
- criação da RAW;
- tratamento da Trusted;
- diagnósticos de integração;
- criação da Delivery;
- conexão JDBC;
- persistência no PostgreSQL;
- problemas encontrados durante o desenvolvimento;
- diagnóstico da qualidade textual;
- validação final do pipeline.

Essas evidências constituem parte do processo de rastreabilidade e reprodutibilidade do trabalho.

---

## 19. Observação de segurança

As credenciais utilizadas no PostgreSQL:

```text
usuario: postgres
senha: postgres
```

foram adotadas exclusivamente para o ambiente acadêmico local.

Em ambiente produtivo, credenciais não devem ser armazenadas diretamente em arquivos de configuração ou código-fonte, devendo ser utilizadas variáveis de ambiente ou soluções específicas de gerenciamento de segredos.

---

## 20. Resultado final

O pipeline desenvolvido atendeu aos objetivos propostos para a Atividade 3, contemplando:

- ingestão de fontes heterogêneas;
- organização dos dados em RAW, Trusted e Delivery;
- tratamento integral com Apache Spark;
- armazenamento intermediário em Parquet;
- integração de múltiplas fontes;
- tratamento de duplicidades;
- preservação da cardinalidade;
- enriquecimento dos dados;
- disponibilização final no PostgreSQL;
- documentação das limitações de qualidade existentes nas fontes;
- geração de evidências de execução e validação.

O resultado final contém **918 registros e 47 colunas**, com correspondência de enriquecimento para **434 registros**, equivalente a uma cobertura global de **47,28%**.

A execução foi encerrada com:

```text
VALIDACAO_FINAL_PIPELINE_OK
```