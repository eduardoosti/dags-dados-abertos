# DAGs de dados abertos

Pipelines Apache Airflow para ingestão de dados abertos do governo brasileiro no lakehouse.

## DAGs

| Fonte | Pipeline | Destino |
|---|---|---|
| Receita Federal | [CNPJ — instruções e validação](dags/receita_federal/README.md) | HDFS e tabelas Iceberg/Nessie |

A infraestrutura utilizada é [arquitetura-lakehouse](https://github.com/eduardoosti/arquitetura-lakehouse), com Airflow 3, Spark standalone, HDFS, Nessie e Trino. Este repositório guarda as DAGs e os respectivos scripts; a infraestrutura permanece no repositório próprio.

Copie a pasta dags/receita_federal para airflow/dags/receita_federal na infraestrutura. As configurações específicas estão no README da fonte.

## Organização

Cada fonte terá sua pasta em dags/, contendo a DAG, o script de processamento e instruções curtas. Credenciais, arquivos baixados e configurações locais não devem ser versionados.

## Créditos

A adaptação inicial do pipeline CNPJ deriva dos arquivos fornecidos por Eduardo William Alves Osti, cujo cabeçalho atribui a criação à Equipe IA / CGE (28/07/2026). A atribuição original foi preservada. A Receita Federal é a fonte dos dados, não a autora deste software. Nenhuma licença aberta foi atribuída ao código sem definição dos responsáveis.
