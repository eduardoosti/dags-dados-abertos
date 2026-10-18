# Receita Federal no lakehouse Docker

Adaptação direta dos dois arquivos originais. Crédito original: Equipe IA / CGE.

## Instalação

1. Copie os dois arquivos .py para arquitetura-lakehouse/airflow/dags/receita_federal/.
2. No environment do serviço airflow-worker, acrescente RFB_BASE_URL com a URL oficial até a pasta CNPJ, sem o mês e sem o nome do ZIP. É o mesmo endereço base usado no original; o identificador de compartilhamento não foi incluído neste pacote.
3. Recrie o worker com docker compose up -d airflow-worker. A imagem do repositório já contém Java e PySpark.

A DAG continua sendo ingestao_receitafed_cnpj, com início, processamento e fim. Para disparar uma competência específica, informe {"reference_month": "2026-09"}. O agendamento continua no último dia do mês, às 05h, no fuso configurado no Airflow.

## O que mudou

- DataprocSubmitJobOperator virou BashOperator com spark-submit para spark-master:7077.
- Endereços HDFS e Nessie passaram aos nomes dos serviços Docker e às bibliotecas do notebook da infraestrutura.
- run_hdfs_cmd usa a JVM do Spark, pois a imagem Airflow não contém o executável hdfs. O teste com glob foi preservado usando globStatus.
- Mantidos os dois loops originais, 37 arquivos, dez esquemas, caminhos por ano/mês, coluna dtca e particionamentos.
- HTTPS usa validação normal; falhas de download/extração/upload interrompem a tarefa.

Não há classes novas, argparse, framework, Compose adicional ou configuração de catálogo.

## Verificação

Esta versão foi testada no Docker: uma execução agendada e uma manual terminaram com success no Airflow, incluindo os passos início e fim. O Trino confirmou dez tabelas, 1.395 registros e dtca preenchido em todas as linhas após a reexecução, sem duplicação. Foram usados Cnaes.zip real de 2026-09 (1.359 linhas) e 36 arquivos sintéticos. A carga nacional completa não foi executada. Os dez esquemas foram comparados com os originais e preservados.
