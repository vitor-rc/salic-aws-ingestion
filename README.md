# salic-aws-ingestion

Este projeto visa a ingestão dos dados da API do SALIC.

arquitetura base:

![Arquitetura Base](images/arquitetura_v0.png)

#WIP

###Now:
- Script para lidar com a API
- Ingestao dos dados de forma programática
- transformação e movimentação entre os buckets (camadas) com Spark

###Next:
- Automação com Airflow
- Camada de validação e Data Quality

##Later:
- Ingestão incremental
- Módulos de reprocessamento
- Camada de visualização (Metabase)
- Disponibilização faseada para a comunidade