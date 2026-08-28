\# Arquitetura da Solução



```mermaid

flowchart LR



&#x20;   BD\["Base dos Dados<br/>INEP / BigQuery"]

&#x20;   PY\["Python<br/>Ingestão Batch"]



&#x20;   SIM\["Simulador de Eventos<br/>Python"]

&#x20;   SQS\["Amazon SQS<br/>fila-alfabetizacao-eventos"]

&#x20;   LAMBDA\["AWS Lambda<br/>SQS → Bronze"]



&#x20;   BRONZE\_BATCH\["Amazon S3<br/>Bronze / Batch"]

&#x20;   BRONZE\_STREAM\["Amazon S3<br/>Bronze / Streaming"]



&#x20;   GLUE\_BS\["AWS Glue<br/>Bronze → Silver"]

&#x20;   GLUE\_STREAM\["AWS Glue<br/>Streaming → Silver/Gold"]



&#x20;   SILVER\["Amazon S3<br/>Silver"]

&#x20;   GLUE\_SG\["AWS Glue<br/>Silver → Gold"]



&#x20;   GOLD\["Amazon S3<br/>Gold"]



&#x20;   CATALOG\["AWS Glue<br/>Data Catalog / Crawlers"]

&#x20;   ATHENA\["Amazon Athena<br/>Consultas SQL"]



&#x20;   CLOUDWATCH\["Amazon CloudWatch<br/>Logs e Monitoramento"]



&#x20;   BD --> PY

&#x20;   PY --> BRONZE\_BATCH



&#x20;   SIM --> SQS

&#x20;   SQS --> LAMBDA

&#x20;   LAMBDA --> BRONZE\_STREAM



&#x20;   BRONZE\_BATCH --> GLUE\_BS

&#x20;   GLUE\_BS --> SILVER



&#x20;   BRONZE\_STREAM --> GLUE\_STREAM

&#x20;   GLUE\_STREAM --> SILVER

&#x20;   GLUE\_STREAM --> GOLD



&#x20;   SILVER --> GLUE\_SG

&#x20;   GLUE\_SG --> GOLD



&#x20;   BRONZE\_BATCH -.-> CATALOG

&#x20;   SILVER -.-> CATALOG

&#x20;   GOLD -.-> CATALOG



&#x20;   CATALOG --> ATHENA

&#x20;   GOLD --> ATHENA

&#x20;   SILVER --> ATHENA



&#x20;   LAMBDA -. Logs .-> CLOUDWATCH

&#x20;   GLUE\_BS -. Logs .-> CLOUDWATCH

&#x20;   GLUE\_SG -. Logs .-> CLOUDWATCH

&#x20;   GLUE\_STREAM -. Logs .-> CLOUDWATCH

```



\## Fluxo resumido



\### Batch



`Base dos Dados → Python → S3 Bronze → Glue → Silver → Glue → Gold → Athena`



\### Streaming



`Simulador → SQS → Lambda → S3 Bronze → Glue → Silver/Gold → Athena`



O Amazon CloudWatch é utilizado para observabilidade dos componentes da pipeline, principalmente AWS Lambda e AWS Glue.

