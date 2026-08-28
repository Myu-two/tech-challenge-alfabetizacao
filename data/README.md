\\# Dados do Projeto







Os dados utilizados neste projeto são provenientes da plataforma \\\*\\\*Base dos Dados\\\*\\\*, utilizando o dataset de Avaliação da Alfabetização do INEP.







\\## Fonte







Dataset:







`br\\\_inep\\\_avaliacao\\\_alfabetizacao`







As principais entidades utilizadas são:







\\- UF



\\- Município



\\- Meta de Alfabetização Brasil



\\- Meta de Alfabetização por UF



\\- Meta de Alfabetização por Município



\\- Dados de alunos







\\## Armazenamento







Os dados não são versionados diretamente neste repositório.







A ingestão Batch é realizada pelo script:







`src/ingestion/batch/main.py`







Após a coleta, os dados são enviados para o Amazon S3 e organizados segundo a Arquitetura Medalhão:







```text



S3



├── bronze/



├── silver/



└── gold/





