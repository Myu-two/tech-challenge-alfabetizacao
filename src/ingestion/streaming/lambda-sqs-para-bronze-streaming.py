import json
import logging
import os
from datetime import datetime, timezone

import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

S3 = boto3.client("s3")
BUCKET_NAME = os.environ.get(
    "BUCKET_NAME",
    "pipeline-alfabetizacao-gabrielle-rosa",
)
BRONZE_PREFIX = os.environ.get(
    "BRONZE_PREFIX",
    "bronze/streaming/indicadores",
).strip("/")


def lambda_handler(event, context):
    """Persiste no S3 as mensagens recebidas da fila SQS."""
    falhas = []
    salvos = 0

    for indice, record in enumerate(event.get("Records", [])):
        message_id = record.get("messageId") or (
            f"{context.aws_request_id}-{indice}"
        )

        try:
            corpo_original = record.get("body", "")
            evento = json.loads(corpo_original)

            if not isinstance(evento, dict):
                raise ValueError(
                    "O corpo da mensagem deve ser um objeto JSON"
                )

            agora = datetime.now(timezone.utc)
            recebido_em = agora.isoformat(
                timespec="milliseconds"
            ).replace("+00:00", "Z")

            documento_bronze = {
                "_ingestao": {
                    "recebido_em_utc": recebido_em,
                    "id_mensagem_sqs": message_id,
                    "origem": "amazon-sqs",
                    "fila_arn": record.get("eventSourceARN"),
                },
                "evento": evento,
            }

            chave_s3 = (
                f"{BRONZE_PREFIX}/"
                f"ano={agora:%Y}/mes={agora:%m}/"
                f"dia={agora:%d}/hora={agora:%H}/"
                f"{message_id}.json"
            )

            S3.put_object(
                Bucket=BUCKET_NAME,
                Key=chave_s3,
                Body=(
                    json.dumps(
                        documento_bronze,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8"),
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )

            salvos += 1
            LOGGER.info(
                "Mensagem %s salva em s3://%s/%s",
                message_id,
                BUCKET_NAME,
                chave_s3,
            )

        except Exception:
            LOGGER.exception(
                "Falha ao processar a mensagem %s",
                message_id,
            )
            falhas.append({"itemIdentifier": message_id})

    LOGGER.info(
        "Lote concluido: recebidas=%s salvas=%s falhas=%s",
        len(event.get("Records", [])),
        salvos,
        len(falhas),
    )

    return {"batchItemFailures": falhas}