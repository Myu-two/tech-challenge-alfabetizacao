import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import boto3


REGIAO = os.environ.get("AWS_REGION", "us-east-2")
QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.us-east-2.amazonaws.com/450993990817/"
    "fila-alfabetizacao-eventos",
)

DESTINOS = [
    {
        "nivel": "UF",
        "sigla_uf": "SP",
        "nome_localidade": "Sao Paulo",
        "id_municipio": None,
        "rede": "Publica",
    },
    {
        "nivel": "UF",
        "sigla_uf": "MG",
        "nome_localidade": "Minas Gerais",
        "id_municipio": None,
        "rede": "Publica",
    },
    {
        "nivel": "UF",
        "sigla_uf": "BA",
        "nome_localidade": "Bahia",
        "id_municipio": None,
        "rede": "Publica",
    },
    {
        "nivel": "Municipio",
        "sigla_uf": "SP",
        "nome_localidade": "Sao Paulo",
        "id_municipio": "3550308",
        "rede": "Municipal",
    },
    {
        "nivel": "Municipio",
        "sigla_uf": "RJ",
        "nome_localidade": "Rio de Janeiro",
        "id_municipio": "3304557",
        "rede": "Municipal",
    },
    {
        "nivel": "Municipio",
        "sigla_uf": "MG",
        "nome_localidade": "Belo Horizonte",
        "id_municipio": "3106200",
        "rede": "Municipal",
    },
    {
        "nivel": "Municipio",
        "sigla_uf": "BA",
        "nome_localidade": "Salvador",
        "id_municipio": "2927408",
        "rede": "Municipal",
    },
    {
        "nivel": "Municipio",
        "sigla_uf": "PR",
        "nome_localidade": "Curitiba",
        "id_municipio": "4106902",
        "rede": "Municipal",
    },
]


def criar_evento(numero):
    destino = random.choice(DESTINOS)
    instante = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    return {
        "evento_id": str(uuid.uuid4()),
        "sequencia": numero,
        "timestamp_evento": instante.replace("+00:00", "Z"),
        "tipo_evento": "atualizacao_indicador_alfabetizacao",
        "ano_referencia": 2024,
        "nivel": destino["nivel"],
        "sigla_uf": destino["sigla_uf"],
        "id_municipio": destino["id_municipio"],
        "nome_localidade": destino["nome_localidade"],
        "serie": "2o ano do Ensino Fundamental",
        "rede": destino["rede"],
        "taxa_alfabetizacao": round(random.uniform(55.0, 88.0), 2),
        "media_portugues": round(random.uniform(700.0, 810.0), 2),
        "proporcao_niveis_adequados": round(random.uniform(35.0, 82.0), 2),
        "origem": "simulador-python",
    }


def executar_simulacao(quantidade, intervalo):
    sqs = boto3.client("sqs", region_name=REGIAO)

    print("=" * 68)
    print("SIMULADOR DE EVENTOS DE ALFABETIZACAO")
    print(f"Regiao: {REGIAO}")
    print(f"Quantidade: {quantidade}")
    print(f"Intervalo: {intervalo} segundo(s)")
    print("=" * 68)

    for numero in range(1, quantidade + 1):
        evento = criar_evento(numero)
        resposta = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(evento, ensure_ascii=False),
        )

        print(
            f"[{numero:03d}/{quantidade:03d}] "
            f"{evento['nivel']:<9} "
            f"{evento['nome_localidade']:<18} "
            f"taxa={evento['taxa_alfabetizacao']:>5.2f}% "
            f"message_id={resposta['MessageId']}"
        )

        if numero < quantidade and intervalo > 0:
            time.sleep(intervalo)

    print("Simulacao concluida.")


def main():
    parser = argparse.ArgumentParser(
        description="Envia eventos simulados de alfabetizacao ao Amazon SQS."
    )
    parser.add_argument(
        "--quantidade",
        type=int,
        default=20,
        help="Numero de eventos a enviar (padrao: 20).",
    )
    parser.add_argument(
        "--intervalo",
        type=float,
        default=1.0,
        help="Intervalo em segundos entre eventos (padrao: 1).",
    )
    args = parser.parse_args()

    if args.quantidade < 1:
        parser.error("--quantidade deve ser maior que zero")
    if args.intervalo < 0:
        parser.error("--intervalo nao pode ser negativo")

    try:
        executar_simulacao(args.quantidade, args.intervalo)
    except KeyboardInterrupt:
        print("\nSimulacao interrompida pelo usuario.")


if __name__ == "__main__":
    main()
