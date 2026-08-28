from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import basedosdados as bd


BILLING_PROJECT_ID = "tech-challenge-506502"
BASE_DIR = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"


QUERY_UF = """
WITH
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'serie'
      AND id_tabela = 'uf'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'rede'
      AND id_tabela = 'uf'
)
SELECT
    dados.ano AS ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao AS taxa_alfabetizacao,
    dados.media_portugues AS media_portugues,
    dados.proporcao_aluno_nivel_0 AS proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 AS proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 AS proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 AS proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 AS proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 AS proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 AS proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 AS proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 AS proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf` AS dados
LEFT JOIN (
    SELECT DISTINCT sigla, nome
    FROM `basedosdados.br_bd_diretorios_brasil.uf`
) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN dicionario_serie
    ON dados.serie = chave_serie
LEFT JOIN dicionario_rede
    ON dados.rede = chave_rede
"""


QUERY_MUNICIPIO = """
WITH
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'serie'
      AND id_tabela = 'municipio'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'rede'
      AND id_tabela = 'municipio'
)
SELECT
    dados.ano AS ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao AS taxa_alfabetizacao,
    dados.media_portugues AS media_portugues,
    dados.proporcao_aluno_nivel_0 AS proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 AS proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 AS proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 AS proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 AS proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 AS proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 AS proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 AS proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 AS proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio` AS dados
LEFT JOIN (
    SELECT DISTINCT id_municipio, nome
    FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN dicionario_serie
    ON dados.serie = chave_serie
LEFT JOIN dicionario_rede
    ON dados.rede = chave_rede
"""


QUERY_META_UF = """
SELECT
    dados.ano AS ano,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.rede AS rede,
    dados.taxa_alfabetizacao AS taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 AS meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 AS meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 AS meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 AS meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 AS meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 AS meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 AS meta_alfabetizacao_2030,
    dados.percentual_participacao AS percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf` AS dados
LEFT JOIN (
    SELECT DISTINCT sigla, nome
    FROM `basedosdados.br_bd_diretorios_brasil.uf`
) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
"""


QUERY_META_BRASIL = """
SELECT
    dados.ano AS ano,
    dados.rede AS rede,
    dados.taxa_alfabetizacao AS taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 AS meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 AS meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 AS meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 AS meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 AS meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 AS meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 AS meta_alfabetizacao_2030,
    dados.percentual_participacao AS percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil` AS dados
"""


QUERY_META_MUNICIPIO = """
SELECT
    dados.ano AS ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.rede AS rede,
    dados.taxa_alfabetizacao AS taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 AS meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 AS meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 AS meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 AS meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 AS meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 AS meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 AS meta_alfabetizacao_2030,
    dados.nivel_alfabetizacao AS nivel_alfabetizacao,
    dados.percentual_participacao AS percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` AS dados
LEFT JOIN (
    SELECT DISTINCT id_municipio, nome
    FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
"""


QUERY_ALUNOS = """
WITH
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'serie'
      AND id_tabela = 'alunos'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'rede'
      AND id_tabela = 'alunos'
),
dicionario_presenca AS (
    SELECT
        chave AS chave_presenca,
        valor AS descricao_presenca
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'presenca'
      AND id_tabela = 'alunos'
),
dicionario_preenchimento_caderno AS (
    SELECT
        chave AS chave_preenchimento_caderno,
        valor AS descricao_preenchimento_caderno
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'preenchimento_caderno'
      AND id_tabela = 'alunos'
),
dicionario_alfabetizado AS (
    SELECT
        chave AS chave_alfabetizado,
        valor AS descricao_alfabetizado
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE nome_coluna = 'alfabetizado'
      AND id_tabela = 'alunos'
)
SELECT
    dados.ano AS ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.id_escola AS id_escola,
    dados.id_aluno AS id_aluno,
    dados.caderno AS caderno,
    descricao_serie AS serie,
    descricao_rede AS rede,
    descricao_presenca AS presenca,
    descricao_preenchimento_caderno AS preenchimento_caderno,
    descricao_alfabetizado AS alfabetizado,
    dados.proficiencia AS proficiencia,
    dados.peso_aluno AS peso_aluno
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos` AS dados
LEFT JOIN (
    SELECT DISTINCT id_municipio, nome
    FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN dicionario_serie
    ON dados.serie = chave_serie
LEFT JOIN dicionario_rede
    ON dados.rede = chave_rede
LEFT JOIN dicionario_presenca
    ON dados.presenca = chave_presenca
LEFT JOIN dicionario_preenchimento_caderno
    ON dados.preenchimento_caderno = chave_preenchimento_caderno
LEFT JOIN dicionario_alfabetizado
    ON dados.alfabetizado = chave_alfabetizado
"""


TABELAS = [
    {
        "nome": "alfabetizacao_uf",
        "categoria": Path("resultados") / "uf",
        "query": QUERY_UF,
    },
    {
        "nome": "alfabetizacao_municipio",
        "categoria": Path("resultados") / "municipio",
        "query": QUERY_MUNICIPIO,
    },
    {
        "nome": "meta_alfabetizacao_brasil",
        "categoria": Path("metas") / "brasil",
        "query": QUERY_META_BRASIL,
    },
    {
        "nome": "meta_alfabetizacao_uf",
        "categoria": Path("metas") / "uf",
        "query": QUERY_META_UF,
    },
    {
        "nome": "meta_alfabetizacao_municipio",
        "categoria": Path("metas") / "municipio",
        "query": QUERY_META_MUNICIPIO,
    },
    {
        "nome": "alunos",
        "categoria": Path("microdados") / "alunos",
        "query": QUERY_ALUNOS,
    },
]


def criar_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Baixa as tabelas de Avaliação da Alfabetização da Base dos Dados "
            "e organiza os CSVs na camada Bronze."
        )
    )
    parser.add_argument(
        "--sobrescrever",
        action="store_true",
        help="Baixa novamente arquivos que já existem.",
    )
    parser.add_argument(
        "--pular-alunos",
        action="store_true",
        help="Baixa todas as tabelas, exceto a tabela de alunos.",
    )
    parser.add_argument(
        "--limite-alunos",
        type=int,
        default=None,
        help="Limita a tabela de alunos para teste, por exemplo: 10000.",
    )
    parser.add_argument(
        "--somente",
        nargs="+",
        choices=[tabela["nome"] for tabela in TABELAS],
        default=None,
        help=(
            "Baixa somente as tabelas informadas. Exemplo: "
            "--somente alunos meta_alfabetizacao_uf"
        ),
    )
    return parser.parse_args()


def salvar_csv_atomico(df, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".csv.part")

    try:
        df.to_csv(temporario, index=False, encoding="utf-8-sig")
        temporario.replace(destino)
    finally:
        if temporario.exists():
            temporario.unlink()


def baixar_tabela(
    nome: str,
    categoria: Path,
    query: str,
    sobrescrever: bool,
    limite_alunos: int | None,
) -> dict:
    destino = BRONZE_DIR / categoria / f"{nome}.csv"

    if destino.exists() and not sobrescrever:
        print(f"[IGNORADO] {nome}: o arquivo já existe.")
        print(f"           {destino}")
        return {
            "nome": nome,
            "status": "ignorado",
            "arquivo": str(destino),
        }

    query_final = query.strip()
    if nome == "alunos" and limite_alunos is not None:
        if limite_alunos <= 0:
            raise ValueError("--limite-alunos deve ser maior que zero.")
        query_final = f"{query_final}\nLIMIT {limite_alunos}"

    print(f"\n[BAIXANDO] {nome}")
    df = bd.read_sql(
        query=query_final,
        billing_project_id=BILLING_PROJECT_ID,
    )

    salvar_csv_atomico(df, destino)

    anos = []
    if "ano" in df.columns:
        anos = sorted(str(ano) for ano in df["ano"].dropna().unique())

    resultado = {
        "nome": nome,
        "status": "baixado",
        "arquivo": str(destino),
        "linhas": int(len(df)),
        "colunas": int(len(df.columns)),
        "nomes_colunas": [str(coluna) for coluna in df.columns],
        "anos": anos,
    }

    print(f"[CONCLUÍDO] {nome}")
    print(f"            Linhas: {len(df):,}")
    print(f"            Colunas: {len(df.columns)}")
    print(f"            Arquivo: {destino}")

    return resultado


def salvar_manifesto(resultados: list[dict]) -> Path:
    metadata_dir = BRONZE_DIR / "_metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    destino = metadata_dir / "manifesto_download.json"

    manifesto = {
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "billing_project_id": BILLING_PROJECT_ID,
        "camada": "bronze",
        "fonte": "Base dos Dados - Avaliação da Alfabetização",
        "resultados": resultados,
    }

    destino.write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return destino


def main() -> None:
    argumentos = criar_argumentos()
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 68)
    print("DOWNLOAD DA BASE DOS DADOS - CAMADA BRONZE")
    print(f"Projeto Google Cloud: {BILLING_PROJECT_ID}")
    print(f"Destino: {BRONZE_DIR}")
    print("=" * 68)

    resultados = []
    falhas = []

    for tabela in TABELAS:
        if argumentos.somente and tabela["nome"] not in argumentos.somente:
            continue

        if tabela["nome"] == "alunos" and argumentos.pular_alunos:
            print("\n[IGNORADO] alunos: opção --pular-alunos utilizada.")
            resultados.append(
                {
                    "nome": "alunos",
                    "status": "ignorado_por_opcao",
                }
            )
            continue

        try:
            resultado = baixar_tabela(
                nome=tabela["nome"],
                categoria=tabela["categoria"],
                query=tabela["query"],
                sobrescrever=argumentos.sobrescrever,
                limite_alunos=argumentos.limite_alunos,
            )
            resultados.append(resultado)
        except Exception as erro:
            print(f"[ERRO] {tabela['nome']}: {erro}")
            falhas.append(tabela["nome"])
            resultados.append(
                {
                    "nome": tabela["nome"],
                    "status": "erro",
                    "erro": str(erro),
                }
            )

    manifesto = salvar_manifesto(resultados)

    print("\n" + "=" * 68)
    print("RESUMO")
    print(f"Manifesto: {manifesto}")

    if falhas:
        print(f"Falhas: {', '.join(falhas)}")
        raise SystemExit(1)

    print("Todos os downloads solicitados foram concluídos.")


if __name__ == "__main__":
    main()