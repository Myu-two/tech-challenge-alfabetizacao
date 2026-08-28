"""Glue Job Silver -> Gold - versão integrada com comparativos."""

import sys

import boto3
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


SILVER_DATABASE = "alfabetizacao_silver_db"
GOLD_DATABASE = "alfabetizacao_gold_db"
GOLD_BASE = "s3://pipeline-alfabetizacao-gabrielle-rosa/gold"


def ler_tabela(glue_context: GlueContext, nome: str) -> DataFrame:
    print(f"[LEITURA] {SILVER_DATABASE}.{nome}")
    return glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name=nome,
        transformation_ctx=f"origem_{nome}",
    ).toDF()


def faixa_desempenho(taxa: Column) -> Column:
    return (
        F.when(taxa.isNull(), F.lit("Sem informação"))
        .when(taxa >= 80, F.lit("Muito alto"))
        .when(taxa >= 70, F.lit("Alto"))
        .when(taxa >= 60, F.lit("Intermediário"))
        .otherwise(F.lit("Baixo"))
    )


def criar_indicadores_uf(df: DataFrame) -> DataFrame:
    taxa = F.col("taxa_alfabetizacao").cast("double")
    niveis_adequados = sum(
        (F.coalesce(F.col(f"proporcao_aluno_nivel_{nivel}"), F.lit(0.0))
         for nivel in range(5, 9)),
        F.lit(0.0),
    )

    janela_ranking = Window.partitionBy("ano", "serie", "rede").orderBy(
        F.col("taxa_alfabetizacao").desc_nulls_last()
    )

    return (
        df.withColumn("ano", F.col("ano").cast("int"))
        .withColumn("taxa_alfabetizacao", taxa)
        .withColumn("proporcao_niveis_adequados", niveis_adequados)
        .withColumn("faixa_desempenho", faixa_desempenho(taxa))
        .withColumn("ranking_uf", F.dense_rank().over(janela_ranking))
        .select(
            "ano",
            "sigla_uf",
            "sigla_uf_nome",
            "serie",
            "rede",
            "taxa_alfabetizacao",
            "media_portugues",
            "proporcao_niveis_adequados",
            "faixa_desempenho",
            "ranking_uf",
        )
    )


def criar_indicadores_municipio(df: DataFrame) -> DataFrame:
    taxa = F.col("taxa_alfabetizacao").cast("double")
    niveis_adequados = sum(
        (F.coalesce(F.col(f"proporcao_aluno_nivel_{nivel}"), F.lit(0.0))
         for nivel in range(5, 9)),
        F.lit(0.0),
    )

    janela_ranking = Window.partitionBy("ano", "serie", "rede").orderBy(
        F.col("taxa_alfabetizacao").desc_nulls_last()
    )

    return (
        df.withColumn("ano", F.col("ano").cast("int"))
        .withColumn("taxa_alfabetizacao", taxa)
        .withColumn("proporcao_niveis_adequados", niveis_adequados)
        .withColumn("faixa_desempenho", faixa_desempenho(taxa))
        .withColumn("ranking_municipio", F.dense_rank().over(janela_ranking))
        .select(
            "ano",
            "id_municipio",
            "id_municipio_nome",
            "serie",
            "rede",
            "taxa_alfabetizacao",
            "media_portugues",
            "proporcao_niveis_adequados",
            "faixa_desempenho",
            "ranking_municipio",
        )
    )


def status_comparacao(taxa: Column, meta: Column, status_integracao: Column) -> Column:
    return (
        F.when(status_integracao != "Correspondente", status_integracao)
        .when(meta.isNull(), F.lit("Sem meta para o ano"))
        .when(taxa >= meta, F.lit("Meta atingida"))
        .otherwise(F.lit("Abaixo da meta"))
    )


def criar_comparativo_uf(df: DataFrame) -> DataFrame:
    taxa = F.col("taxa_resultado").cast("double")
    meta = F.col("meta_ano").cast("double")
    janela = Window.partitionBy("ano", "rede").orderBy(taxa.desc_nulls_last())

    return (
        df.withColumn("ano", F.col("ano").cast("int"))
        .withColumn("ranking_uf", F.dense_rank().over(janela))
        .withColumn(
            "status_comparacao",
            status_comparacao(taxa, meta, F.col("status_integracao")),
        )
        .select(
            "ano",
            "sigla_uf",
            "sigla_uf_nome",
            "serie",
            "rede",
            "taxa_resultado",
            "taxa_referencia_meta",
            "meta_ano",
            "meta_alfabetizacao_2030",
            "gap_para_meta_ano",
            "gap_para_meta_2030",
            "percentual_participacao",
            "status_integracao",
            "status_comparacao",
            "ranking_uf",
        )
    )


def criar_comparativo_municipio(df: DataFrame) -> DataFrame:
    taxa = F.col("taxa_resultado").cast("double")
    meta = F.col("meta_ano").cast("double")
    janela = Window.partitionBy("ano", "rede").orderBy(taxa.desc_nulls_last())

    return (
        df.withColumn("ano", F.col("ano").cast("int"))
        .withColumn("ranking_municipio", F.dense_rank().over(janela))
        .withColumn(
            "status_comparacao",
            status_comparacao(taxa, meta, F.col("status_integracao")),
        )
        .select(
            "ano",
            "id_municipio",
            "id_municipio_nome",
            "serie",
            "rede",
            "taxa_resultado",
            "taxa_referencia_meta",
            "meta_ano",
            "meta_alfabetizacao_2030",
            "gap_para_meta_ano",
            "gap_para_meta_2030",
            "nivel_alfabetizacao",
            "percentual_participacao",
            "status_integracao",
            "status_comparacao",
            "ranking_municipio",
        )
    )


def metas_em_formato_longo(
    df: DataFrame,
    nivel_geografico: str,
) -> DataFrame:
    metas = F.array(
        *[
            F.struct(
                F.lit(ano_meta).cast("int").alias("ano_meta"),
                F.col(f"meta_alfabetizacao_{ano_meta}")
                .cast("double")
                .alias("meta_alfabetizacao"),
            )
            for ano_meta in range(2024, 2031)
        ]
    )

    base = (
        df.withColumn("ano_referencia", F.col("ano").cast("int"))
        .withColumn("item_meta", F.explode(metas))
        .withColumn("nivel_geografico", F.lit(nivel_geografico))
    )

    def existente_ou_nulo(nome: str, tipo: str) -> F.Column:
        if nome in base.columns:
            return F.col(nome).cast(tipo)
        return F.lit(None).cast(tipo)

    taxa = F.col("taxa_alfabetizacao").cast("double")
    meta = F.col("item_meta.meta_alfabetizacao")

    return base.select(
        F.col("nivel_geografico"),
        F.col("ano_referencia"),
        F.col("item_meta.ano_meta").alias("ano_meta"),
        existente_ou_nulo("sigla_uf", "string").alias("sigla_uf"),
        existente_ou_nulo("sigla_uf_nome", "string").alias("nome_uf"),
        existente_ou_nulo("id_municipio", "string").alias("id_municipio"),
        existente_ou_nulo("id_municipio_nome", "string").alias("nome_municipio"),
        existente_ou_nulo("rede", "string").alias("rede"),
        taxa.alias("taxa_alfabetizacao"),
        meta.alias("meta_alfabetizacao"),
        F.round(meta - taxa, 2).alias("gap_para_meta"),
        existente_ou_nulo("percentual_participacao", "double").alias(
            "percentual_participacao"
        ),
        existente_ou_nulo("nivel_alfabetizacao", "int").alias(
            "nivel_alfabetizacao"
        ),
        F.when(
            taxa.isNull() | meta.isNull(),
            F.lit("Sem informação"),
        )
        .when(taxa >= meta, F.lit("Meta atingida"))
        .otherwise(F.lit("Abaixo da meta"))
        .alias("status_meta"),
    )


def criar_painel_metas(
    meta_brasil: DataFrame,
    meta_uf: DataFrame,
    meta_municipio: DataFrame,
) -> DataFrame:
    return (
        metas_em_formato_longo(meta_brasil, "Brasil")
        .unionByName(metas_em_formato_longo(meta_uf, "UF"))
        .unionByName(metas_em_formato_longo(meta_municipio, "Município"))
        .filter(F.col("meta_alfabetizacao").isNotNull())
    )


def criar_resumo_alunos(df: DataFrame) -> DataFrame:
    texto_alfabetizado = F.translate(
        F.lower(F.trim(F.coalesce(F.col("alfabetizado"), F.lit("")))),
        "ãáàâéêíóôõúç",
        "aaaaeeiooouc",
    )

    indicador_alfabetizado = F.when(
        texto_alfabetizado.isin("sim", "1", "true", "alfabetizado", "alfabetizada")
        | (
            texto_alfabetizado.contains("alfabetiz")
            & ~texto_alfabetizado.contains("nao")
        ),
        F.lit(1),
    ).otherwise(F.lit(0))

    classificacao_conhecida = F.when(
        F.col("alfabetizado").isNotNull(), F.lit(1)
    ).otherwise(F.lit(0))

    proficiencia_valida = F.col("proficiencia").isNotNull() & (
        F.col("peso_aluno") > 0
    )

    agregado = (
        df.withColumn("ano", F.col("ano").cast("int"))
        .withColumn("indicador_alfabetizado", indicador_alfabetizado)
        .withColumn("classificacao_conhecida", classificacao_conhecida)
        .groupBy(
            "ano",
            "id_municipio",
            "id_municipio_nome",
            "serie",
            "rede",
        )
        .agg(
            F.countDistinct("id_aluno").alias("total_alunos"),
            F.countDistinct("id_escola").alias("total_escolas"),
            F.sum("classificacao_conhecida").alias("alunos_com_classificacao"),
            F.sum("indicador_alfabetizado").alias("alunos_alfabetizados"),
            F.round(F.avg("proficiencia"), 2).alias("media_proficiencia"),
            F.sum(
                F.when(
                    proficiencia_valida,
                    F.col("proficiencia") * F.col("peso_aluno"),
                ).otherwise(F.lit(0.0))
            ).alias("soma_proficiencia_ponderada"),
            F.sum(
                F.when(proficiencia_valida, F.col("peso_aluno")).otherwise(
                    F.lit(0.0)
                )
            ).alias("soma_pesos_validos"),
        )
    )

    return (
        agregado.withColumn(
            "taxa_alfabetizacao_calculada",
            F.when(
                F.col("alunos_com_classificacao") > 0,
                F.round(
                    100.0
                    * F.col("alunos_alfabetizados")
                    / F.col("alunos_com_classificacao"),
                    2,
                ),
            ),
        )
        .withColumn(
            "media_proficiencia_ponderada",
            F.when(
                F.col("soma_pesos_validos") > 0,
                F.round(
                    F.col("soma_proficiencia_ponderada")
                    / F.col("soma_pesos_validos"),
                    2,
                ),
            ),
        )
        .drop("soma_proficiencia_ponderada", "soma_pesos_validos")
    )


def garantir_database_gold() -> None:
    cliente = boto3.client("glue")
    try:
        cliente.get_database(Name=GOLD_DATABASE)
        print(f"[CATÁLOGO] Database {GOLD_DATABASE} já existe.")
    except ClientError as erro:
        if erro.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise
        cliente.create_database(
            DatabaseInput={
                "Name": GOLD_DATABASE,
                "Description": "Tabelas analíticas da pipeline de alfabetização",
            }
        )
        print(f"[CATÁLOGO] Database {GOLD_DATABASE} criado.")


def gravar_e_catalogar(
    glue_context: GlueContext,
    df: DataFrame,
    nome_tabela: str,
    destino: str,
    particoes: list[str],
) -> None:
    print(f"[GRAVAÇÃO] {nome_tabela} -> {destino}")

    # A Gold é derivada e pode ser recriada. A limpeza evita duplicação em reexecuções.
    glue_context.purge_s3_path(destino, options={"retentionPeriod": 0})

    saida = df.withColumn("processado_gold_em_utc", F.current_timestamp())
    dynamic_frame = DynamicFrame.fromDF(
        saida,
        glue_context,
        f"dynamic_{nome_tabela}",
    )

    sink = glue_context.getSink(
        connection_type="s3",
        path=destino,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=particoes,
        transformation_ctx=f"destino_{nome_tabela}",
    )
    sink.setCatalogInfo(
        catalogDatabase=GOLD_DATABASE,
        catalogTableName=nome_tabela,
    )
    sink.setFormat("glueparquet")
    sink.writeFrame(dynamic_frame)
    print(f"[CONCLUÍDO] {GOLD_DATABASE}.{nome_tabela}")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    resultado_uf = ler_tabela(glue_context, "resultado_uf")
    resultado_municipio = ler_tabela(glue_context, "resultado_municipio")
    meta_brasil = ler_tabela(glue_context, "meta_brasil")
    meta_uf = ler_tabela(glue_context, "meta_uf")
    meta_municipio = ler_tabela(glue_context, "meta_municipio")
    alunos = ler_tabela(glue_context, "alunos")
    integrado_uf = ler_tabela(glue_context, "integrado_uf")
    integrado_municipio = ler_tabela(glue_context, "integrado_municipio")

    indicadores_uf = criar_indicadores_uf(resultado_uf)
    indicadores_municipio = criar_indicadores_municipio(resultado_municipio)
    painel_metas = criar_painel_metas(meta_brasil, meta_uf, meta_municipio)
    resumo_alunos = criar_resumo_alunos(alunos)
    comparativo_uf = criar_comparativo_uf(integrado_uf)
    comparativo_municipio = criar_comparativo_municipio(integrado_municipio)

    garantir_database_gold()

    gravar_e_catalogar(
        glue_context,
        indicadores_uf,
        "indicadores_uf",
        f"{GOLD_BASE}/indicadores/uf/",
        ["ano"],
    )
    gravar_e_catalogar(
        glue_context,
        indicadores_municipio,
        "indicadores_municipio",
        f"{GOLD_BASE}/indicadores/municipio/",
        ["ano"],
    )
    gravar_e_catalogar(
        glue_context,
        painel_metas,
        "painel_metas",
        f"{GOLD_BASE}/metas/",
        ["ano_meta"],
    )
    gravar_e_catalogar(
        glue_context,
        resumo_alunos,
        "resumo_alunos_municipio",
        f"{GOLD_BASE}/alunos/municipio/",
        ["ano"],
    )
    gravar_e_catalogar(
        glue_context,
        comparativo_uf,
        "comparativo_uf",
        f"{GOLD_BASE}/comparativos/uf/",
        ["ano"],
    )
    gravar_e_catalogar(
        glue_context,
        comparativo_municipio,
        "comparativo_municipio",
        f"{GOLD_BASE}/comparativos/municipio/",
        ["ano"],
    )

    job.commit()
    print("Camada Gold criada e catalogada com sucesso.")


if __name__ == "__main__":
    main()
