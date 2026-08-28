"""Glue Job: Bronze Streaming -> Silver -> integracao Batch + Streaming -> Gold."""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


BUCKET = "pipeline-alfabetizacao-gabrielle-rosa"

BRONZE_STREAMING = f"s3://{BUCKET}/bronze/streaming/indicadores/"
SILVER_EVENTOS = f"s3://{BUCKET}/silver/streaming/eventos/"
SILVER_HIBRIDO = f"s3://{BUCKET}/silver/streaming/integrado_batch/"
GOLD_HIBRIDO = f"s3://{BUCKET}/gold/streaming/painel_hibrido_atual/"
QUALITY_REJEITADOS = f"s3://{BUCKET}/quality/streaming/rejeitados/"
QUALITY_RESUMO = f"s3://{BUCKET}/quality/streaming/resumo_execucao/"

SILVER_DATABASE = "alfabetizacao_silver_db"
GOLD_DATABASE = "alfabetizacao_gold_db"


def ler_bronze_streaming(spark) -> DataFrame:
    """Le todos os JSONs do fluxo, inclusive dentro das particoes do S3."""
    print(f"[LEITURA] {BRONZE_STREAMING}")
    return (
        spark.read.option("recursiveFileLookup", "true")
        .option("mode", "PERMISSIVE")
        .json(BRONZE_STREAMING)
    )


def normalizar_rede(nivel: Column, rede: Column) -> Column:
    rede_normalizada = F.lower(F.trim(rede))
    return (
        F.when(rede_normalizada.startswith("municip"), F.lit("Municipal"))
        .when(rede_normalizada.startswith("public"), F.lit("Pública"))
        .when(nivel == "UF", F.lit("Pública"))
        .when(nivel == "Municipio", F.lit("Municipal"))
        .otherwise(F.trim(rede))
    )


def achatar_eventos(df: DataFrame) -> DataFrame:
    """Extrai o envelope criado pela Lambda e padroniza os tipos."""
    nivel = F.trim(F.col("evento.nivel"))
    ano_referencia = F.coalesce(
        F.col("evento.ano_referencia"),
        F.col("evento.ano"),
    ).cast("int")

    return df.select(
        F.col("_ingestao.id_mensagem_sqs").cast("string").alias(
            "id_mensagem_sqs"
        ),
        F.to_timestamp(F.col("_ingestao.recebido_em_utc")).alias(
            "recebido_em_utc"
        ),
        F.col("evento.evento_id").cast("string").alias("evento_id"),
        F.col("evento.sequencia").cast("long").alias("sequencia"),
        F.to_timestamp(F.col("evento.timestamp_evento")).alias(
            "timestamp_evento"
        ),
        F.col("evento.tipo_evento").cast("string").alias("tipo_evento"),
        ano_referencia.alias("ano_referencia"),
        nivel.alias("nivel"),
        F.upper(F.trim(F.col("evento.sigla_uf"))).alias("sigla_uf"),
        F.trim(F.col("evento.id_municipio").cast("string")).alias(
            "id_municipio"
        ),
        F.col("evento.nome_localidade").cast("string").alias(
            "nome_localidade"
        ),
        F.col("evento.serie").cast("string").alias("serie"),
        normalizar_rede(nivel, F.col("evento.rede")).alias("rede"),
        F.col("evento.taxa_alfabetizacao").cast("double").alias(
            "taxa_alfabetizacao"
        ),
        F.col("evento.media_portugues").cast("double").alias(
            "media_portugues"
        ),
        F.col("evento.proporcao_niveis_adequados").cast("double").alias(
            "proporcao_niveis_adequados"
        ),
        F.col("evento.origem").cast("string").alias("origem"),
    )


def adicionar_validacao(df: DataFrame) -> DataFrame:
    """Acrescenta os motivos de rejeicao sem descartar o evento original."""
    motivo = F.concat_ws(
        "; ",
        F.when(
            F.col("evento_id").isNull() | (F.trim("evento_id") == ""),
            F.lit("evento_id ausente"),
        ),
        F.when(
            F.col("timestamp_evento").isNull(),
            F.lit("timestamp_evento inválido"),
        ),
        F.when(
            F.col("ano_referencia").isNull(),
            F.lit("ano_referencia ausente"),
        ),
        F.when(
            ~F.col("nivel").isin("UF", "Municipio"),
            F.lit("nível inválido"),
        ),
        F.when(
            F.col("sigla_uf").isNull()
            | (F.length(F.col("sigla_uf")) != 2),
            F.lit("sigla_uf inválida"),
        ),
        F.when(
            (F.col("nivel") == "Municipio")
            & (
                F.col("id_municipio").isNull()
                | (F.length(F.col("id_municipio")) != 7)
            ),
            F.lit("id_municipio inválido"),
        ),
        F.when(
            F.col("taxa_alfabetizacao").isNull(),
            F.lit("taxa_alfabetizacao ausente"),
        ),
        F.when(
            F.col("taxa_alfabetizacao").isNotNull()
            & ~F.col("taxa_alfabetizacao").between(0, 100),
            F.lit("taxa_alfabetizacao fora de 0 a 100"),
        ),
        F.when(
            F.col("media_portugues").isNotNull()
            & (F.col("media_portugues") < 0),
            F.lit("media_portugues negativa"),
        ),
        F.when(
            F.col("proporcao_niveis_adequados").isNotNull()
            & ~F.col("proporcao_niveis_adequados").between(0, 100),
            F.lit("proporcao_niveis_adequados fora de 0 a 100"),
        ),
    )

    return df.withColumn("motivo_rejeicao", motivo)


def deduplicar_eventos(df: DataFrame) -> DataFrame:
    """Mantem a versao mais recente de cada evento_id."""
    janela = Window.partitionBy("evento_id").orderBy(
        F.col("timestamp_evento").desc_nulls_last(),
        F.col("recebido_em_utc").desc_nulls_last(),
        F.col("id_mensagem_sqs").desc_nulls_last(),
    )
    return (
        df.withColumn("_ordem_evento", F.row_number().over(janela))
        .filter(F.col("_ordem_evento") == 1)
        .drop("_ordem_evento", "motivo_rejeicao")
        .withColumn("ano_ingestao", F.year("recebido_em_utc"))
        .withColumn("mes_ingestao", F.month("recebido_em_utc"))
        .withColumn("dia_ingestao", F.dayofmonth("recebido_em_utc"))
    )


def ler_tabela_silver(glue_context: GlueContext, tabela: str) -> DataFrame:
    print(f"[LEITURA] {SILVER_DATABASE}.{tabela}")
    return glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name=tabela,
        transformation_ctx=f"origem_{tabela}",
    ).toDF()


def eventos_mais_recentes(df: DataFrame) -> DataFrame:
    """Seleciona o evento mais recente de cada UF ou municipio."""
    chave_localidade = F.when(
        F.col("nivel") == "UF",
        F.col("sigla_uf"),
    ).otherwise(F.col("id_municipio"))

    janela = Window.partitionBy(
        "nivel", "ano_referencia", chave_localidade
    ).orderBy(
        F.col("timestamp_evento").desc_nulls_last(),
        F.col("recebido_em_utc").desc_nulls_last(),
    )

    return (
        df.withColumn("_ordem_localidade", F.row_number().over(janela))
        .filter(F.col("_ordem_localidade") == 1)
        .drop("_ordem_localidade")
    )


def status_meta(taxa_streaming: Column, meta: Column) -> Column:
    return (
        F.when(taxa_streaming.isNull(), F.lit("Sem evento streaming"))
        .when(meta.isNull(), F.lit("Sem meta para o ano"))
        .when(taxa_streaming >= meta, F.lit("Meta atingida"))
        .otherwise(F.lit("Abaixo da meta"))
    )


def integrar_uf(streaming: DataFrame, batch: DataFrame) -> DataFrame:
    s = streaming.filter(F.col("nivel") == "UF").alias("s")
    b = batch.alias("b")

    condicao = (
        (F.col("s.ano_referencia") == F.col("b.ano").cast("int"))
        & (F.col("s.sigla_uf") == F.col("b.sigla_uf"))
    )
    combinado = s.join(b, condicao, "full_outer")

    taxa_streaming = F.col("s.taxa_alfabetizacao").cast("double")
    taxa_batch = F.col("b.taxa_resultado").cast("double")
    meta = F.col("b.meta_ano").cast("double")

    correspondencia = (
        F.when(
            F.col("s.evento_id").isNotNull()
            & F.col("b.sigla_uf").isNotNull(),
            F.lit("Correspondente"),
        )
        .when(F.col("s.evento_id").isNotNull(), F.lit("Somente streaming"))
        .otherwise(F.lit("Somente batch"))
    )

    return combinado.select(
        F.lit("UF").alias("nivel"),
        F.coalesce(
            F.col("s.ano_referencia"), F.col("b.ano").cast("int")
        ).alias("ano_referencia"),
        F.coalesce(F.col("s.sigla_uf"), F.col("b.sigla_uf")).alias(
            "sigla_uf"
        ),
        F.lit(None).cast("string").alias("id_municipio"),
        F.coalesce(
            F.col("s.nome_localidade"), F.col("b.sigla_uf_nome")
        ).alias("nome_localidade"),
        F.coalesce(F.col("s.serie"), F.col("b.serie")).alias("serie"),
        F.coalesce(F.col("s.rede"), F.col("b.rede")).alias("rede"),
        F.col("s.evento_id").alias("evento_id"),
        F.col("s.timestamp_evento").alias("timestamp_evento"),
        F.col("s.recebido_em_utc").alias("recebido_em_utc"),
        taxa_streaming.alias("taxa_streaming"),
        F.col("s.media_portugues").alias("media_portugues_streaming"),
        F.col("s.proporcao_niveis_adequados").alias(
            "proporcao_niveis_adequados_streaming"
        ),
        taxa_batch.alias("taxa_batch"),
        meta.alias("meta_ano"),
        F.round(taxa_streaming - taxa_batch, 2).alias(
            "variacao_streaming_vs_batch"
        ),
        F.round(meta - taxa_streaming, 2).alias("gap_streaming_para_meta"),
        status_meta(taxa_streaming, meta).alias("status_meta_streaming"),
        correspondencia.alias("status_correspondencia"),
        F.col("b.status_integracao").alias("status_integracao_batch"),
        F.lit("Batch + Streaming").alias("origem_integracao"),
    )


def integrar_municipio(streaming: DataFrame, batch: DataFrame) -> DataFrame:
    s = streaming.filter(F.col("nivel") == "Municipio").alias("s")
    b = batch.alias("b")

    condicao = (
        (F.col("s.ano_referencia") == F.col("b.ano").cast("int"))
        & (
            F.col("s.id_municipio")
            == F.col("b.id_municipio").cast("string")
        )
    )
    combinado = s.join(b, condicao, "full_outer")

    taxa_streaming = F.col("s.taxa_alfabetizacao").cast("double")
    taxa_batch = F.col("b.taxa_resultado").cast("double")
    meta = F.col("b.meta_ano").cast("double")

    correspondencia = (
        F.when(
            F.col("s.evento_id").isNotNull()
            & F.col("b.id_municipio").isNotNull(),
            F.lit("Correspondente"),
        )
        .when(F.col("s.evento_id").isNotNull(), F.lit("Somente streaming"))
        .otherwise(F.lit("Somente batch"))
    )

    return combinado.select(
        F.lit("Municipio").alias("nivel"),
        F.coalesce(
            F.col("s.ano_referencia"), F.col("b.ano").cast("int")
        ).alias("ano_referencia"),
        F.coalesce(F.col("s.sigla_uf"), F.lit(None).cast("string")).alias(
            "sigla_uf"
        ),
        F.coalesce(
            F.col("s.id_municipio"), F.col("b.id_municipio").cast("string")
        ).alias("id_municipio"),
        F.coalesce(
            F.col("s.nome_localidade"), F.col("b.id_municipio_nome")
        ).alias("nome_localidade"),
        F.coalesce(F.col("s.serie"), F.col("b.serie")).alias("serie"),
        F.coalesce(F.col("s.rede"), F.col("b.rede")).alias("rede"),
        F.col("s.evento_id").alias("evento_id"),
        F.col("s.timestamp_evento").alias("timestamp_evento"),
        F.col("s.recebido_em_utc").alias("recebido_em_utc"),
        taxa_streaming.alias("taxa_streaming"),
        F.col("s.media_portugues").alias("media_portugues_streaming"),
        F.col("s.proporcao_niveis_adequados").alias(
            "proporcao_niveis_adequados_streaming"
        ),
        taxa_batch.alias("taxa_batch"),
        meta.alias("meta_ano"),
        F.round(taxa_streaming - taxa_batch, 2).alias(
            "variacao_streaming_vs_batch"
        ),
        F.round(meta - taxa_streaming, 2).alias("gap_streaming_para_meta"),
        status_meta(taxa_streaming, meta).alias("status_meta_streaming"),
        correspondencia.alias("status_correspondencia"),
        F.col("b.status_integracao").alias("status_integracao_batch"),
        F.lit("Batch + Streaming").alias("origem_integracao"),
    )


def criar_painel_gold(df: DataFrame) -> DataFrame:
    """Mantem somente localidades que receberam evento streaming."""
    faixa = (
        F.when(F.col("taxa_streaming") >= 80, F.lit("Muito alto"))
        .when(F.col("taxa_streaming") >= 70, F.lit("Alto"))
        .when(F.col("taxa_streaming") >= 60, F.lit("Intermediário"))
        .otherwise(F.lit("Baixo"))
    )

    return (
        df.filter(F.col("evento_id").isNotNull())
        .withColumn("faixa_desempenho_streaming", faixa)
        .withColumn(
            "desvio_absoluto_batch",
            F.round(
                F.abs(F.col("taxa_streaming") - F.col("taxa_batch")), 2
            ),
        )
    )


def gravar_catalogado(
    glue_context: GlueContext,
    df: DataFrame,
    database: str,
    tabela: str,
    destino: str,
    particoes: list[str],
) -> None:
    """Recria o dataset no S3 e atualiza a tabela no Glue Data Catalog."""
    print(f"[GRAVAÇÃO] {database}.{tabela} -> {destino}")
    glue_context.purge_s3_path(destino, options={"retentionPeriod": 0})

    saida = df.withColumn("processado_em_utc", F.current_timestamp())
    dynamic_frame = DynamicFrame.fromDF(
        saida,
        glue_context,
        f"dynamic_{tabela}",
    )

    sink = glue_context.getSink(
        connection_type="s3",
        path=destino,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=particoes,
        transformation_ctx=f"destino_{tabela}",
    )
    sink.setCatalogInfo(
        catalogDatabase=database,
        catalogTableName=tabela,
    )
    sink.setFormat("glueparquet")
    sink.writeFrame(dynamic_frame)
    print(f"[CONCLUÍDO] {database}.{tabela}")


def gravar_qualidade(
    spark,
    glue_context: GlueContext,
    rejeitados: DataFrame,
    total: int,
    validos_antes_dedup: int,
    validos_depois_dedup: int,
) -> None:
    """Persiste rejeitados e as metricas de qualidade de cada execucao."""
    quantidade_rejeitados = total - validos_antes_dedup
    duplicados_removidos = validos_antes_dedup - validos_depois_dedup

    glue_context.purge_s3_path(
        QUALITY_REJEITADOS,
        options={"retentionPeriod": 0},
    )
    if quantidade_rejeitados > 0:
        (
            rejeitados.write.mode("overwrite")
            .format("json")
            .save(QUALITY_REJEITADOS)
        )

    resumo = spark.createDataFrame(
        [
            (
                total,
                validos_antes_dedup,
                validos_depois_dedup,
                quantidade_rejeitados,
                duplicados_removidos,
            )
        ],
        "total_recebidos long, validos_antes_dedup long, "
        "validos_finais long, rejeitados long, duplicados_removidos long",
    ).withColumn("processado_em_utc", F.current_timestamp())

    resumo.coalesce(1).write.mode("append").json(QUALITY_RESUMO)

    print(
        "[QUALIDADE] "
        f"total={total} "
        f"validos={validos_depois_dedup} "
        f"rejeitados={quantidade_rejeitados} "
        f"duplicados_removidos={duplicados_removidos}"
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bronze = ler_bronze_streaming(spark)
    eventos = adicionar_validacao(achatar_eventos(bronze)).cache()

    total = eventos.count()
    if total == 0:
        raise ValueError("Nenhum evento foi encontrado na Bronze streaming")

    validos = eventos.filter(F.col("motivo_rejeicao") == "").cache()
    rejeitados = eventos.filter(F.col("motivo_rejeicao") != "").cache()

    quantidade_validos = validos.count()
    if quantidade_validos == 0:
        gravar_qualidade(
            spark,
            glue_context,
            rejeitados,
            total,
            0,
            0,
        )
        raise ValueError("Todos os eventos foram rejeitados pelas validações")

    eventos_deduplicados = deduplicar_eventos(validos).cache()
    quantidade_finais = eventos_deduplicados.count()

    gravar_qualidade(
        spark,
        glue_context,
        rejeitados,
        total,
        quantidade_validos,
        quantidade_finais,
    )

    gravar_catalogado(
        glue_context,
        eventos_deduplicados,
        SILVER_DATABASE,
        "eventos_streaming",
        SILVER_EVENTOS,
        ["ano_ingestao", "mes_ingestao", "dia_ingestao"],
    )

    atuais = eventos_mais_recentes(eventos_deduplicados)
    batch_uf = ler_tabela_silver(glue_context, "integrado_uf")
    batch_municipio = ler_tabela_silver(
        glue_context,
        "integrado_municipio",
    )

    hibrido_uf = integrar_uf(atuais, batch_uf)
    hibrido_municipio = integrar_municipio(atuais, batch_municipio)
    hibrido = hibrido_uf.unionByName(hibrido_municipio).cache()

    gravar_catalogado(
        glue_context,
        hibrido,
        SILVER_DATABASE,
        "integrado_batch_streaming",
        SILVER_HIBRIDO,
        ["ano_referencia", "nivel"],
    )

    painel_gold = criar_painel_gold(hibrido)
    gravar_catalogado(
        glue_context,
        painel_gold,
        GOLD_DATABASE,
        "painel_hibrido_atual",
        GOLD_HIBRIDO,
        ["ano_referencia", "nivel"],
    )

    print(
        "Pipeline streaming concluída: Bronze -> Silver -> integração Batch "
        "+ Streaming -> Gold."
    )
    job.commit()


if __name__ == "__main__":
    main()
