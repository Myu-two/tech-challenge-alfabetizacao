import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


DATABASE = "alfabetizacao_db"
SILVER_BASE = "s3://pipeline-alfabetizacao-gabrielle-rosa/silver"

# Chaves usadas para eliminar registros repetidos em cada conjunto.
TABELAS = {
    "resultado_uf": {
        "destino": f"{SILVER_BASE}/resultados/uf/",
        "chaves": ["ano", "sigla_uf", "serie", "rede"],
        "obrigatorias": ["ano", "sigla_uf"],
    },
    "resultado_municipio": {
        "destino": f"{SILVER_BASE}/resultados/municipio/",
        "chaves": ["ano", "id_municipio", "serie", "rede"],
        "obrigatorias": ["ano", "id_municipio"],
    },
    "meta_brasil": {
        "destino": f"{SILVER_BASE}/metas/brasil/",
        "chaves": ["ano", "rede"],
        "obrigatorias": ["ano"],
    },
    "meta_uf": {
        "destino": f"{SILVER_BASE}/metas/uf/",
        "chaves": ["ano", "sigla_uf", "rede"],
        "obrigatorias": ["ano", "sigla_uf"],
    },
    "meta_municipio": {
        "destino": f"{SILVER_BASE}/metas/municipio/",
        "chaves": ["ano", "id_municipio", "rede"],
        "obrigatorias": ["ano", "id_municipio"],
    },
    "alunos": {
        "destino": f"{SILVER_BASE}/microdados/alunos/",
        "chaves": ["ano", "id_municipio", "id_escola", "id_aluno"],
        "obrigatorias": ["ano", "id_municipio", "id_aluno"],
    },
}


COLUNAS_INTEIRAS = {
    "ano",
    "nivel_alfabetizacao",
}

COLUNAS_IDENTIFICADORAS = {
    "id_municipio",
    "id_escola",
    "id_aluno",
}


def normalizar_strings(df: DataFrame) -> DataFrame:
    """Remove espaços e converte textos vazios em nulo."""
    for campo in df.schema.fields:
        if isinstance(campo.dataType, StringType):
            coluna = F.trim(F.col(campo.name))
            df = df.withColumn(
                campo.name,
                F.when((coluna == "") | coluna.isNull(), F.lit(None)).otherwise(coluna),
            )
    return df


def eh_coluna_decimal(nome: str) -> bool:
    prefixos = (
        "taxa_",
        "media_",
        "proporcao_",
        "meta_",
        "percentual_",
    )
    return nome.startswith(prefixos) or nome in {"proficiencia", "peso_aluno"}


def corrigir_tipos(df: DataFrame) -> DataFrame:
    """Aplica tipos consistentes às colunas conhecidas."""
    for nome in df.columns:
        if nome in COLUNAS_IDENTIFICADORAS:
            df = df.withColumn(nome, F.col(nome).cast("string"))
        elif nome in COLUNAS_INTEIRAS:
            df = df.withColumn(nome, F.col(nome).cast("int"))
        elif eh_coluna_decimal(nome):
            texto_numerico = F.regexp_replace(F.col(nome).cast("string"), ",", ".")
            df = df.withColumn(nome, texto_numerico.cast("double"))
    return df


def tratar_tabela(
    df: DataFrame,
    nome_tabela: str,
    chaves: list[str],
    obrigatorias: list[str],
) -> DataFrame:
    """Executa as regras gerais de limpeza da camada Silver."""
    df = normalizar_strings(df)
    df = corrigir_tipos(df)

    obrigatorias_existentes = [c for c in obrigatorias if c in df.columns]
    if obrigatorias_existentes:
        df = df.dropna(subset=obrigatorias_existentes)

    chaves_existentes = [c for c in chaves if c in df.columns]
    if chaves_existentes:
        df = df.dropDuplicates(chaves_existentes)
    else:
        df = df.dropDuplicates()

    # Taxas, metas, percentuais e proporções válidos devem ficar entre 0 e 100.
    for nome in df.columns:
        if nome.startswith(("taxa_", "meta_", "percentual_", "proporcao_")):
            df = df.withColumn(
                nome,
                F.when(
                    F.col(nome).between(0.0, 100.0),
                    F.col(nome),
                ).otherwise(F.lit(None).cast("double")),
            )

    return (
        df.withColumn("fonte_tabela", F.lit(nome_tabela))
        .withColumn("processado_em_utc", F.current_timestamp())
    )


def gravar_parquet(df: DataFrame, destino: str) -> None:
    writer = (
        df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
    )

    if "ano" in df.columns:
        writer.partitionBy("ano").save(destino)
    else:
        writer.save(destino)


def meta_correspondente_ao_ano(ano, alias_meta: str):
    """Seleciona a meta do mesmo ano do registro, quando disponível."""
    expressao = F.lit(None).cast("double")
    for ano_meta in range(2024, 2031):
        expressao = F.when(
            ano == ano_meta,
            F.col(f"{alias_meta}.meta_alfabetizacao_{ano_meta}"),
        ).otherwise(expressao)
    return expressao


def integrar_uf(resultado: DataFrame, meta: DataFrame) -> DataFrame:
    """Integra resultados públicos e metas de UF sem descartar chaves órfãs."""
    r = resultado.filter(
        F.col("rede") == "Pública (Estadual e Municipal)"
    ).alias("r")
    m = meta.alias("m")

    condicao = (
        (F.col("r.ano") == F.col("m.ano"))
        & (F.col("r.sigla_uf") == F.col("m.sigla_uf"))
    )
    integrado = r.join(m, condicao, "full_outer")

    ano = F.coalesce(F.col("r.ano"), F.col("m.ano")).cast("int")
    taxa_resultado = F.col("r.taxa_alfabetizacao").cast("double")
    meta_ano = meta_correspondente_ao_ano(ano, "m")
    meta_2030 = F.col("m.meta_alfabetizacao_2030").cast("double")

    status_integracao = (
        F.when(
            F.col("r.sigla_uf").isNotNull() & F.col("m.sigla_uf").isNotNull(),
            F.lit("Correspondente"),
        )
        .when(F.col("r.sigla_uf").isNotNull(), F.lit("Somente resultado"))
        .otherwise(F.lit("Somente meta"))
    )

    colunas_proporcao = [
        F.col(f"r.proporcao_aluno_nivel_{nivel}").alias(
            f"proporcao_aluno_nivel_{nivel}"
        )
        for nivel in range(0, 9)
    ]
    colunas_metas = [
        F.col(f"m.meta_alfabetizacao_{ano_meta}").alias(
            f"meta_alfabetizacao_{ano_meta}"
        )
        for ano_meta in range(2024, 2031)
    ]

    return integrado.select(
        ano.alias("ano"),
        F.coalesce(F.col("r.sigla_uf"), F.col("m.sigla_uf")).alias("sigla_uf"),
        F.coalesce(F.col("r.sigla_uf_nome"), F.col("m.sigla_uf_nome")).alias(
            "sigla_uf_nome"
        ),
        F.col("r.serie").alias("serie"),
        F.lit("Pública").alias("rede"),
        taxa_resultado.alias("taxa_resultado"),
        F.col("m.taxa_alfabetizacao").cast("double").alias(
            "taxa_referencia_meta"
        ),
        F.col("r.media_portugues").alias("media_portugues"),
        *colunas_proporcao,
        *colunas_metas,
        meta_ano.alias("meta_ano"),
        F.round(meta_ano - taxa_resultado, 2).alias("gap_para_meta_ano"),
        F.round(meta_2030 - taxa_resultado, 2).alias("gap_para_meta_2030"),
        F.col("m.percentual_participacao").alias("percentual_participacao"),
        status_integracao.alias("status_integracao"),
    )


def integrar_municipio(resultado: DataFrame, meta: DataFrame) -> DataFrame:
    """Integra resultados e metas municipais sem descartar chaves órfãs."""
    r = resultado.filter(F.col("rede") == "Municipal").alias("r")
    m = meta.alias("m")

    condicao = (
        (F.col("r.ano") == F.col("m.ano"))
        & (F.col("r.id_municipio") == F.col("m.id_municipio"))
    )
    integrado = r.join(m, condicao, "full_outer")

    ano = F.coalesce(F.col("r.ano"), F.col("m.ano")).cast("int")
    taxa_resultado = F.col("r.taxa_alfabetizacao").cast("double")
    meta_ano = meta_correspondente_ao_ano(ano, "m")
    meta_2030 = F.col("m.meta_alfabetizacao_2030").cast("double")

    status_integracao = (
        F.when(
            F.col("r.id_municipio").isNotNull()
            & F.col("m.id_municipio").isNotNull(),
            F.lit("Correspondente"),
        )
        .when(F.col("r.id_municipio").isNotNull(), F.lit("Somente resultado"))
        .otherwise(F.lit("Somente meta"))
    )

    colunas_proporcao = [
        F.col(f"r.proporcao_aluno_nivel_{nivel}").alias(
            f"proporcao_aluno_nivel_{nivel}"
        )
        for nivel in range(0, 9)
    ]
    colunas_metas = [
        F.col(f"m.meta_alfabetizacao_{ano_meta}").alias(
            f"meta_alfabetizacao_{ano_meta}"
        )
        for ano_meta in range(2024, 2031)
    ]

    return integrado.select(
        ano.alias("ano"),
        F.coalesce(F.col("r.id_municipio"), F.col("m.id_municipio")).alias(
            "id_municipio"
        ),
        F.coalesce(
            F.col("r.id_municipio_nome"), F.col("m.id_municipio_nome")
        ).alias("id_municipio_nome"),
        F.col("r.serie").alias("serie"),
        F.lit("Municipal").alias("rede"),
        taxa_resultado.alias("taxa_resultado"),
        F.col("m.taxa_alfabetizacao").cast("double").alias(
            "taxa_referencia_meta"
        ),
        F.col("r.media_portugues").alias("media_portugues"),
        *colunas_proporcao,
        *colunas_metas,
        meta_ano.alias("meta_ano"),
        F.round(meta_ano - taxa_resultado, 2).alias("gap_para_meta_ano"),
        F.round(meta_2030 - taxa_resultado, 2).alias("gap_para_meta_2030"),
        F.col("m.nivel_alfabetizacao").alias("nivel_alfabetizacao"),
        F.col("m.percentual_participacao").alias("percentual_participacao"),
        status_integracao.alias("status_integracao"),
    )


def gravar_e_catalogar_integracao(
    glue_context: GlueContext,
    df: DataFrame,
    nome_tabela: str,
    destino: str,
) -> None:
    """Recria o dataset integrado e atualiza o Data Catalog automaticamente."""
    glue_context.purge_s3_path(destino, options={"retentionPeriod": 0})
    saida = df.withColumn("processado_em_utc", F.current_timestamp())
    dynamic_frame = DynamicFrame.fromDF(
        saida, glue_context, f"dynamic_{nome_tabela}"
    )

    sink = glue_context.getSink(
        connection_type="s3",
        path=destino,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["ano"],
        transformation_ctx=f"destino_{nome_tabela}",
    )
    sink.setCatalogInfo(
        catalogDatabase="alfabetizacao_silver_db",
        catalogTableName=nome_tabela,
    )
    sink.setFormat("glueparquet")
    sink.writeFrame(dynamic_frame)
    print(f"[CONCLUÍDO] alfabetizacao_silver_db.{nome_tabela}")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    tabelas_tratadas = {}

    for nome_tabela, configuracao in TABELAS.items():
        print(f"[INÍCIO] Processando {DATABASE}.{nome_tabela}")

        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=DATABASE,
            table_name=nome_tabela,
            transformation_ctx=f"origem_{nome_tabela}",
        )

        dataframe = dynamic_frame.toDF()
        dataframe_tratado = tratar_tabela(
            df=dataframe,
            nome_tabela=nome_tabela,
            chaves=configuracao["chaves"],
            obrigatorias=configuracao["obrigatorias"],
        )

        tabelas_tratadas[nome_tabela] = dataframe_tratado

        gravar_parquet(dataframe_tratado, configuracao["destino"])
        print(f"[CONCLUÍDO] {nome_tabela} -> {configuracao['destino']}")

    integrado_uf = integrar_uf(
        tabelas_tratadas["resultado_uf"],
        tabelas_tratadas["meta_uf"],
    )
    integrado_municipio = integrar_municipio(
        tabelas_tratadas["resultado_municipio"],
        tabelas_tratadas["meta_municipio"],
    )

    gravar_e_catalogar_integracao(
        glue_context,
        integrado_uf,
        "integrado_uf",
        f"{SILVER_BASE}/integrado/uf/",
    )
    gravar_e_catalogar_integracao(
        glue_context,
        integrado_municipio,
        "integrado_municipio",
        f"{SILVER_BASE}/integrado/municipio/",
    )

    job.commit()
    print("Camada Silver criada com sucesso.")


if __name__ == "__main__":
    main()