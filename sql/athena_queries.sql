-- ============================================================
-- Tech Challenge - Pipeline Híbrido de Alfabetização
-- Consultas utilizadas no Amazon Athena
-- ============================================================


-- ============================================================
-- 1. VISÃO FINAL - INTEGRAÇÃO BATCH + STREAMING
-- ============================================================

SELECT
    nivel,
    nome_localidade,
    ano_referencia,
    ROUND(taxa_batch, 2) AS taxa_batch,
    ROUND(taxa_streaming, 2) AS taxa_streaming,
    ROUND(meta_ano, 2) AS meta_ano,
    variacao_streaming_vs_batch,
    gap_streaming_para_meta,
    status_meta_streaming,
    status_correspondencia
FROM alfabetizacao_gold_db.painel_hibrido_atual
ORDER BY nivel, nome_localidade;


-- ============================================================
-- 2. CONTAGEM DAS TABELAS DO PIPELINE HÍBRIDO
-- ============================================================

SELECT
    'eventos_streaming' AS tabela,
    COUNT(*) AS registros
FROM alfabetizacao_silver_db.eventos_streaming

UNION ALL

SELECT
    'integrado_batch_streaming',
    COUNT(*)
FROM alfabetizacao_silver_db.integrado_batch_streaming

UNION ALL

SELECT
    'painel_hibrido_atual',
    COUNT(*)
FROM alfabetizacao_gold_db.painel_hibrido_atual;


-- ============================================================
-- 3. VALIDAÇÃO DA INTEGRAÇÃO ENTRE RESULTADOS E METAS
-- ============================================================

SELECT
    'UF' AS nivel,
    COUNT(*) AS total,
    COUNT_IF(status_integracao = 'Correspondente') AS correspondencias,
    COUNT_IF(status_integracao = 'Somente resultado') AS somente_resultado,
    COUNT_IF(status_integracao = 'Somente meta') AS somente_meta
FROM alfabetizacao_gold_db.comparativo_uf

UNION ALL

SELECT
    'Município',
    COUNT(*),
    COUNT_IF(status_integracao = 'Correspondente'),
    COUNT_IF(status_integracao = 'Somente resultado'),
    COUNT_IF(status_integracao = 'Somente meta')
FROM alfabetizacao_gold_db.comparativo_municipio;


-- ============================================================
-- 4. CORRESPONDÊNCIA ENTRE BASES NA CAMADA SILVER
-- ============================================================

WITH
uf_resultado AS (
    SELECT DISTINCT ano, sigla_uf
    FROM alfabetizacao_silver_db.resultado_uf
    WHERE rede = 'Pública (Estadual e Municipal)'
),
uf_meta AS (
    SELECT DISTINCT ano, sigla_uf
    FROM alfabetizacao_silver_db.meta_uf
),
municipio_resultado AS (
    SELECT DISTINCT ano, id_municipio
    FROM alfabetizacao_silver_db.resultado_municipio
    WHERE rede = 'Municipal'
),
municipio_meta AS (
    SELECT DISTINCT ano, id_municipio
    FROM alfabetizacao_silver_db.meta_municipio
)

SELECT
    'UF' AS nivel,
    COUNT_IF(r.sigla_uf IS NOT NULL AND m.sigla_uf IS NOT NULL) AS correspondencias,
    COUNT_IF(r.sigla_uf IS NOT NULL AND m.sigla_uf IS NULL) AS somente_resultado,
    COUNT_IF(r.sigla_uf IS NULL AND m.sigla_uf IS NOT NULL) AS somente_meta
FROM uf_resultado r
FULL OUTER JOIN uf_meta m
    ON r.ano = m.ano
   AND r.sigla_uf = m.sigla_uf

UNION ALL

SELECT
    'Município',
    COUNT_IF(r.id_municipio IS NOT NULL AND m.id_municipio IS NOT NULL),
    COUNT_IF(r.id_municipio IS NOT NULL AND m.id_municipio IS NULL),
    COUNT_IF(r.id_municipio IS NULL AND m.id_municipio IS NOT NULL)
FROM municipio_resultado r
FULL OUTER JOIN municipio_meta m
    ON r.ano = m.ano
   AND r.id_municipio = m.id_municipio;


-- ============================================================
-- 5. VOLUME DE REGISTROS POR ANO
-- ============================================================

SELECT 'resultado_uf' AS origem, ano, COUNT(*) AS registros
FROM alfabetizacao_silver_db.resultado_uf
WHERE rede = 'Pública (Estadual e Municipal)'
GROUP BY ano

UNION ALL

SELECT 'meta_uf', ano, COUNT(*)
FROM alfabetizacao_silver_db.meta_uf
GROUP BY ano

UNION ALL

SELECT 'resultado_municipio', ano, COUNT(*)
FROM alfabetizacao_silver_db.resultado_municipio
WHERE rede = 'Municipal'
GROUP BY ano

UNION ALL

SELECT 'meta_municipio', ano, COUNT(*)
FROM alfabetizacao_silver_db.meta_municipio
GROUP BY ano

ORDER BY origem, ano;


-- ============================================================
-- 6. VOLUME DE DADOS NA CAMADA GOLD
-- ============================================================

SELECT 'indicadores_uf' AS tabela, COUNT(*) AS registros
FROM alfabetizacao_gold_db.indicadores_uf

UNION ALL

SELECT 'indicadores_municipio', COUNT(*)
FROM alfabetizacao_gold_db.indicadores_municipio

UNION ALL

SELECT 'painel_metas', COUNT(*)
FROM alfabetizacao_gold_db.painel_metas

UNION ALL

SELECT 'resumo_alunos_municipio', COUNT(*)
FROM alfabetizacao_gold_db.resumo_alunos_municipio;


-- ============================================================
-- 7. AMOSTRA DA CAMADA GOLD
-- ============================================================

SELECT *
FROM alfabetizacao_gold_db.indicadores_uf
LIMIT 10;