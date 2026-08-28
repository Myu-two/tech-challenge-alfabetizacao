-- ============================================================
-- Tech Challenge - Alfabetização
-- Validações de qualidade da ingestão Streaming
-- ============================================================

-- 1. Verifica eventos sem identificador
SELECT
    'evento_id_nulo' AS teste,
    COUNT(*) AS problemas
FROM alfabetizacao_silver_db.eventos_streaming
WHERE evento_id IS NULL

UNION ALL

-- 2. Verifica eventos sem timestamp
SELECT
    'timestamp_nulo',
    COUNT(*)
FROM alfabetizacao_silver_db.eventos_streaming
WHERE timestamp_evento IS NULL

UNION ALL

-- 3. Verifica taxas de alfabetização fora do intervalo válido
SELECT
    'taxa_fora_intervalo',
    COUNT(*)
FROM alfabetizacao_silver_db.eventos_streaming
WHERE taxa_alfabetizacao NOT BETWEEN 0 AND 100

UNION ALL

-- 4. Verifica IDs de eventos duplicados
SELECT
    'evento_id_duplicado',
    COUNT(*)
FROM (
    SELECT evento_id
    FROM alfabetizacao_silver_db.eventos_streaming
    GROUP BY evento_id
    HAVING COUNT(*) > 1
);