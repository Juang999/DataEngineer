SELECT
    ptnr_id AS customer_id,
    ptnr_en_id AS entity_id,
    ptnr_name AS nama_mitra,
    ptnr_ptnrg_id AS group_id,
    ptnr_code AS customer_code,
    CAST(TO_CHAR(ptnr_add_date,'YYYYMMDD') AS BIGINT) AS date_id,
    ptnr_add_date AS created_at
FROM public.ptnr_mstr
WHERE ptnr_add_date > '{created_at}'
ORDER BY ptnr_id ASC