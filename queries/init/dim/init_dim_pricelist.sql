SELECT
    pi_id AS pricelist_id,
    pi_ptnrg_id AS group_id,
    pi_en_id AS entity_id,
    pi_cu_id AS currency_id,
    pi_desc AS pricelist_name,
    pi_code AS pricelist_code,
    CASE WHEN pi_active = 'Y' THEN TRUE ELSE FALSE END AS is_active,
    pi_add_date AS created_at
FROM public.pi_mstr
ORDER BY pi_id ASC