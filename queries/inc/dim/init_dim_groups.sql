SELECT
    ptnrg_id AS group_id,
    ptnrg_en_id AS entity_id,
    ptnrg_code AS group_code,
    ptnrg_name AS group_name,
    CASE WHEN ptnrg_desc IS NULL THEN '-' ELSE ptnrg_desc END AS group_description,
    CASE WHEN ptnrg_active = 'Y' THEN TRUE::BOOLEAN ELSE FALSE::BOOLEAN END AS is_active,
    ptnrg_add_date AS created_at
FROM public.ptnrg_grp
WHERE ptnrg_id not in (101)
ORDER BY ptnrg_id ASC
