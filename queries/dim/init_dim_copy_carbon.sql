SELECT
    cc_id AS copy_carbon_id,
    cc_en_id AS entity_id,
    CASE WHEN cc_code IS NULL THEN '-' ELSE cc_code END AS carbon_copy_code,
    cc_desc AS carbon_copy_desc,
    CASE WHEN cc_active = 'Y' THEN TRUE ELSE FALSE END AS is_active,
    cc_add_by AS created_by,
    cc_add_date AS created_at
FROM PUBLIC.cc_mstr
ORDER BY cc_id ASC