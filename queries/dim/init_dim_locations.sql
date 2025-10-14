SELECT
    loc_id AS location_id,
    loc_en_id AS entity_id,
    loc_code AS location_code,
    loc_ptnr_id AS customer_id,
    loc_desc AS location_name,
    loc_active AS is_active,
    loc_add_date AS created_at
FROM public.loc_mstr
ORDER BY loc_id ASC