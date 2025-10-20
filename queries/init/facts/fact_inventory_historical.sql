SELECT
    CAST(TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS BIGINT) AS date_id,
    invc_en_id AS entity_id,
    invc_si_id AS site_id,
    invc_loc_id AS location_id,
    invc_pt_id AS product_id,
    invc_qty AS qty_real,
    invc_qty_old AS qty_old,
    CURRENT_TIMESTAMP AS created_at
FROM public.invc_mstr
ORDER BY created_at ASC